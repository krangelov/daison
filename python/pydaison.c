#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>
#include "../c/sqlite3Btree.h"

#ifdef _MSC_VER
#include <malloc.h>
#define alloca _alloca
#endif

typedef struct {
    PyObject_HEAD
    Btree *pBtree;
    u32 cookie;
    PyObject *schema;
    PyObject *genAlias;
    PyObject *unionGenAlias;
    PyObject *enumMeta;
} DBObject;

typedef struct {
    PyObject_HEAD
    DBObject *db;
    int mode;
} TransObject;

typedef struct {
    PyObject_HEAD
    PyObject *name;
    PyObject *type;
    PyObject *indices;
} TableObject;

typedef enum {
    SIMPLE_INDEX,
    LIST_INDEX,
    MAYBE_INDEX
} IndexKind;

typedef struct {
    PyObject_HEAD
    PyObject *table;
    PyObject *name;
    PyObject *fun;
    IndexKind kind;
    PyObject *type;
} IndexObject;

PyObject *DBError;

static int
checkSqlite3Error(int rc)
{
    if (rc != SQLITE_OK) {
        const char *msg = sqlite3BtreeErrName(rc);
        PyErr_SetString(DBError, msg);
        return 0;
    }
    return 1;
}

typedef struct {
    uint8_t *p;
    uint8_t *start;
    uint8_t *end;
} buffer;

static int
putWord8(uint8_t w, buffer *buf)
{
    if (buf->p >= buf->end) {
        size_t len     = (buf->p - buf->start);
        size_t new_len = (len+1) * 2;
        uint8_t *p = realloc(buf->start, new_len);
        if (p == NULL) {
            PyErr_NoMemory();
            return 0;
        }
        buf->start = p;
        buf->p     = buf->start + len;
        buf->end   = buf->start + new_len;
    }
    *buf->p = w;
    buf->p++;
    return 1;
}

static int
putBytes(const char *bytes, size_t n_bytes, buffer *buf)
{
    if (buf->p+n_bytes >= buf->end) {
        size_t len     = (buf->p - buf->start);
        size_t new_len = len * 2;
        if (buf->p+n_bytes > buf->start+new_len) {
            new_len = buf->p+n_bytes - buf->start;
        }

        uint8_t *p = realloc(buf->start, new_len);
        if (p == NULL) {
            PyErr_NoMemory();
            return 0;
        }
        buf->start = p;
        buf->p     = buf->start + len;
        buf->end   = buf->start + new_len;
    }
    memcpy(buf->p, bytes, n_bytes);
    buf->p += n_bytes;
    return 1;
}

static int
putRest(int64_t n, buffer *buf)
{
    for (;;) {
        uint8_t n0 = (n & 0x7F) << 1;
        n = n >> 7;

        if ((n == 0 && (n0 & 0x80) == 0) || (n == -1 && (n0 & 0x80) != 0)) {
            return putWord8(n0,buf);
        } else {
            if (!putWord8(n0 | 1,buf))
                return 0;
        }
    }
}

static int
putTag(uint8_t tag, buffer *buf)
{
    return putWord8(tag, buf);
}

static int
putFloat(buffer *buf, float f)
{
    if (buf->p+sizeof(float) > buf->end) {
        size_t len     = (buf->p - buf->start);
        size_t new_len = (len+sizeof(float)) * 2;
        uint8_t *p = realloc(buf->start, new_len);
        if (p == NULL) {
            PyErr_NoMemory();
            return 0;
        }
        buf->start = p;
        buf->p     = buf->start + len;
        buf->end   = buf->start + new_len;
    }

    *((float*) buf->p) = f;
    buf->p += sizeof(float);
    return 1;
}

static int
putVInt(uint8_t tag, int bits, int64_t n, buffer *buf)
{
    int rbits = 7-bits;
    int64_t n0 = (n & ((1 << rbits) - 1)) << (bits+1);
    n = n >> rbits;

    if ((n == 0 && (n0 & 0x80) == 0) || (n == -1 && (n0 & 0x80) != 0)) {
        return putWord8(n0 | tag, buf);
    } else {
        if (!putWord8(n0 | (1 << bits) | tag, buf))
            return 0;
        return putRest(n,buf);
    }
}

static int
serialize(DBObject *py_db, PyObject *type, PyObject *obj, buffer *buf);

static int
serializeObject(DBObject *py_db, PyObject *type, PyObject *obj, buffer *buf)
{
    if (PyObject_HasAttrString(obj, "__serialize__")) {
        PyObject *py_capsule = PyCapsule_New(buf, "daison-buffer", NULL);
        if (py_capsule == NULL)
            return 0;
        PyObject *res = PyObject_CallMethod(obj, "__serialize__", "OO", py_db, py_capsule);
        Py_XDECREF(res);
        Py_DECREF(py_capsule);
        return 1;
    }

    PyObject *init = PyObject_GetAttrString(type, "__init__");
    if (init == NULL)
        return 0;
    PyObject *annotations = PyObject_GetAttrString(init, "__annotations__");
    Py_DECREF(init);
    if (annotations == NULL)
        return 0;

    Py_ssize_t n_args = PyDict_Size(annotations)-1;

    int i = 0;
    Py_ssize_t pos = 0;
    PyObject *field_name, *field_type;
    while (PyDict_Next(annotations, &pos, &field_name, &field_type)) {
        if (i >= n_args)
            break;

        PyObject *field_value = PyObject_GetAttr(obj, field_name);
        if (field_value == NULL) {
            Py_DECREF(annotations);
            return 0;
        }

        if (!serialize(py_db,field_type,field_value,buf)) {
            Py_DECREF(field_value);
            Py_DECREF(annotations);
            return 0;
        }

        Py_DECREF(field_value);
        i++;
    }

    Py_DECREF(annotations);
    return 1;
}

static int
serialize(DBObject *py_db, PyObject *type, PyObject *obj, buffer *buf)
{
    if (type == (PyObject*) &PyUnicode_Type) {
        Py_ssize_t len = PyUnicode_GetLength(obj);

        putVInt(0b10, 2, len, buf);
        if (PyErr_Occurred())
            return 0;

        Py_ssize_t size;
        const char *utf8 = PyUnicode_AsUTF8AndSize(obj, &size);
        if (!utf8)
            return 0;

        putBytes(utf8, size, buf);
        return 1;
    } else if (type == (PyObject*) &PyLong_Type) {
        return putVInt(0b01, 2, PyLong_AsLong(obj), buf);
    } else if (type == (PyObject*) &PyFloat_Type) {
        float f = PyFloat_AsDouble(obj);
        if (PyErr_Occurred())
            return 0;

        if (!putTag(0b01111, buf))
            return 0;

        if (!putFloat(buf, f))
            return 0;

        return 1;
    } else if (obj == Py_False) {
        if (!putVInt(0b011, 3, 1, buf))
            return 0;
        return putTag(0b00111, buf);
    } else if (obj == Py_True) {
        if (!putVInt(0b011, 3, 2, buf))
            return 0;
        return putTag(0b00111, buf);
    } else if (PyObject_IsInstance(type, py_db->enumMeta)) {
        PyObject *members = PyObject_GetAttrString(type, "__members__");
        if (members == NULL)
            return 0;

        PyObject *iterator = PyObject_GetIter(members);
        if (iterator == NULL) {
            Py_DECREF(members);
            return 0;
        }

        size_t index = 1;
        PyObject *member_name;
        while ((member_name = PyIter_Next(iterator))) {
            PyObject *member = PyObject_GetItem(members, member_name);
            Py_DECREF(member);
            if (obj == member)
                break;
            index++;
            Py_DECREF(member_name);
        }

        Py_DECREF(iterator);
        Py_DECREF(members);

        if (member_name == NULL) {
            PyErr_SetString(PyExc_TypeError, "An object not a member of the Enum class");
            return 0;
        }

        if (!putVInt(0b011, 3, index, buf))
            return 0;

        return putTag(0b00111, buf);
    } else if (PyObject_IsInstance(type, py_db->unionGenAlias)) {
        PyObject *args = PyObject_GetAttrString(type, "__args__");
        if (args == NULL)
            return 0;
        Py_ssize_t n_args = PyTuple_Size(args);
        PyObject *obj_type = (PyObject *) Py_TYPE(obj);

        if (n_args == 2 && PyTuple_GetItem(args, 1) == (PyObject *) Py_TYPE(Py_None)) {
            if (obj == Py_None) {
                Py_DECREF(args);
                if (!putVInt(0b011, 3, 1, buf)) {
                    return 0;
                }
            } else {
                PyObject *arg_type = PyTuple_GetItem(args, 0);
                if (arg_type == NULL) {
                    Py_DECREF(args);
                    return 0;
                }
                if (arg_type != obj_type) {
                    PyErr_SetString(PyExc_TypeError, "Classes does not match");
                    Py_DECREF(args);
                    return 0;
                }

                Py_DECREF(args);

                if (!putVInt(0b011, 3, 2, buf)) {
                    return 0;
                }
                if (!serialize(py_db, obj_type, obj, buf)) {
                    return 0;
                }
            }
        } else {
            Py_ssize_t index = 0;
            for (Py_ssize_t i = 0; i < n_args; i++) {
                PyObject *arg_type = PyTuple_GetItem(args, i);
                if (arg_type == NULL) {
                    Py_DECREF(args);
                    return 0;
                }
                if (obj_type == arg_type)
                    index = i+1;
            }

            Py_DECREF(args);

            if (index == 0) {
                PyErr_SetString(PyExc_TypeError, "Classes does not match");
                return 0;
            }

            if (!putVInt(0b011, 3, index, buf)) {
                return 0;
            }

            if (!serializeObject(py_db, obj_type, obj, buf)) {
                return 0;
            }
        }

        return putTag(0b00111, buf);
    } else if (PyObject_IsInstance(type, py_db->genAlias)) {
        PyObject *origin = PyObject_GetAttrString(type, "__origin__");
        PyObject *args = PyObject_GetAttrString(type, "__args__");

        if (origin == (PyObject*) &PyList_Type) {
            Py_ssize_t len = PyList_Size(obj);
            if (!putVInt(0b00, 2, len, buf))
                return 0;

            PyObject *item_type = PyTuple_GetItem(args, 0);
            for (Py_ssize_t i = 0; i < len; i++) {
                PyObject *item = PyList_GetItem(obj, i);
                if (!serialize(py_db, item_type, item, buf)) {
                    return 0;
                }
            }

            return 1;
        } else if (origin == (PyObject*) &PyTuple_Type) {
            if (!putVInt(0b011, 3, 1, buf))
                return 0;

            Py_ssize_t n_args = PyTuple_Size(args);
            for (Py_ssize_t i = 0; i < n_args; i++) {
                PyObject *arg_type = PyTuple_GetItem(args, i);
                if (arg_type == NULL) {
                    return 0;
                }

                PyObject *arg = PyTuple_GetItem(obj, i);
                if (arg == NULL) {
                    return 0;
                }

                if (!serialize(py_db, arg_type, arg, buf))
                    return 0;
            }

            return putTag(0b00111, buf);
        }

        return 0;
    } else {
        if (!PyObject_IsInstance(obj, type)) {
            PyErr_SetString(PyExc_TypeError, "Classes does not match");
            return 0;
        }

        Py_ssize_t index = 1;
        if (PyObject_HasAttrString(type, "__get_index__")) {
            PyObject *res = PyObject_CallMethod(type, "__get_index__", "O", Py_TYPE(obj));
            if (res) {
                index = PyLong_AsSsize_t(res);
                Py_DECREF(res);
                if (PyErr_Occurred())
                    return 0;
            }
        }

        if (!putVInt(0b011, 3, index, buf))
            return 0;

        if (!serializeObject(py_db, type, obj, buf))
            return 0;

        return putTag(0b00111, buf);
    }
}

static uint8_t
getWord8(buffer *buf)
{
    if (buf->p >= buf->end) {
        PyErr_Format(DBError, "buffer too short");
        return 0;
    }

    return *buf->p++;
}

static int64_t
getRest(int bits, int64_t n, buffer *buf)
{
    uint8_t w;
    for (;;) {
        w = getWord8(buf);
        if (PyErr_Occurred())
            return 0;

        n |= (((w & 0xFE) >> 1) << bits);
        if ((w & 1) == 0)
            break;
        bits += 7;
    }
    if ((w & 0x80) != 0) {
        n |= ((-128) << bits);
    }
    return n;
}

static int64_t
getVInt(uint8_t tag, int bits, const char *name, buffer *buf)
{
    uint8_t w = getWord8(buf);
    if (PyErr_Occurred())
        return 0;

    if ((w & ((1 << bits) - 1)) == tag) {
        int64_t n = w >> (bits+1);
        if ((w & (1 << bits)) == 0) {
            if ((w & 0x80) != 0)
                return (n | (0xFF << (7-bits)));
            return n;
        } else {
            return getRest(7-bits, n, buf);
        }
    } else {
        PyErr_Format(DBError, "failed to find %s", name);
        return 0;
    }
}

static float
getFloat(buffer *buf)
{
    if (buf->p+sizeof(float) > buf->end) {
        PyErr_Format(DBError, "buffer too short");
        return 0;
    }

    float f = *((float*) buf->p);
    buf->p += sizeof(float);
    return f;
}

static uint64_t
getTag(uint8_t tag, int bits, const char *name, buffer *buf)
{
    uint8_t w = getWord8(buf);
    if (PyErr_Occurred())
        return 0;

    if ((w & ((1 << bits) - 1)) == tag) {
        return 1;
    } else {
        PyErr_Format(DBError, "failed to find %s", name);
        return 0;
    }
}

static PyObject *
deserialize(DBObject *py_db, PyObject *type, buffer *buf);

static PyObject *
deserializeObject(DBObject *py_db, uint64_t index, PyObject *type, buffer *buf)
{
    if (PyObject_HasAttrString(type, "__deserialize__")) {
        PyObject *py_capsule = PyCapsule_New(buf, "daison-buffer", NULL);
        if (py_capsule == NULL)
            return NULL;
        PyObject *res = PyObject_CallMethod(type, "__deserialize__", "OlO", py_db, index, py_capsule);
        Py_DECREF(py_capsule);
        return res;
    }

    PyObject *init = PyObject_GetAttrString(type, "__init__");
    if (init == NULL)
        return NULL;
    PyObject *annotations = PyObject_GetAttrString(init, "__annotations__");
    Py_DECREF(init);
    if (annotations == NULL)
        return NULL;

    Py_ssize_t n_args = PyDict_Size(annotations)-1;
    PyObject *args = PyTuple_New(n_args);

    int i = 0;
    Py_ssize_t pos = 0;
    PyObject *field_name, *field_type;
    while (PyDict_Next(annotations, &pos, &field_name, &field_type)) {
        if (i >= n_args)
            break;

        PyObject *field_value = deserialize(py_db,field_type,buf);
        if (field_value == NULL) {
            Py_DECREF(annotations);
            Py_DECREF(args);
            return NULL;
        }

        PyTuple_SetItem(args, i, field_value); i++;
    }

    Py_DECREF(annotations);

    PyObject *res = PyObject_CallObject((PyObject *) type, args);

    Py_DECREF(args);

    return res;
}

static PyObject *
deserialize(DBObject *py_db, PyObject *type, buffer *buf)
{
    PyObject *res = NULL;

    if (type == (PyObject*) &PyUnicode_Type) {
        size_t len = getVInt(0b10, 2, "a string", buf);
        if (PyErr_Occurred())
            return NULL;
        const uint8_t *s = buf->p;
        for (size_t i = 0; i < len; i++) {
            if (buf->p >= buf->end) {
                PyErr_Format(DBError, "buffer too short");
                return 0;
            }

            uint8_t c = *buf->p;
            buf->p +=
                (c < 0x80 ? 1 :
                 c < 0xe0 ? 2 :
	             c < 0xf0 ? 3 :
	             c < 0xf8 ? 4 :
	             c < 0xfc ? 5 :
	                        6
	            );
        }
        res = PyUnicode_FromStringAndSize((const char *) s,buf->p-s);
    } else if (type == (PyObject*) &PyLong_Type) {
        uint64_t value = getVInt(0b01, 2, "an int", buf);
        if (PyErr_Occurred())
            return NULL;
        res = PyLong_FromLong(value);
    } else if (type == (PyObject*) &PyFloat_Type) {
        getTag(0b01111, 5, "a float", buf);
        if (PyErr_Occurred())
            return NULL;

        float f = getFloat(buf);
        if (PyErr_Occurred())
            return NULL;

        res = PyFloat_FromDouble(f);
    } else if (type == (PyObject*) &PyBool_Type) {
        uint64_t index = getVInt(0b011, 3, "a constructor", buf);
        if (PyErr_Occurred())
            return NULL;
        if (index == 1) {
            res = Py_False;
            Py_INCREF(res);
        } else if (index == 2) {
            res = Py_True;
            Py_INCREF(res);
        } else {
            return NULL;
        }
        getTag(0b00111, 5, "an args-end", buf);
        if (PyErr_Occurred()) {
            Py_DECREF(res);
            return NULL;
        }
    } else if (PyObject_IsInstance(type, py_db->enumMeta)) {
        uint64_t index = getVInt(0b011, 3, "a constructor", buf);
        if (PyErr_Occurred())
            return NULL;

        PyObject *members = PyObject_GetAttrString(type, "__members__");
        if (members == NULL)
            return NULL;

        PyObject *iterator = PyObject_GetIter(members);
        if (iterator == NULL) {
            Py_DECREF(members);
            return NULL;
        }

        PyObject *member_name;
        while ((member_name = PyIter_Next(iterator))) {
            if (index == 1) {
                res = PyObject_GetItem(members, member_name);
                break;
            }
            index--;
            Py_DECREF(member_name);
        }

        Py_DECREF(iterator);
        Py_DECREF(members);
        
        getTag(0b00111, 5, "an args-end", buf);
        if (PyErr_Occurred()) {
            Py_DECREF(res);
            return NULL;
        }
    } else if (PyObject_IsInstance(type, py_db->unionGenAlias)) {
        uint64_t index = getVInt(0b011, 3, "a constructor", buf);

        PyObject *args = PyObject_GetAttrString(type, "__args__");
        Py_ssize_t n_args = PyTuple_Size(args);

        if (n_args == 2 && PyTuple_GetItem(args, 1) == (PyObject *) Py_TYPE(Py_None)) {
            if (index == 1) {
                res = Py_None;
                Py_INCREF(res);
            } else {
                PyObject *arg_type = PyTuple_GetItem(args, 0);
                if (arg_type == NULL) {
                    Py_DECREF(args);
                    return NULL;
                }
                res = deserialize(py_db, arg_type, buf);
            }
        } else {
            PyObject *arg_type = PyTuple_GetItem(args, index-1);
            if (arg_type == NULL) {
                Py_DECREF(args);
                return NULL;
            }
            res = deserializeObject(py_db, index, arg_type, buf);
        }

        Py_DECREF(args);

        if (res == NULL)
            return NULL;

        getTag(0b00111, 5, "an args-end", buf);
        if (PyErr_Occurred()) {
            Py_DECREF(res);
            return NULL;
        }
    } else if (PyObject_IsInstance(type, py_db->genAlias)) {
        PyObject *origin = PyObject_GetAttrString(type, "__origin__");
        PyObject *args = PyObject_GetAttrString(type, "__args__");

        if (origin == (PyObject*) &PyList_Type) {
            Py_ssize_t len = getVInt(0b00, 2, "a list", buf);
            if (PyErr_Occurred())
                return NULL;

            res = PyList_New(len);
            if (res == NULL)
                return NULL;

            PyObject *item_type = PyTuple_GetItem(args, 0);
            for (Py_ssize_t i = 0; i < len; i++) {
                PyObject *item = deserialize(py_db, item_type, buf);
                if (item == NULL) {
                    Py_DECREF(res);
                    return NULL;
                }
                PyList_SetItem(res, i, item);
            }
        } else if (origin == (PyObject*) &PyTuple_Type) {
            getVInt(0b011, 3, "a constructor", buf);
            if (PyErr_Occurred())
                return NULL;

            Py_ssize_t n_args = PyTuple_Size(args);
            res = PyTuple_New(n_args);
            if (res == NULL)
                return NULL;

            for (Py_ssize_t i = 0; i < n_args; i++) {
                PyObject *arg_type = PyTuple_GetItem(args, i);
                PyObject *arg = deserialize(py_db, arg_type, buf);
                if (arg == NULL) {
                    Py_DECREF(res);
                    return NULL;
                }
                PyTuple_SetItem(res, i, arg);
            }

            getTag(0b00111, 5, "an args-end", buf);
            if (PyErr_Occurred()) {
                Py_DECREF(res);
                return NULL;
            }
        }

        Py_DECREF(args);
        Py_DECREF(origin);
    } else {
        uint64_t index = getVInt(0b011, 3, "a constructor", buf);
        if (PyErr_Occurred())
            return NULL;

        res = deserializeObject(py_db, index, type, buf);
        if (res == NULL)
            return NULL;

        getTag(0b00111, 5, "an args-end", buf);
        if (PyErr_Occurred()) {
            Py_DECREF(res);
            return NULL;
        }
    }

    return res;
}

static PyObject *
deserializeIds(buffer *buf)
{
    PyObject *py_keys = PyList_New(0);
    if (py_keys == NULL)
        return NULL;

    while (buf->p < buf->end) {
        int64_t key = getRest(0, 0, buf);
        if (PyErr_Occurred()) {
            Py_DECREF(py_keys);
            return NULL;
        }
        
        PyObject *py_key = PyLong_FromLong(key);
        if (py_key == NULL) {
            Py_DECREF(py_keys);
            return NULL;
        }

        if (PyList_Append(py_keys, py_key) != 0) {
            Py_DECREF(py_key);
            Py_DECREF(py_keys);
            return NULL;
        }

        Py_DECREF(py_key);
    }

    return py_keys;
}

static int
insertId(i64 id, buffer *buf)
{
    size_t required = sizeof(id)*2;
    if (buf->p + required > buf->end) {
        size_t size = buf->p-buf->start;
        uint8_t *p = realloc(buf->start, size+required);
        if (p == NULL) {
            PyErr_NoMemory();
            return 0;
        }
        buf->start=p;
        buf->p    =buf->start+size;
        buf->end  =buf->start+size+required;
    }
    return putRest(id,buf);
}

static int
deleteId(i64 id, buffer *buf)
{
    uint8_t *target = buf->p;

    while (buf->p < buf->end) {
        u64 id1  = getRest(0, 0, buf);
        if (PyErr_Occurred()) {
            return 0;
        }

        if (id == (i64) id1)
            break;

        target = buf->p;
    }

    memcpy(target, buf->p, buf->end-buf->p);
    buf->p = buf->end-(buf->p-target);
    return 1;
}

static int
daison_fetchSchema(DBObject *py_db)
{
    int rc;
    
    rc = sqlite3BtreeLockTable(py_db->pBtree, 1, 0);
    if (!checkSqlite3Error(rc)) {
        return 0;
    }

    u32 cookie;
    sqlite3BtreeGetMeta(py_db->pBtree, 1, &cookie);

    if (cookie == py_db->cookie)
        return 1;

    PyDict_Clear(py_db->schema);

    BtCursor *pCursor;
    rc = sqlite3BtreeCursor(py_db->pBtree, 1, 0, 0, 0, &pCursor);
    if (!checkSqlite3Error(rc)) {
        return 0;
    }

    int res;
    rc = sqlite3BtreeFirst(pCursor, &res);
    if (!checkSqlite3Error(rc)) {
        sqlite3BtreeCloseCursor(pCursor);
        return 0;
    }

    PyObject *args = Py_BuildValue("OO", &PyUnicode_Type, &PyLong_Type);
    if (args == NULL) {
        sqlite3BtreeCloseCursor(pCursor);
        return 0;
    }

    PyObject *type = PyObject_CallFunction(py_db->genAlias, "OO", &PyTuple_Type, args);
    Py_DECREF(args);
    if (type == NULL) {
        sqlite3BtreeCloseCursor(pCursor);
        return 0;
    }

    while (res == 0) {
        i64 key;
        rc = sqlite3BtreeKeySize(pCursor, &key);
        if (!checkSqlite3Error(rc)) {
            Py_DECREF(type);
            sqlite3BtreeCloseCursor(pCursor);
            return 0;
        }
        
        PyObject *py_key = PyLong_FromLong(key);
        if (py_key == NULL) {
            Py_DECREF(type);
            sqlite3BtreeCloseCursor(pCursor);
            return 0;
        }

        u32 size;
        rc = sqlite3BtreeDataSize(pCursor,&size);
        if (!checkSqlite3Error(rc)) {
            Py_DECREF(py_key);
            Py_DECREF(type);
            sqlite3BtreeCloseCursor(pCursor);
            return 0;
        }

        buffer buf;
        buf.start = (uint8_t *) sqlite3BtreeDataFetch(pCursor, &size);
        buf.p     = buf.start;
        buf.end   = buf.start + size;

        PyObject *py_value = deserialize(py_db, type, &buf);
        if (py_value == NULL) {
            Py_DECREF(py_key);
            Py_DECREF(type);
            sqlite3BtreeCloseCursor(pCursor);
            return 0;
        }

        PyObject *name = PyTuple_GetItem(py_value, 0);
        PyObject *tnum = PyTuple_GetItem(py_value, 1);
        
        PyObject *pair = PyTuple_Pack(2, py_key, tnum);

        if (PyDict_SetItem(py_db->schema, name, pair) != 0) {
            Py_DECREF(py_value);
            Py_DECREF(py_key);
            Py_DECREF(type);
            sqlite3BtreeCloseCursor(pCursor);
            return 0;
        }

        Py_DECREF(py_value);
        Py_DECREF(py_key);

        rc = sqlite3BtreeNext(pCursor, &res);
        if (!checkSqlite3Error(rc)) {
            Py_DECREF(type);
            sqlite3BtreeCloseCursor(pCursor);
            return 0;
        }
    }

    Py_DECREF(type);

    rc = sqlite3BtreeCloseCursor(pCursor);
    if (!checkSqlite3Error(rc)) {
        return 0;
    }

    return 1;
}

static void
Trans_dealloc(TransObject *self)
{
    Py_XDECREF(self->db);
    Py_TYPE(self)->tp_free(self);
}

static TransObject *
Trans_enter(TransObject *self, PyObject *Py_UNUSED(ignored))
{
    int rc = sqlite3BtreeBeginTrans(self->db->pBtree, self->mode);
    if (!checkSqlite3Error(rc)) {
        return NULL;
    }

    if (!daison_fetchSchema(self->db)) {
        return NULL;
    }

    Py_INCREF((PyObject *) self);
    return self;
}

// cpython/Modules/_multiprocessing/clinic/semaphore.c.h
// cpython/Modules/_sqlite/connection.c
static PyObject *
Trans_exit(TransObject *self, PyObject *const *args, Py_ssize_t nargs)
{
    PyObject *exc_type = Py_None;
    PyObject *exc_value = Py_None;
    PyObject *exc_tb = Py_None;

    if (nargs < 0 || nargs > 3) {
        PyErr_Format(PyExc_TypeError, "%d arguments provided, 3 expected", nargs);
        return NULL;
    }
    if (nargs < 1) {
        goto skip_optional;
    }
    exc_type = args[0];
    if (nargs < 2) {
        goto skip_optional;
    }
    exc_value = args[1];
    if (nargs < 3) {
        goto skip_optional;
    }
    exc_tb = args[2];
skip_optional:

    if (exc_type == Py_None && exc_value == Py_None && exc_tb == Py_None) {
        int rc = sqlite3BtreeCommit(self->db->pBtree);
        if (!checkSqlite3Error(rc)) {
            return NULL;
        }

        Py_RETURN_NONE;
    } else {
        int rc = sqlite3BtreeRollback(self->db->pBtree, SQLITE_ABORT_ROLLBACK, 0);
        if (!checkSqlite3Error(rc)) {
            return NULL;
        }

        PyErr_SetObject(exc_type, exc_value);
        return NULL;
    }
}

static PyObject *
Trans_cursor(TransObject *self, PyObject *args);

static PyObject *
Trans_indexCursor(TransObject *self, PyObject *args);

static PyObject *
Trans_store(TransObject *self, PyObject *args);

static PyMethodDef Transaction_methods[] = {
    {"__enter__", (PyCFunction) Trans_enter, METH_NOARGS, ""},
    {"__exit__", (PyCFunction) Trans_exit, METH_FASTCALL, ""},
    {"cursor",  (void*)Trans_cursor,  METH_VARARGS,
     "Returns an iterator over a table or an index"},
    {"indexCursor",  (void*)Trans_indexCursor,  METH_VARARGS,
     "Returns an iterator over an index and returns a tuple of id and table value"},
    {"store",  (void*)Trans_store,  METH_VARARGS,
     "t.store(tbl,id,o) stores the object o with the given id "
     "in the table tbl under transaction t. If id is None, then "
     "a new id is generated. In all cases the method returns the id "
     "under which the object was stored."},
    {NULL}  /* Sentinel */
};

static PyTypeObject daison_TransactionType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    //0,                       /*ob_size*/
    "daison.Transaction",      /*tp_name*/
    sizeof(TransObject),       /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)Trans_dealloc, /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0, // (reprfunc) PGF_str,        /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
    "Transaction object",      /*tp_doc*/
    0,                         /*tp_traverse */
    0,                         /*tp_clear */
    0,                         /*tp_richcompare */
    0,                         /*tp_weaklistoffset */
    0,                         /*tp_iter */
    0,                         /*tp_iternext */
    Transaction_methods,       /*tp_methods */
    0,                         /*tp_members */
    0,                         /*tp_getset */
    0,                         /*tp_base */
    0,                         /*tp_dict */
    0,                         /*tp_descr_get */
    0,                         /*tp_descr_set */
    0,                         /*tp_dictoffset */
    0,                         /*tp_init */
    0,                         /*tp_alloc */
    0,                         /*tp_new */
};

static TransObject *
DB_run(DBObject *self, PyObject *args)
{
    const char *smode;
    if (!PyArg_ParseTuple(args, "s", &smode))
        return NULL;

    int mode;
    if (strcmp(smode, "r") == 0)
        mode = 0;
    else if (strcmp(smode, "w") == 0)
        mode = 1;
    else {
        PyErr_SetString(PyExc_TypeError, "the mode must be 'r' or 'w'");
        return NULL;
    }

    TransObject *py_trans = (TransObject *)daison_TransactionType.tp_alloc(&daison_TransactionType, 0);
    if (py_trans == NULL) {
        return NULL;
    }

    py_trans->db   = self;
    py_trans->mode = mode;
    Py_INCREF(py_trans->db);

    return py_trans;
}

static PyObject *
DB_close(DBObject *self, PyObject *Py_UNUSED(ignored))
{
    if (self->pBtree != NULL) {
        int rc = sqlite3BtreeClose(self->pBtree);
        if (!checkSqlite3Error(rc)) {
            return NULL;
        }
        self->pBtree = NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
DB_getJournalMode(DBObject *self, PyObject *Py_UNUSED(ignored))
{
    int eMode = sqlite3BtreeGetJournalMode(self->pBtree);
    if (eMode == PAGER_JOURNALMODE_DELETE)
        return PyUnicode_FromString("DELETE");
    else if (eMode == PAGER_JOURNALMODE_PERSIST)
        return PyUnicode_FromString("PERSIST");
    else if (eMode == PAGER_JOURNALMODE_OFF)
        return PyUnicode_FromString("OFF");
    else if (eMode == PAGER_JOURNALMODE_TRUNCATE)
        return PyUnicode_FromString("TRUNCATE");
    else if (eMode == PAGER_JOURNALMODE_MEMORY)
        return PyUnicode_FromString("MEMORY");
    else if (eMode == PAGER_JOURNALMODE_WAL)
        return PyUnicode_FromString("WAL");
    else {
        PyErr_SetString(DBError, "The journal mode must be one of the following - DELETE, PERSIST, OFF, TRUNCATE, MEMORY or WAL");
        return NULL;
    }
}

static PyObject *
DB_setJournalMode(DBObject *self, PyObject *mode)
{
    int eMode;
    if (PyUnicode_CompareWithASCIIString(mode, "DELETE") == 0)
        eMode = PAGER_JOURNALMODE_DELETE;
    else if (PyUnicode_CompareWithASCIIString(mode, "PERSIST") == 0)
        eMode = PAGER_JOURNALMODE_PERSIST;
    else if (PyUnicode_CompareWithASCIIString(mode, "OFF") == 0)
        eMode = PAGER_JOURNALMODE_OFF;
    else if (PyUnicode_CompareWithASCIIString(mode, "TRUNCATE") == 0)
        eMode = PAGER_JOURNALMODE_TRUNCATE;
    else if (PyUnicode_CompareWithASCIIString(mode, "MEMORY") == 0)
        eMode = PAGER_JOURNALMODE_MEMORY;
    else if (PyUnicode_CompareWithASCIIString(mode, "WAL") == 0)
        eMode = PAGER_JOURNALMODE_WAL;
    else {
        PyErr_SetString(DBError, "The journal mode must be one of the following - DELETE, PERSIST, OFF, TRUNCATE, MEMORY or WAL");
        return NULL;
    }

    int rc = sqlite3BtreeSetJournalMode(self->pBtree, eMode);
    if (!checkSqlite3Error(rc)) {
        return NULL;
    }

    Py_RETURN_NONE;
}

static void
DB_dealloc(DBObject *self)
{
    DB_close(self, NULL);
    Py_XDECREF(self->enumMeta);
    Py_XDECREF(self->unionGenAlias);
    Py_XDECREF(self->genAlias);
    Py_XDECREF(self->schema);
    Py_TYPE(self)->tp_free(self);
}

static PyObject *
DB_deserialize(DBObject *self, PyObject *args)
{
    PyObject *py_type, *py_capsule;
    if (!PyArg_ParseTuple(args, "O!O", &PyType_Type, &py_type, &py_capsule))
        return NULL;

    buffer *buf = PyCapsule_GetPointer(py_capsule, "daison-buffer");
    if (buf == NULL)
        return NULL;

    return deserialize(self, py_type, buf);
}

static PyObject *
DB_serialize(DBObject *self, PyObject *args)
{
    PyObject *py_type, *py_obj, *py_capsule;
    if (!PyArg_ParseTuple(args, "O!OO", &PyType_Type, &py_type, &py_obj, &py_capsule))
        return NULL;

    buffer *buf = PyCapsule_GetPointer(py_capsule, "daison-buffer");
    if (buf == NULL)
        return NULL;

    serialize(self, py_type, py_obj, buf);
    Py_RETURN_NONE;
}

static DBObject *
DB_enter(DBObject *self, PyObject *Py_UNUSED(ignored))
{
    Py_INCREF(self);
    return self;
}

static PyMethodDef DB_methods[] = {
    {"run",  (void*)DB_run,  METH_VARARGS,
     "Runs a transaction over the database"},
    {"setJournalMode",  (void*)DB_setJournalMode,  METH_O,
     "Changes the journal mode"},
    {"getJournalMode",  (void*)DB_getJournalMode,  METH_NOARGS,
     "Retrieves the current journal mode"},
    {"close",  (void*)DB_close,  METH_NOARGS,
     "Closes a Daison database"},
    {"__serialize__", (PyCFunction) DB_serialize, METH_VARARGS, ""},
    {"__deserialize__", (PyCFunction) DB_deserialize, METH_VARARGS, ""},
    {"__enter__", (PyCFunction) DB_enter, METH_NOARGS, ""},
    {"__exit__", (PyCFunction) DB_close, METH_VARARGS, ""},
    {NULL}  /* Sentinel */
};

static PyMemberDef DB_members[] = {
    {"__schema__", T_OBJECT, offsetof(DBObject, schema), READONLY, NULL},
    {NULL}  /* Sentinel */
};

static PyTypeObject daison_DBType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    //0,                        /*ob_size*/
    "daison.Database",         /*tp_name*/
    sizeof(DBObject),          /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)DB_dealloc,    /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0, // (reprfunc) PGF_str,        /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
    "Database object",         /*tp_doc*/
    0,                         /*tp_traverse */
    0,                         /*tp_clear */
    0,                         /*tp_richcompare */
    0,                         /*tp_weaklistoffset */
    0,                         /*tp_iter */
    0,                         /*tp_iternext */
    DB_methods,                /*tp_methods */
    DB_members,                /*tp_members */
    0,                         /*tp_getset */
    0,                         /*tp_base */
    0,                         /*tp_dict */
    0,                         /*tp_descr_get */
    0,                         /*tp_descr_set */
    0,                         /*tp_dictoffset */
    0,                         /*tp_init */
    0,                         /*tp_alloc */
    0,                         /*tp_new */
};

static PyObject *
daison_openDB(PyObject *self, PyObject *args)
{
    const char *fpath;
    if (!PyArg_ParseTuple(args, "s", &fpath))
        return NULL;

    DBObject *py_db = (DBObject *)daison_DBType.tp_alloc(&daison_DBType, 0);
    py_db->pBtree = NULL;
    py_db->cookie = -1;
    py_db->schema = NULL;
    py_db->genAlias = NULL;
    py_db->unionGenAlias = NULL;
    py_db->enumMeta = NULL;

    int rc;
    rc = sqlite3BtreeOpen(NULL, fpath, &py_db->pBtree, 0, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB);
    if (!checkSqlite3Error(rc)) {
        Py_DECREF(py_db);
        return NULL;
    }

    py_db->schema = PyDict_New();
    if (py_db->schema == NULL) {
        Py_DECREF(py_db);
        return NULL;
    }

    PyObject *types = PyImport_ImportModule("types");
    if (types == NULL) {
        Py_DECREF(py_db);
        return NULL;
    }

    py_db->genAlias = PyDict_GetItemString(PyModule_GetDict(types), "GenericAlias");
    Py_DECREF(types);
    if (py_db->genAlias == NULL) {
        Py_DECREF(py_db);
        return NULL;
    }

    PyObject *typing = PyImport_ImportModule("typing");
    if (typing == NULL) {
        Py_DECREF(py_db);
        return NULL;
    }

    py_db->unionGenAlias = PyDict_GetItemString(PyModule_GetDict(typing), "_UnionGenericAlias");
    Py_DECREF(typing);
    if (py_db->unionGenAlias == NULL) {
        Py_DECREF(py_db);
        return NULL;
    }

    PyObject *enum_ = PyImport_ImportModule("enum");
    if (enum_ == NULL) {
        Py_DECREF(py_db);
        return NULL;
    }

    py_db->enumMeta = PyDict_GetItemString(PyModule_GetDict(enum_), "EnumMeta");
    Py_DECREF(enum_);
    if (py_db->enumMeta == NULL) {
        Py_DECREF(py_db);
        return NULL;
    }

    rc = sqlite3BtreeBeginTrans(py_db->pBtree,0);
    if (!checkSqlite3Error(rc)) {
        Py_DECREF(py_db);
        return NULL;
    }

    if (!daison_fetchSchema(py_db)) {
        Py_DECREF(py_db);
        return NULL;
    }

    rc = sqlite3BtreeCommit(py_db->pBtree);
    if (!checkSqlite3Error(rc)) {
        Py_DECREF(py_db);
        return NULL;
    }

    return (PyObject*) py_db;
}

static TableObject*
Table_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    TableObject* self = (TableObject *)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->name = NULL;
        self->type = NULL;
        self->indices = NULL;
    }

    return self;
}

static void
Table_dealloc(TableObject *self)
{
    Py_XDECREF(self->name);
    Py_XDECREF(self->type);
    Py_XDECREF(self->indices);
    Py_TYPE(self)->tp_free(self);
}

static int
Table_init(TableObject *self, PyObject *args, PyObject *kwds)
{
    if (!PyArg_ParseTuple(args, "UO", &self->name, &self->type)) {
        return -1;
    }
    Py_INCREF(self->name);
    Py_INCREF(self->type);

    self->indices = PyList_New(0);
    if (self->indices == NULL)
        return -1;

    return 0;
}

static PyObject *
Table_addIndex(TableObject *self, PyObject *args, PyObject *kwds);

static IndexObject*
Index_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    IndexObject* self = (IndexObject *)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->table = NULL;
        self->name  = NULL;
        self->fun   = NULL;
    }

    return self;
}

static void
Index_dealloc(IndexObject *self)
{
    Py_XDECREF(self->table);
    Py_XDECREF(self->name);
    Py_XDECREF(self->fun);
    Py_TYPE(self)->tp_free(self);
}

static int
Index_init(IndexObject *self, PyObject *args, PyObject *kwds);

static PyMethodDef Table_methods[] = {
    {"addIndex",  (void*)Table_addIndex,  METH_VARARGS,
     "Adds an index to the table."},
    {NULL}  /* Sentinel */
};

static PyMemberDef Table_members[] = {
    {"name", T_OBJECT, offsetof(TableObject, name), READONLY, NULL},
    {NULL}  /* Sentinel */
};

static PyTypeObject daison_TableType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    //0,                       /*ob_size*/
    "daison.table",            /*tp_name*/
    sizeof(TableObject),       /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)Table_dealloc, /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
    "Table object",            /*tp_doc*/
    0,                         /*tp_traverse */
    0,                         /*tp_clear */
    0,                         /*tp_richcompare */
    0,                         /*tp_weaklistoffset */
    0,                         /*tp_iter */
    0,                         /*tp_iternext */
    Table_methods,             /*tp_methods */
    Table_members,             /*tp_members */
    0,                         /*tp_getset */
    0,                         /*tp_base */
    0,                         /*tp_dict */
    0,                         /*tp_descr_get */
    0,                         /*tp_descr_set */
    0,                         /*tp_dictoffset */
    (initproc)Table_init,      /*tp_init */
    0,                         /*tp_alloc */
    (newfunc)Table_new,        /*tp_init */
};

static PyMemberDef Index_members[] = {
    {"table", T_OBJECT, offsetof(IndexObject, table), READONLY, NULL},
    {"name",  T_OBJECT, offsetof(IndexObject, name),  READONLY, NULL},
    {"fun",   T_OBJECT, offsetof(IndexObject, fun),   READONLY, NULL},
    {NULL}  /* Sentinel */
};

static PyTypeObject daison_IndexType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    //0,                       /*ob_size*/
    "daison.index",            /*tp_name*/
    sizeof(IndexObject),       /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)Index_dealloc, /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
    "Index object",            /*tp_doc*/
    0,                         /*tp_traverse */
    0,                         /*tp_clear */
    0,                         /*tp_richcompare */
    0,                         /*tp_weaklistoffset */
    0,                         /*tp_iter */
    0,                         /*tp_iternext */
    0,                         /*tp_methods */
    Index_members,             /*tp_members */
    0,                         /*tp_getset */
    0,                         /*tp_base */
    0,                         /*tp_dict */
    0,                         /*tp_descr_get */
    0,                         /*tp_descr_set */
    0,                         /*tp_dictoffset */
    (initproc)Index_init,      /*tp_init */
    0,                         /*tp_alloc */
    (newfunc)Index_new,        /*tp_new */
};

static PyObject *
Table_addIndex(TableObject *self, PyObject *args, PyObject *kwds)
{
    PyObject *index = NULL;
    if (!PyArg_ParseTuple(args, "O!", &daison_IndexType, &index)) {
        return NULL;
    }

    if (PyList_Append(self->indices, index) != 0)
        return NULL;

    Py_RETURN_NONE;
}

static int
Index_init(IndexObject *self, PyObject *args, PyObject *kwds)
{
    PyObject *name; 
    if (!PyArg_ParseTuple(args, "O!UOO", &daison_TableType, &self->table, &name, &self->fun, &self->type)) {
        return -1;
    }

    Py_INCREF(self->table);
    Py_INCREF(self->fun);
    Py_INCREF(self->type);

    self->kind = SIMPLE_INDEX;
    self->name = PyUnicode_FromFormat("%U_%U", ((TableObject *) self->table)->name, name);
    if (self->name == NULL)
        return -1;

    return 0;
}

static PyObject *
daison_listIndex(PyObject *self, PyObject *args, PyObject *kwds)
{
    IndexObject* py_index = (IndexObject *)daison_IndexType.tp_alloc(&daison_IndexType, 0);
    if (py_index == NULL) {
        return NULL;
    }

    PyObject *name; 
    if (!PyArg_ParseTuple(args, "O!UOO", &daison_TableType, &py_index->table, &name, &py_index->fun, &py_index->type)) {
        Py_DECREF(py_index);
        return NULL;
    }

    Py_INCREF(py_index->table);
    Py_INCREF(py_index->fun);
    Py_INCREF(py_index->type);

    py_index->kind = LIST_INDEX;
    py_index->name = PyUnicode_FromFormat("%U_%U", ((TableObject *) py_index->table)->name, name);
    if (py_index->name == NULL) {
        Py_DECREF(py_index);
        return NULL;
    }

    return (PyObject *) py_index;
}

static PyObject *
daison_maybeIndex(PyObject *self, PyObject *args, PyObject *kwds)
{
    IndexObject* py_index = (IndexObject *)daison_IndexType.tp_alloc(&daison_IndexType, 0);
    if (py_index == NULL) {
        return NULL;
    }

    PyObject *name; 
    if (!PyArg_ParseTuple(args, "O!UOO", &daison_TableType, &py_index->table, &name, &py_index->fun, &py_index->type)) {
        Py_DECREF(py_index);
        return NULL;
    }

    Py_INCREF(py_index->table);
    Py_INCREF(py_index->fun);
    Py_INCREF(py_index->type);

    py_index->kind = MAYBE_INDEX;
    py_index->name = PyUnicode_FromFormat("%U_%U", ((TableObject *) py_index->table)->name, name);
    if (py_index->name == NULL) {
        Py_DECREF(py_index);
        return NULL;
    }

    return (PyObject *) py_index;
}

static PyObject *
Table_cursor_everything(DBObject *db, TableObject *table)
{
    int rc;

    PyObject *py_info = PyDict_GetItem(db->schema, table->name);
    if (PyErr_Occurred())
        return NULL;

    if (py_info == NULL) {
        PyErr_Format(DBError, "Table %U does not exist", table->name);
        return NULL;
    }

    PyObject *py_tnum = PyTuple_GetItem(py_info, 1);
    if (py_tnum == NULL)
        return NULL;
    int tnum = PyLong_AsLong(py_tnum);

    rc = sqlite3BtreeLockTable(db->pBtree, tnum, 0);
    if (!checkSqlite3Error(rc)) {
        return NULL;
    }

    BtCursor *pCursor = NULL;
    rc = sqlite3BtreeCursor(db->pBtree, tnum, 0, 0, 0, &pCursor);
    if (!checkSqlite3Error(rc)) {
        return NULL;
    }

    int res;
    rc = sqlite3BtreeFirst(pCursor, &res);
    if (!checkSqlite3Error(rc)) {
        sqlite3BtreeCloseCursor(pCursor);
        return NULL;
    }

    PyObject *list = PyList_New(0);
    while (res == 0) {
        i64 id;
        rc = sqlite3BtreeKeySize(pCursor, &id);
        if (!checkSqlite3Error(rc)) {
            Py_DECREF(list);
            list = NULL;
        }

        u32 payloadSize;
        rc = sqlite3BtreeDataSize(pCursor, &payloadSize);
        if (!checkSqlite3Error(rc)) {
            Py_DECREF(list);
            list = NULL;
            break;
        }

        buffer buf;
        buf.start = malloc(payloadSize);
        buf.p     = buf.start;
        buf.end   = buf.start+payloadSize;

        rc = sqlite3BtreeData(pCursor, 0, payloadSize, buf.start);
        if (!checkSqlite3Error(rc)) {
            free(buf.start);
            Py_DECREF(list);
            list = NULL;
        }

        PyObject *py_value =
            deserialize(db, table->type, &buf);

        free(buf.start);

        PyObject *pair = PyTuple_New(2);
        if (pair == NULL) {
            Py_DECREF(py_value);
            Py_DECREF(list);
            list = NULL;
            break;
        }
        PyTuple_SetItem(pair, 0, PyLong_FromLong(id));
        PyTuple_SetItem(pair, 1, py_value);

        PyList_Append(list, pair);
        Py_DECREF(pair);

        rc = sqlite3BtreeNext(pCursor, &res);
        if (!checkSqlite3Error(rc)) {
            Py_DECREF(list);
            list = NULL;
            break;
        }
    }

    sqlite3BtreeCloseCursor(pCursor);

    return list;
}

static PyObject *
Index_cursor_everything(DBObject *db, IndexObject *index)
{
    int rc;

    PyObject *py_info = PyDict_GetItem(db->schema, index->name);
    if (PyErr_Occurred())
        return NULL;

    if (py_info == NULL) {
        PyErr_Format(DBError, "Index %U does not exist", index->name);
        return NULL;
    }

    PyObject *py_tnum = PyTuple_GetItem(py_info, 1);
    if (py_tnum == NULL)
        return NULL;
    int tnum = PyLong_AsLong(py_tnum);

    rc = sqlite3BtreeLockTable(db->pBtree, tnum, 0);
    if (!checkSqlite3Error(rc)) {
        return NULL;
    }

    BtCursor *pCursor = NULL;
    rc = sqlite3BtreeCursor(db->pBtree, tnum, 0, 1, 1, &pCursor);
    if (!checkSqlite3Error(rc)) {
        return NULL;
    }

    int res;
    rc = sqlite3BtreeFirst(pCursor, &res);
    if (!checkSqlite3Error(rc)) {
        sqlite3BtreeCloseCursor(pCursor);
        return NULL;
    }

    PyObject *list = PyList_New(0);
    while (res == 0) {
        i64 payloadSize;
        rc = sqlite3BtreeKeySize(pCursor, &payloadSize);
        if (!checkSqlite3Error(rc)) {
            Py_DECREF(list);
            list = NULL;
        }

        buffer buf;
        buf.start = (uint8_t*) malloc(payloadSize);
        buf.p     = buf.start;
        buf.end   = buf.start+payloadSize;

        rc = sqlite3BtreeKey(pCursor, 0, payloadSize, buf.start);
        if (!checkSqlite3Error(rc)) {
            free(buf.start);
            Py_DECREF(list);
            list = NULL;
        }

        PyObject *value = deserialize(db, index->type, &buf);
        if (value == NULL) {
            free(buf.start);
            Py_DECREF(list);
            list = NULL;
            break;
        }

        PyObject *keys  = deserializeIds(&buf);
        if (keys == NULL) {
            free(buf.start);
            Py_DECREF(value);
            Py_DECREF(list);
            list = NULL;
            break;
        }

        free(buf.start);

        PyObject *pair = PyTuple_New(2);
        if (pair == NULL) {
            Py_DECREF(value);
            Py_DECREF(keys);
            Py_DECREF(list);
            list = NULL;
            break;
        }
        PyTuple_SetItem(pair, 0, value);
        PyTuple_SetItem(pair, 1, keys);

        PyList_Append(list, pair);
        Py_DECREF(pair);
        
        rc = sqlite3BtreeNext(pCursor, &res);
        if (!checkSqlite3Error(rc)) {
            Py_DECREF(list);
            list = NULL;
            break;
        }
    }

    sqlite3BtreeCloseCursor(pCursor);

    return list;
}

static PyObject *
Table_cursor_at(DBObject *db, TableObject *table, PyObject *py_key)
{
    int rc;

    i64 key = PyLong_AsLong(py_key);
    if (PyErr_Occurred())
        return NULL;

    PyObject *py_info = PyDict_GetItem(db->schema, table->name);
    if (PyErr_Occurred())
        return NULL;

    if (py_info == NULL) {
        PyErr_Format(DBError, "Table %U does not exist", table->name);
        return NULL;
    }

    PyObject *py_tnum = PyTuple_GetItem(py_info, 1);
    if (py_tnum == NULL)
        return NULL;
    int tnum = PyLong_AsLong(py_tnum);

    rc = sqlite3BtreeLockTable(db->pBtree, tnum, 0);
    if (!checkSqlite3Error(rc)) {
        return NULL;
    }

    BtCursor *pCursor = NULL;
    rc = sqlite3BtreeCursor(db->pBtree, tnum, 0, 0, 0, &pCursor);
    if (!checkSqlite3Error(rc)) {
        return NULL;
    }

    int res;
    rc = sqlite3BtreeMoveTo(pCursor, NULL, key, 0, &res);
    if (!checkSqlite3Error(rc)) {
        sqlite3BtreeCloseCursor(pCursor);
        return NULL;
    }

    if (res != 0) {
        return PyList_New(0);
    }

    u32 payloadSize;
    rc = sqlite3BtreeDataSize(pCursor, &payloadSize);
    if (!checkSqlite3Error(rc)) {
        sqlite3BtreeCloseCursor(pCursor);
        return NULL;
    }

    buffer buf;
    buf.start = alloca(payloadSize);
    buf.p     = buf.start;
    buf.end   = buf.start+payloadSize;

    rc = sqlite3BtreeData(pCursor, 0, payloadSize, buf.start);
    if (!checkSqlite3Error(rc)) {
        sqlite3BtreeCloseCursor(pCursor);
        return NULL;
    }

    PyObject *py_value =
        deserialize(db, table->type, &buf);

    sqlite3BtreeCloseCursor(pCursor);

    if (py_value == NULL)
        return NULL;

    PyObject *py_values = PyList_New(1);
    if (py_values == NULL) {
        Py_DECREF(py_value);
        return NULL;
    }

    if (PyList_SetItem(py_values, 0, py_value) != 0) {
        Py_DECREF(py_value);
        Py_DECREF(py_values);
        return NULL;
    }

    return py_values;
}

static PyObject *
Index_cursor_at(DBObject *db, IndexObject *index, PyObject *key)
{
    int rc;

    PyObject *py_info = PyDict_GetItem(db->schema, index->name);
    if (PyErr_Occurred())
        return NULL;

    if (py_info == NULL) {
        PyErr_Format(DBError, "Index %U does not exist", index->name);
        return NULL;
    }

    PyObject *py_tnum = PyTuple_GetItem(py_info, 1);
    if (py_tnum == NULL)
        return NULL;
    int tnum = PyLong_AsLong(py_tnum);

    rc = sqlite3BtreeLockTable(db->pBtree, tnum, 0);
    if (!checkSqlite3Error(rc)) {
        return NULL;
    }

    BtCursor *pCursor = NULL;
    rc = sqlite3BtreeCursor(db->pBtree, tnum, 0, 1, 1, &pCursor);
    if (!checkSqlite3Error(rc)) {
        return NULL;
    }
    
    buffer buf = { NULL, NULL, NULL };
    if (!serialize(db, index->type, key, &buf)) {
        free(buf.start);
        sqlite3BtreeCloseCursor(pCursor);
        return NULL;
    }

    int res;
    i64 indexSize = buf.p - buf.start;
    rc = sqlite3BtreeMoveTo(pCursor, buf.start, indexSize, 0, &res);
    free(buf.start);
    if (!checkSqlite3Error(rc)) {
        sqlite3BtreeCloseCursor(pCursor);
        return NULL;
    }

    if (res != 0) {
        return PyList_New(0);
    }

    i64 payloadSize;
    rc = sqlite3BtreeKeySize(pCursor, &payloadSize);
    if (!checkSqlite3Error(rc)) {
        sqlite3BtreeCloseCursor(pCursor);
        return NULL;
    }

    i64 size = payloadSize-indexSize;
    buf.start = alloca(size);
    buf.p     = buf.start;
    buf.end   = buf.start+size;

    rc = sqlite3BtreeKey(pCursor, indexSize, size, buf.start);
    if (!checkSqlite3Error(rc)) {
        sqlite3BtreeCloseCursor(pCursor);
        return NULL;
    }

    PyObject *keys = deserializeIds(&buf);

    sqlite3BtreeCloseCursor(pCursor);

    return keys;
}

static PyObject *
TableIndex_cursor_at(DBObject *db, IndexObject *index, PyObject *key)
{
    int rc;

    PyObject *py_index_info = PyDict_GetItem(db->schema, index->name);
    if (PyErr_Occurred())
        return NULL;

    if (py_index_info == NULL) {
        PyErr_Format(DBError, "Index %U does not exist", index->name);
        return NULL;
    }

    PyObject *py_index_tnum = PyTuple_GetItem(py_index_info, 1);
    if (py_index_tnum == NULL)
        return NULL;
    int index_tnum = PyLong_AsLong(py_index_tnum);

    rc = sqlite3BtreeLockTable(db->pBtree, index_tnum, 0);
    if (!checkSqlite3Error(rc)) {
        return NULL;
    }

    TableObject *py_table = (TableObject *)index->table;
    PyObject *py_table_info = PyDict_GetItem(db->schema, py_table->name);
    if (PyErr_Occurred())
        return NULL;

    if (py_table_info == NULL) {
        PyErr_Format(DBError, "Table %U does not exist", py_table->name);
        return NULL;
    }

    PyObject *py_table_tnum = PyTuple_GetItem(py_table_info, 1);
    if (py_table_info == NULL)
        return NULL;
    int table_tnum = PyLong_AsLong(py_table_tnum);

    rc = sqlite3BtreeLockTable(db->pBtree, table_tnum, 0);
    if (!checkSqlite3Error(rc)) {
        return NULL;
    }


    BtCursor *pIndexCursor = NULL;
    rc = sqlite3BtreeCursor(db->pBtree, index_tnum, 0, 1, 1, &pIndexCursor);
    if (!checkSqlite3Error(rc)) {
        return NULL;
    }

    BtCursor *pTableCursor = NULL;
    rc = sqlite3BtreeCursor(db->pBtree, table_tnum, 0, 0, 0, &pTableCursor);
    if (!checkSqlite3Error(rc)) {
        sqlite3BtreeCloseCursor(pIndexCursor);
        return NULL;
    }

    buffer buf = { NULL, NULL, NULL };
    if (!serialize(db, index->type, key, &buf)) {
        free(buf.start);
        sqlite3BtreeCloseCursor(pIndexCursor);
        sqlite3BtreeCloseCursor(pTableCursor);
        return NULL;
    }

    int res;
    i64 indexSize = buf.p - buf.start;
    rc = sqlite3BtreeMoveTo(pIndexCursor, buf.start, indexSize, 0, &res);
    free(buf.start);
    if (!checkSqlite3Error(rc)) {
        sqlite3BtreeCloseCursor(pIndexCursor);
        sqlite3BtreeCloseCursor(pTableCursor);
        return NULL;
    }

    if (res != 0) {
        sqlite3BtreeCloseCursor(pIndexCursor);
        sqlite3BtreeCloseCursor(pTableCursor);
        return PyList_New(0);
    }

    i64 payloadSize;
    rc = sqlite3BtreeKeySize(pIndexCursor, &payloadSize);
    if (!checkSqlite3Error(rc)) {
        sqlite3BtreeCloseCursor(pIndexCursor);
        sqlite3BtreeCloseCursor(pTableCursor);
        return NULL;
    }

    i64 size = payloadSize-indexSize;
    buf.start = alloca(size);
    buf.p     = buf.start;
    buf.end   = buf.start+size;

    rc = sqlite3BtreeKey(pIndexCursor, indexSize, size, buf.start);
    if (!checkSqlite3Error(rc)) {
        sqlite3BtreeCloseCursor(pIndexCursor);
        sqlite3BtreeCloseCursor(pTableCursor);
        return NULL;
    }

    PyObject *py_values = PyList_New(0);
    if (py_values == NULL) {
        sqlite3BtreeCloseCursor(pIndexCursor);
        sqlite3BtreeCloseCursor(pTableCursor);
        return NULL;
    }

    while (buf.p < buf.end) {
        int64_t key = getRest(0, 0, &buf);
        if (PyErr_Occurred()) {
            sqlite3BtreeCloseCursor(pIndexCursor);
            sqlite3BtreeCloseCursor(pTableCursor);
            Py_DECREF(py_values);
            return NULL;
        }

        rc = sqlite3BtreeMoveTo(pTableCursor, NULL, key, 0, &res);
        if (!checkSqlite3Error(rc) || res != 0) {
            sqlite3BtreeCloseCursor(pIndexCursor);
            sqlite3BtreeCloseCursor(pTableCursor);
            Py_DECREF(py_values);
            return NULL;
        }

        u32 payloadSize;
        rc = sqlite3BtreeDataSize(pTableCursor, &payloadSize);
        if (!checkSqlite3Error(rc)) {
            sqlite3BtreeCloseCursor(pIndexCursor);
            sqlite3BtreeCloseCursor(pTableCursor);
            Py_DECREF(py_values);
            return NULL;
        }

        buffer tbuf;
        tbuf.start = malloc(payloadSize);
        tbuf.p     = tbuf.start;
        tbuf.end   = tbuf.start+payloadSize;

        rc = sqlite3BtreeData(pTableCursor, 0, payloadSize, tbuf.start);
        if (!checkSqlite3Error(rc)) {
            free(tbuf.start);
            sqlite3BtreeCloseCursor(pIndexCursor);
            sqlite3BtreeCloseCursor(pTableCursor);
            Py_DECREF(py_values);
            return NULL;
        }

        PyObject *py_key   = PyLong_FromLong(key);
        PyObject *py_value = deserialize(db, py_table->type, &tbuf);
        free(tbuf.start);
        if (py_key == NULL || py_value == NULL) {
            sqlite3BtreeCloseCursor(pIndexCursor);
            sqlite3BtreeCloseCursor(pTableCursor);
            Py_XDECREF(py_value);
            Py_XDECREF(py_key);
            Py_DECREF(py_values);
            return NULL;
        }

        PyObject *py_tuple = PyTuple_Pack(2, py_key, py_value);
        Py_DECREF(py_value);
        Py_DECREF(py_key);
        if (py_tuple == NULL) {
            sqlite3BtreeCloseCursor(pIndexCursor);
            sqlite3BtreeCloseCursor(pTableCursor);
            Py_DECREF(py_tuple);
            Py_DECREF(py_values);
            return NULL;
        }

        if (PyList_Append(py_values, py_tuple) != 0) {
            Py_DECREF(py_key);
            Py_DECREF(py_values);
            return NULL;
        }
        Py_DECREF(py_tuple);
    }

    sqlite3BtreeCloseCursor(pIndexCursor);
    sqlite3BtreeCloseCursor(pTableCursor);

    return py_values;
}

static PyObject *
Trans_cursor(TransObject *self, PyObject *args)
{
    Py_ssize_t n_args = PyTuple_Size(args);
    if (n_args == 1) {
        PyObject *source = PyTuple_GetItem(args,0);

        if (PyObject_IsInstance(source, (PyObject *) &daison_TableType)) {
            return Table_cursor_everything(self->db, (TableObject *) source);
        } else if (PyObject_IsInstance(source, (PyObject *) &daison_IndexType)) {
            return Index_cursor_everything(self->db, (IndexObject *) source);
        } else {
            PyErr_SetString(PyExc_TypeError, "the first argument must be a table or an index");
            return NULL;
        }
    } else if (n_args == 2) {
        PyObject *source = PyTuple_GetItem(args,0);
        PyObject *key    = PyTuple_GetItem(args,1);

        if (PyObject_IsInstance(source, (PyObject *) &daison_TableType)) {
            return Table_cursor_at(self->db, (TableObject *) source, key);
        } else if (PyObject_IsInstance(source, (PyObject *) &daison_IndexType)) {
            return Index_cursor_at(self->db, (IndexObject *) source, key);
        } else {
            PyErr_SetString(PyExc_TypeError, "the first argument must be a table or an index");
            return NULL;
        }
    } else {
        PyErr_SetString(PyExc_TypeError, "function takes 1 or 2 arguments");
        return NULL;
    }
}

static PyObject *
Trans_indexCursor(TransObject *self, PyObject *args)
{
    Py_ssize_t n_args = PyTuple_Size(args);
    if (n_args == 2) {
        PyObject *source = PyTuple_GetItem(args,0);
        PyObject *key    = PyTuple_GetItem(args,1);

        if (PyObject_IsInstance(source, (PyObject *) &daison_IndexType)) {
            return TableIndex_cursor_at(self->db, (IndexObject *) source, key);
        } else {
            PyErr_SetString(PyExc_TypeError, "the first argument must be an index");
            return NULL;
        }
    } else {
        PyErr_SetString(PyExc_TypeError, "function takes 2 arguments");
        return NULL;
    }
}

static int
updateIndicesHelper(DBObject *db, int tnum, buffer *buf, i64 id,
                    int (*update)(i64 id, buffer *buf))
{
    int rc;

    BtCursor *pCursor = NULL;
    rc = sqlite3BtreeCursor(db->pBtree, tnum, 1, 1, 1, &pCursor);
    if (!checkSqlite3Error(rc)) {
        free(buf->start);
        return 0;
    }

    int res;
    i64 indexSize = buf->p - buf->start;
    rc = sqlite3BtreeMoveTo(pCursor, buf->start, indexSize, 0, &res);
    if (!checkSqlite3Error(rc)) {
        free(buf->start);
        sqlite3BtreeCloseCursor(pCursor);
        return 0;
    }

    if (res == 0) {
        free(buf->start);

        i64 payloadSize;
        rc = sqlite3BtreeKeySize(pCursor, &payloadSize);
        if (!checkSqlite3Error(rc)) {
            sqlite3BtreeCloseCursor(pCursor);
            return 0;
        }

        buf->start = malloc(payloadSize);
        buf->p     = buf->start+indexSize;
        buf->end   = buf->start+payloadSize;

        if (buf->start == NULL) {
            sqlite3BtreeCloseCursor(pCursor);
            PyErr_NoMemory();
            return 0;
        }

        rc = sqlite3BtreeKey(pCursor, 0, payloadSize, buf->start);
        if (!checkSqlite3Error(rc)) {
            free(buf->start);
            sqlite3BtreeCloseCursor(pCursor);
            return 0;
        }
    }

    if (!update(id, buf)) {
        free(buf->start);
        sqlite3BtreeCloseCursor(pCursor);
        return 0;
    }

    if (buf->p==buf->start+indexSize) {
        if (res == 0)
            rc = sqlite3BtreeDelete(pCursor, 0);
        else
            rc = SQLITE_OK;
    } else {
        rc = sqlite3BtreeInsert(pCursor, buf->start, buf->p-buf->start, NULL, 0, 0, 0, 0);
    }

    free(buf->start);

    if (!checkSqlite3Error(rc)) {
        sqlite3BtreeCloseCursor(pCursor);
        return 0;
    }

    sqlite3BtreeCloseCursor(pCursor);
    return 1;
}

static int
updateIndices(DBObject *db,
              TableObject *table, i64 id, PyObject *obj,
              int (*update)(i64 id, buffer *buf))
{
    int rc;

    PyObject *args = PyTuple_New(1);
    if (args == NULL) {
        Py_DECREF(obj);
        return 0;
    }
    Py_INCREF(obj);
    PyTuple_SET_ITEM(args, 0, obj);

    Py_ssize_t n_indices = PyList_GET_SIZE(table->indices);
    for (Py_ssize_t i = 0; i < n_indices; i++) {
        IndexObject *index = (IndexObject*) PyList_GET_ITEM(table->indices, i);

        PyObject *py_info = PyDict_GetItem(db->schema, index->name);
        if (PyErr_Occurred()) {
            Py_DECREF(args);
            return 0;
        }

        if (py_info == NULL) {
            PyErr_Format(DBError, "Index %U does not exist", index->name);
            Py_DECREF(args);
            return 0;
        }

        int tnum = PyLong_AsLong(PyTuple_GET_ITEM(py_info, 1));
        rc = sqlite3BtreeLockTable(db->pBtree, tnum, 1);
        if (!checkSqlite3Error(rc)) {
            Py_DECREF(args);
            return 0;
        }

        PyObject *key = PyObject_CallObject(index->fun, args);
        if (key == NULL) {
            Py_DECREF(args);
            return 0;
        }

        if (index->kind == LIST_INDEX) {
            PyObject *iterator = PyObject_GetIter(key);
            if (iterator == NULL) {
                Py_DECREF(key);
                Py_DECREF(args);
                return 0;
            }

            PyObject *item;
            while ((item = PyIter_Next(iterator))) {
                buffer buf = { NULL, NULL, NULL };
                if (!serialize(db, index->type, item, &buf)) {
                    free(buf.start);
                    Py_DECREF(item);
                    Py_DECREF(iterator);
                    Py_DECREF(key);
                    Py_DECREF(args);
                    return 0;
                }

                Py_DECREF(item);

                if (!updateIndicesHelper(db, tnum, &buf, id, update)) {
                    Py_DECREF(iterator);
                    Py_DECREF(key);
                    Py_DECREF(args);
                    return 0;
                }
            }

            Py_DECREF(iterator);
            Py_DECREF(key);
        } else {
            if (index->kind == MAYBE_INDEX && key==Py_None) {
                Py_DECREF(key);
                continue;
            }

            buffer buf = { NULL, NULL, NULL };
            if (!serialize(db, index->type, key, &buf)) {
                free(buf.start);
                Py_DECREF(key);
                Py_DECREF(args);
                return 0;
            }

            Py_DECREF(key);

            if (!updateIndicesHelper(db, tnum, &buf, id, update)) {
                Py_DECREF(args);
                return 0;
            }
        }
    }

    Py_DECREF(args);
    return 1;
}

static PyObject *
Trans_store(TransObject *self, PyObject *args)
{
    TableObject *table;
    PyObject *py_id, *obj;
    if (!PyArg_ParseTuple(args, "O!OO", &daison_TableType, (PyObject**)&table,
                                        &py_id, &obj))
        return NULL;

    int rc;

    PyObject *py_info = PyDict_GetItem(self->db->schema, table->name);
    if (PyErr_Occurred())
        return NULL;

    if (py_info == NULL) {
        PyErr_Format(DBError, "Table %U does not exist", table->name);
        return NULL;
    }

    int tnum = PyLong_AsLong(PyTuple_GET_ITEM(py_info, 1));

    rc = sqlite3BtreeLockTable(self->db->pBtree, tnum, 1);
    if (!checkSqlite3Error(rc)) {
        return NULL;
    }

    BtCursor *pCursor = NULL;
    rc = sqlite3BtreeCursor(self->db->pBtree, tnum, 1, 0, 0, &pCursor);
    if (!checkSqlite3Error(rc)) {
        return NULL;
    }

    i64 id;
    int res;
    if (py_id == Py_None) {
        rc = sqlite3BtreeLast(pCursor, &res);
        if (!checkSqlite3Error(rc)) {
            return NULL;
        }

        if (res != 0)
            id = 1;
        else {
            rc = sqlite3BtreeKeySize(pCursor, &id);
            if (!checkSqlite3Error(rc)) {
                return NULL;
            }

            id++;
        }
        py_id = PyLong_FromLong(id);
    } else {
        id = PyLong_AsLong(py_id);
        if (PyErr_Occurred()) {
            sqlite3BtreeCloseCursor(pCursor);
            return NULL;
        }

        rc = sqlite3BtreeMoveTo(pCursor, NULL, id, 0, &res);
        if (!checkSqlite3Error(rc)) {
            return NULL;
        }

        if (res == 0) {
            u32 payloadSize;
            rc = sqlite3BtreeDataSize(pCursor, &payloadSize);
            if (!checkSqlite3Error(rc)) {
                return NULL;
            }

            buffer buf;
            buf.start = (uint8_t*) alloca(payloadSize);
            buf.p     = buf.start;
            buf.end   = buf.start+payloadSize;

            rc = sqlite3BtreeData(pCursor, 0, payloadSize, buf.start);
            if (!checkSqlite3Error(rc)) {
                return 0;
            }

            PyObject *obj = deserialize(self->db, table->type, &buf);
            if (obj == NULL) {
                return 0;
            }

            if (!updateIndices(self->db, table, id, obj, deleteId)) {
                Py_DECREF(obj);
                return NULL;
            }
            Py_DECREF(obj);
        }

        Py_INCREF(py_id);
    }

    buffer buf = { NULL, NULL, NULL };
    if (!serialize(self->db, table->type, obj, &buf)) {
        free(buf.start);
        Py_DECREF(py_id);
        sqlite3BtreeCloseCursor(pCursor);
        return NULL;
    }

    rc = sqlite3BtreeInsert(pCursor, NULL, id, buf.start, buf.p-buf.start, 0, 0, 0);
    free(buf.start);
    sqlite3BtreeCloseCursor(pCursor);

    if (!checkSqlite3Error(rc)) {
        Py_DECREF(py_id);
        return NULL;
    }

    if (!updateIndices(self->db, table, id, obj, insertId)) {
        Py_DECREF(py_id);
        return NULL;
    }

    return py_id;
}

static PyMethodDef module_methods[] = {
    {"openDB",  (void*)daison_openDB,  METH_VARARGS,
     "Opens a Daison database"},
    {"listIndex",  (void*)daison_listIndex,  METH_VARARGS,
     "Creates an index object where a record is indexed by a list of keys"},
    {"maybeIndex",  (void*)daison_maybeIndex,  METH_VARARGS,
     "Creates an index object where a record is indexed by zero or one keys"},
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

PyMODINIT_FUNC PyInit_daison(void)
{
    static struct PyModuleDef moduledef = {
        PyModuleDef_HEAD_INIT, "daison", "An interface to Daison databases from Python", -1, module_methods 
    };
    PyObject *m = PyModule_Create(&moduledef);

    DBError = PyErr_NewException("daison.DatabaseError", NULL, NULL);
    if (PyModule_AddObject(m, "DatabaseError", (PyObject *)DBError) < 0) {
        Py_DECREF(DBError);
        Py_DECREF(m);
        return NULL;
    }

    if (PyType_Ready(&daison_DBType) < 0)
        return NULL;
    if (PyModule_AddObject(m, "Database", (PyObject *)&daison_DBType) < 0) {
        Py_DECREF(&daison_DBType);
        Py_DECREF(m);
        return NULL;
    }

    if (PyType_Ready(&daison_TransactionType) < 0)
        return NULL;
    if (PyModule_AddObject(m, "Transaction", (PyObject *)&daison_TransactionType) < 0) {
        Py_DECREF(&daison_TransactionType);
        Py_DECREF(m);
        return NULL;
    }

    if (PyType_Ready(&daison_TableType) < 0)
        return NULL;
    if (PyModule_AddObject(m, "table", (PyObject *)&daison_TableType) < 0) {
        Py_DECREF(&daison_TableType);
        Py_DECREF(m);
        return NULL;
    }

    if (PyType_Ready(&daison_IndexType) < 0)
        return NULL;
    if (PyModule_AddObject(m, "index", (PyObject *)&daison_IndexType) < 0) {
        Py_DECREF(&daison_IndexType);
        Py_DECREF(m);
        return NULL;
    }

    return m;
}
