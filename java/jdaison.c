#ifndef __MINGW32__
#include <alloca.h>
#else
#include <malloc.h>
#endif
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <jni.h>
#include "sqlite3Btree.h"

#define l2p(x) ((void*) (intptr_t) (x))
#define p2l(x) ((jlong) (intptr_t) (x))

#if defined(__STDC_ISO_10646__) && WCHAR_MAX >= 0x10FFFF
#include <wchar.h>
#define GU_UCS_WCHAR
typedef wchar_t GuUCS;
#else
typedef int32_t GuUCS;
#endif

static GuUCS
gu_utf8_decode(const uint8_t** src_inout)
{
	const uint8_t* src = *src_inout;
	uint8_t c = src[0];
	if (c < 0x80) {
		*src_inout = src + 1;
		return (GuUCS) c;
	}
	size_t len = (c < 0xe0 ? 1 :
	              c < 0xf0 ? 2 :
	              c < 0xf8 ? 3 :
	              c < 0xfc ? 4 :
	                         5
	             );
	uint64_t mask = 0x0103070F1f7f;
	uint32_t u = c & (mask >> (len * 8));
	for (size_t i = 1; i <= len; i++) {
		c = src[i];
		u = u << 6 | (c & 0x3f);
	}
	*src_inout = &src[len + 1];
	return (GuUCS) u;
}

static jstring
c2j_string(JNIEnv *env, const char* s) {
	const char* utf8 = s;
	size_t len = strlen(s);

	jchar* utf16 = alloca(len*sizeof(jchar));
	jchar* dst   = utf16;
	while (s-utf8 < len) {
		GuUCS ucs = gu_utf8_decode((const uint8_t**) &s);

		if (ucs <= 0xFFFF) {
			*dst++ = ucs;
		} else {
			ucs -= 0x10000;
			*dst++ = 0xD800+((ucs >> 10) & 0x3FF);
			*dst++ = 0xDC00+(ucs & 0x3FF);
		}
	}

	return (*env)->NewString(env, utf16, dst-utf16);
}

static void*
get_ref(JNIEnv *env, jobject self) {
	jfieldID refId = (*env)->GetFieldID(env, (*env)->GetObjectClass(env, self), "ref", "J");
	return l2p((*env)->GetLongField(env, self, refId));
}

static void
throw_rc_exception(JNIEnv *env, int rc)
{
	jclass exception_class = (*env)->FindClass(env, "org/daison/DaisonException");
	if (!exception_class)
		return;
	jmethodID constrId = (*env)->GetMethodID(env, exception_class, "<init>", "(I)V");
	if (!constrId)
		return;
	jobject exception = (*env)->NewObject(env, exception_class, constrId, rc);
	if (!exception)
		return;
	(*env)->Throw(env, exception);
}

JNIEXPORT void JNICALL Java_org_daison_Database_open
  (JNIEnv *env, jobject self, jstring path)
{
	int rc;

	const char *fpath = (*env)->GetStringUTFChars(env, path, 0); 

	Btree* pBtree = NULL;
	rc = sqlite3BtreeOpen(NULL, fpath, &pBtree, 0, 
	                      SQLITE_OPEN_READWRITE | 
	                      SQLITE_OPEN_CREATE | 
	                      SQLITE_OPEN_MAIN_DB);

	(*env)->ReleaseStringUTFChars(env, path, fpath);
	
	if (rc != SQLITE_OK) {
		throw_rc_exception(env, rc);
		return;
	}

	jfieldID refId = (*env)->GetFieldID(env, (*env)->GetObjectClass(env, self), "ref", "J");
	(*env)->SetLongField(env, self, refId, p2l(pBtree));
}

JNIEXPORT jstring JNICALL Java_org_daison_DaisonException_errName
  (JNIEnv *env, jclass cls, jint rc)
{
	return c2j_string(env, sqlite3BtreeErrName(rc));
}

JNIEXPORT void JNICALL Java_org_daison_Database_fetchSchema
  (JNIEnv *env, jobject self)
{
	int rc;
	Btree* pBtree = get_ref(env, self);
	sqlite3BtreeLockTable(pBtree,1,0);

	u32 cookie;
    sqlite3BtreeGetMeta(pBtree, 1, &cookie);

    jclass self_class = (*env)->GetObjectClass(env, self);

   	jfieldID cookieId = (*env)->GetFieldID(env, self_class, "cookie", "J");
	u32 old_cookie = (u32) (*env)->GetLongField(env, self, cookieId);

	if (cookie == old_cookie)
		return;

	jmethodID registerTableId = (*env)->GetMethodID(env, self_class, "registerTable", "(JJI)V");

   	jfieldID schemaId = (*env)->GetFieldID(env, self_class, "schema", "Ljava/util/Map;");
	jobject schema = (*env)->GetObjectField(env, self, schemaId);
	jmethodID clearId = (*env)->GetMethodID(env, (*env)->GetObjectClass(env, schema), "clear", "()V");
	(*env)->CallVoidMethod(env, schema, clearId);

	BtCursor* pCursor;
	rc = sqlite3BtreeCursor(pBtree, 1, 0, 0, 0, &pCursor);
	if (rc != SQLITE_OK) {
		throw_rc_exception(env, rc);
		return;
	}

	int res;
	rc = sqlite3BtreeFirst(pCursor, &res);
	if (rc != SQLITE_OK) {
		sqlite3BtreeCloseCursor(pCursor);
		throw_rc_exception(env, rc);
		return;
	}

	while (res == 0) {
		i64 key;
		rc = sqlite3BtreeKeySize(pCursor, &key);
		if (rc != SQLITE_OK) {
			sqlite3BtreeCloseCursor(pCursor);
			throw_rc_exception(env, rc);
			return;
		}

		u32 size;
        rc = sqlite3BtreeDataSize(pCursor, &size);
		if (rc != SQLITE_OK) {
			sqlite3BtreeCloseCursor(pCursor);
			throw_rc_exception(env, rc);
			return;
		}

        const void *ptr = 
			sqlite3BtreeDataFetch(pCursor, &size);

		(*env)->CallVoidMethod(env, self, registerTableId, key, p2l(ptr), size);
		if ((*env)->ExceptionCheck(env)) {
			sqlite3BtreeCloseCursor(pCursor);
			return;
		}

		rc = sqlite3BtreeNext(pCursor, &res);
		if (rc != SQLITE_OK) {
			sqlite3BtreeCloseCursor(pCursor);
			throw_rc_exception(env, rc);
			return;
		}
	}

	rc = sqlite3BtreeCloseCursor(pCursor);
	if (rc != SQLITE_OK) {
		throw_rc_exception(env, rc);
		return;
	}
}

JNIEXPORT void JNICALL Java_org_daison_Database_close
  (JNIEnv *env, jobject self)
{
	Btree* btree = get_ref(env, self);
	if (btree != NULL)
		sqlite3BtreeClose(btree);
}

JNIEXPORT void JNICALL Java_org_daison_Database_openTransaction
  (JNIEnv *env, jobject self, jint wr)
{
	int rc;
	Btree* btree = get_ref(env, self);	
	rc = sqlite3BtreeBeginTrans(btree, wr);
	if (rc != SQLITE_OK)
		throw_rc_exception(env,rc);
}

JNIEXPORT void JNICALL Java_org_daison_Database_commitTransaction
  (JNIEnv *env, jobject self)
{
	int rc;
	Btree* btree = get_ref(env, self);
	rc = sqlite3BtreeCommit(btree);
	if (rc != SQLITE_OK)
		throw_rc_exception(env,rc);
}

JNIEXPORT void JNICALL Java_org_daison_Database_rollbackTransaction
  (JNIEnv *env, jobject self)
{
	int rc;
	Btree* btree = get_ref(env, self);	
	rc = sqlite3BtreeRollback(btree, SQLITE_ABORT_ROLLBACK, 0);
	if (rc != SQLITE_OK)
		throw_rc_exception(env,rc);
}


JNIEXPORT jlong JNICALL Java_org_daison_Database_openCursor
  (JNIEnv *env, jobject self, jint tnum, jint mode, jint n, jint x)
{
	int rc;
	Btree* btree = get_ref(env, self);

	rc = sqlite3BtreeLockTable(btree, tnum, mode);
	if (rc != SQLITE_OK) {
		throw_rc_exception(env, rc);
		return 0;
	}

	BtCursor* pCursor = NULL;
	rc = sqlite3BtreeCursor(btree, tnum, mode, n, x, &pCursor);
	if (rc != SQLITE_OK) {
		throw_rc_exception(env, rc);
		return 0;
	}

	return p2l(pCursor);
}

JNIEXPORT jint JNICALL Java_org_daison_Database_cursorMoveTo
  (JNIEnv *env, jobject self, jlong cursorRef, jlong bufRef, jlong bufSize)
{
	int rc;
	BtCursor* pCursor = (BtCursor*) l2p(cursorRef);

	int res;
	rc = sqlite3BtreeMoveTo(pCursor, l2p(bufRef), bufSize, 0, &res);
	if (rc != SQLITE_OK) {
		throw_rc_exception(env, rc);
		return 0;
	}

	return res;
}

JNIEXPORT jint JNICALL Java_org_daison_Database_getKeySize
  (JNIEnv *env, jobject self, jlong cursorRef)
{
	int rc;
	BtCursor* pCursor = (BtCursor*) l2p(cursorRef);

	i64 size;
	rc = sqlite3BtreeKeySize(pCursor, &size);
	if (rc != SQLITE_OK) {
		throw_rc_exception(env, rc);
		return 0;
	}

	return (int) size;
}

JNIEXPORT jlong JNICALL Java_org_daison_Database_getKey
  (JNIEnv *env, jobject self, jlong cursorRef, jint offset, jint amt)
{
	int rc;
	BtCursor* pCursor = (BtCursor*) l2p(cursorRef);

	void *buffer = malloc(amt);
	rc = sqlite3BtreeKey(pCursor, offset, amt, buffer);
	if (rc != SQLITE_OK) {
		free(buffer);
		throw_rc_exception(env, rc);
		return 0;
	}

	return p2l(buffer);
}

JNIEXPORT jint JNICALL Java_org_daison_Database_getDataSize
  (JNIEnv *env, jobject self, jlong cursorRef)
{
	int rc;
	BtCursor* pCursor = (BtCursor*) l2p(cursorRef);

	int size;
	rc = sqlite3BtreeDataSize(pCursor, &size);
	if (rc != SQLITE_OK) {
		throw_rc_exception(env, rc);
		return 0;
	}

	return size;
}

JNIEXPORT jlong JNICALL Java_org_daison_Database_getData
  (JNIEnv *env, jobject self, jlong cursorRef, jint offset, jint amt)
{
	int rc;
	BtCursor* pCursor = (BtCursor*) l2p(cursorRef);

	void *buffer = malloc(amt);
	rc = sqlite3BtreeData(pCursor, offset, amt, buffer);
	if (rc != SQLITE_OK) {
		free(buffer);
		throw_rc_exception(env, rc);
		return 0;
	}

	return p2l(buffer);
}

JNIEXPORT void JNICALL Java_org_daison_Database_closeCursor
  (JNIEnv *env, jobject self, jlong cursorRef)
{
	int rc;
	rc = sqlite3BtreeCloseCursor((BtCursor*) l2p(cursorRef));
	if (rc != SQLITE_OK)
		throw_rc_exception(env, rc);
}

JNIEXPORT jbyte JNICALL Java_org_daison_DataStream_peekByte
  (JNIEnv *env, jclass cls, jlong addr, jint offs)
{
	return *((unsigned char*) addr+offs);
}

JNIEXPORT jdouble JNICALL Java_org_daison_DataStream_peekDouble
  (JNIEnv *env, jclass cls, jlong addr, jint offs)
{
	return *((double*) addr+offs);
}

JNIEXPORT jfloat JNICALL Java_org_daison_DataStream_peekFloat
  (JNIEnv *env, jclass cls, jlong addr, jint offs)
{
	return *((float*) addr+offs);
}

JNIEXPORT jlong JNICALL Java_org_daison_DataStream_realloc
  (JNIEnv *env, jclass cls, jlong addr, jint size)
{
	return p2l(realloc(l2p(addr), size));
}

JNIEXPORT void JNICALL Java_org_daison_DataStream_pokeByte
  (JNIEnv *env, jclass cls, jlong addr, jint offs, jbyte b)
{
	*((unsigned char*) addr+offs) = b;
}

JNIEXPORT void JNICALL Java_org_daison_DataStream_pokeDouble
  (JNIEnv *env, jclass cls, jlong addr, jint offs, jdouble d)
{
	*((double*) addr+offs) = d;
}

JNIEXPORT void JNICALL Java_org_daison_DataStream_pokeFloat
  (JNIEnv *env, jclass cls, jlong addr, jint offs, jfloat f)
{
	*((float*) addr+offs) = f;
}

JNIEXPORT void JNICALL Java_org_daison_DataStream_free
  (JNIEnv *env, jclass clas, jlong addr)
{
	free(l2p(addr));
}
