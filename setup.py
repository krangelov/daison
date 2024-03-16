from distutils.core import setup, Extension

daison_module = Extension(
    'daison',
    sources = [
        'python/pydaison.c',
        'c/sqlite3Btree.c'
    ])

setup(
    name = 'daison',
    version = '0.4',
    description = 'An interface to Daison databases from Python',
    author='Krasimir Angelov',
    author_email='kr.angelov@gmail.com',
    license='BSD',
    ext_modules = [daison_module])
