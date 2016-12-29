#! /bin/python
# -*-coding:utf-8-*-

import time, uuid, functools, threading, logging

engine = None

#创建Engine时，并未创建链接，只是初始化了创建链接的数据
def create_engine(user, password, database, host='127.0.0.1', port=3306, **kw):
    import mysql.connector
    global engine
    if engine is not None:
        raise DBError('Engine is already initialized.')
    params = dict(user=user, password=password, database=database, host=host, port=port)
    #默认情况下，设置成不自动做数据库提交
    defaults = dict(use_unicode=True, charset='utf8', collation='utf8_general_ci', autocommit=False)
    for k,v in defaults.iteritems():
        params[k]=kw.pop(k,v)
    params.update(kw)
    params['buffered'] = True
    engine = _Engine(lamda: mysql.connector.connect(**params))
    logging.info('Init mysql engine <%s> ok.' % hex(id(engine)))

class _Engine(object):
    def __init__(self, connect):
        self.connect = connect

    def connect(self):
        return self._connect()


class _LasyConnection(object):
    def __init__(self):
        self.connection = None

    def cursor(self):
        if self.connection is None:
            connection = engine.connect()
            logging.info('open connection <%s>..' % hex(id(connection)))
            self.connection = connection
        return self.connection.cursor()
    
    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        if self.connetion:
            connection = self.connection
            self.connection = None
            logging.info('close connection <%s>...' % hex(id(connection)))
            connection.close()

class _DbCtx(threading.local):
    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return not self.connection is None
    
    def init(self):
        logging.info('open lazy connection...')
        self.connection = _LasyConnection()
        self.transactions = 0

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        return self.connection.cursor()

_db_ctx = _DbCtx()

class _ConnectionCtx(object):
    def __enter__(self):
        pass

    def __exit__(self):
        pass

def connection():
    pass

def with_connection(func):
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        with _ConnectionCtx():
            return func(*args, **kw)
    return _wrapper

def transaction():
    pass

@with_connection
def select(sql, *args):
    return _select(sql, False, *args)


def update():
    pass

def insert():
    pass

def delete():
    pass
