# -*- coding: utf-8 -*-
from threading import Lock
from sqlalchemy.engine import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import visitors
from sqlalchemy.sql import operators
from sqlalchemy.sql.schema import MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.horizontal_shard import ShardedSession
from sqlalchemy.orm.session import Session
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.orm.scoping import scoped_session


__all__ = ['QKSQLAlchemy', 'QKSession', 'QKShardSession']

_DEFAULT_SHARD_ID = ''


class _scoped_session(scoped_session):
    """
    支持 db.session(bind_key) 方式获取指定连接的 session，以兼容旧的代码。
    建议显视使用 db.get_session(bind_key) 来获取指定的 session
    """
    def __call__(self, *args, **kwargs):
        if args:
            # 只取第一个位置参数作为 bind_key，其他参数忽略
            return getattr(self.registry(), 'db').get_session(args[0])
        return super(_scoped_session, self).__call__(**kwargs)


class QKSession(Session):
    def __init__(self, db, **kwargs):
        super(QKSession, self).__init__(**kwargs)
        self.db = db


class QKShardSession(ShardedSession):
    def __init__(self, db, **kwargs):
        super(QKShardSession, self).__init__(
            shard_chooser=db.shard_chooser,
            id_chooser=db.id_chooser,
            query_chooser=db.query_chooser,
            shards=None,
            **kwargs
        )
        self.db = db

    def get_bind(self, mapper, shard_id=None,
                 instance=None, clause=None, **kw):
        """
        返回需要的数据库分片连接。
          - 如果指定了 shard_id，直接返回
          - 否则通过 shard_chooser() 计算需要的连接
        """
        if shard_id is None:
            shard_id = self.shard_chooser(mapper, instance, clause=clause)

        if shard_id is _DEFAULT_SHARD_ID:
            # 如果 shard_chooser 返回 _DEFAULT_SHARD_ID，依然使用缺省的数据库连接
            return self.db.get_engine()
        else:
            return self.db.get_engine(shard_id)

    def bind_shard(self, shard_id, bind):
        pass


class QKSQLAlchemy(object):
    """
    ```
    db = QKSQLAlchemy()
    db.configure({...})
    User = db.reflect_model('user')
    users = db.session.query(User).filter(User.id<10, User.status==0).all()
    for user in users:
        print(user.id, user.display_name)
    ```

    ----

    获取 session 的方法：
      - db.session 返回缺省 session
      - db.get_session(bind_key) 返回指定 session
      - db.session(bind_key) 同 db.get_session(...)

    获取 engine 的方法：
      - db.engine
      - dg.get_engine(bind_key)

    bind_key 不应为空字符串

    ----

    数据库横向拆分
    ```
    db.configure({
        'SQLALCHEMY_ENABLE_SHARD': True,
        'SQLALCHEMY_BINDS': {
            'shard_001': '...',
            'shard_002': '...',
            'shard_003': '...'
        }
    })
    db.shard_chooser = ...
    db.id_chooser = ...
    db.query_chooser = ...

    db.session.(...)
    ```

    通过改变 shard_chooser, id_chooser, query_chooser 方法，自定义数据库拆分实现
    缺省的（db.session）才支持 shard，命名的 session 不执行 chooser 方法
    """
    def __init__(self):
        self._config = {}
        self._config.setdefault('SQLALCHEMY_DATABASE_URI', None)
        self._config.setdefault('SQLALCHEMY_BINDS', None)
        self._config.setdefault('SQLALCHEMY_ENABLE_SHARD', False)
        self._config.setdefault('SQLALCHEMY_ENABLE_POOL', False)
        self._config.setdefault('SQLALCHEMY_POOL_SIZE', 1)
        self._config.setdefault('SQLALCHEMY_POOL_TIMEOUT', 30)
        self._config.setdefault('SQLALCHEMY_POOL_RECYCLE', 60)
        self._config.setdefault('SQLALCHEMY_MAX_OVERFLOW', 10)
        self._config.setdefault('SQLALCHEMY_ECHO', True)

        # 数据库链接引擎是多个请求间的 Session 共享的
        self._engines = {}
        self._engine_lock = Lock()

        self._sessions = {}
        self._session_lock = Lock()

        # auto mapped models
        self._tables = {}
        self._models = {}
        self._reflect_lock = Lock()

        # scopefunc
        self.scopefunc = None

        # shard
        self.shard_chooser = _shard_chooser
        self.id_chooser = _id_chooser
        self.query_chooser = _query_chooser

    @property
    def config(self):
        return self._config

    def configure(self, config=None, **kwargs):
        """
        - SQLALCHEMY_DATABASE_URI: uri
        - SQLALCHEMY_BINDS: {bind_key: uri}
        - SQLALCHEMY_ENABLE_SHARD: False
        - SQLALCHEMY_ENABLE_POOL: False
        - SQLALCHEMY_POOL_SIZE: 1
        - SQLALCHEMY_POOL_TIMEOUT: 30
        - SQLALCHEMY_POOL_RECYCLE: 60
        - SQLALCHEMY_MAX_OVERFLOW: 10
        - SQLALCHEMY_ECHO: True
        """
        if config:
            self._config.update(config)
        if kwargs:
            self._config.update(kwargs)

    def reset(self):
        for session in self._sessions.values():
            session.remove()

    def create_session(self, engine=None, shard=False):
        class_ = QKShardSession if shard else QKSession
        factory = sessionmaker(
            db=self, bind=engine, class_=class_,
            autocommit=False, autoflush=False
        )
        return _scoped_session(factory, scopefunc=self.scopefunc)

    def get_session(self, bind_key=None):
        """ 通过 bind_key 获取 db session
        """
        with self._session_lock:
            if bind_key in self._sessions:
                return self._sessions[bind_key]
            engine = self.get_engine(bind_key)
            if bind_key is None and self._config['SQLALCHEMY_ENABLE_SHARD']:
                session = self.create_session(engine, True)
            else:
                session = self.create_session(engine)
            self._sessions[bind_key] = session
            return session

    @property
    def session(self):
        return self.get_session(bind_key=None)

    def create_engine(self, uri):
        options = {}
        if self._config['SQLALCHEMY_ENABLE_POOL']:
            options.update({
                'pool_size': self._config['SQLALCHEMY_POOL_SIZE'],
                'pool_timeout': self._config['SQLALCHEMY_POOL_TIMEOUT'],
                'pool_recycle': self._config['SQLALCHEMY_POOL_RECYCLE'],
                'max_overflow': self._config['SQLALCHEMY_MAX_OVERFLOW']
            })
        else:
            options.update({'poolclass': NullPool})

        if self._config['SQLALCHEMY_ECHO']:
            options.update({'echo': True, 'echo_pool': True})
        return create_engine(uri, **options)

    def get_engine(self, bind_key=None):
        """ 通过 bind_key 获取 db engine

        if bind_key
          - is None: 通过 SQLALCHEMY_DATABASE_URI 获取 engine
          - is not None: 通过 SQLALCHEMY_BINDS 对应的 key 获取 engine
        """
        with self._engine_lock:
            if bind_key in self._engines:
                return self._engines[bind_key]
            elif bind_key is None:
                uri = self._config['SQLALCHEMY_DATABASE_URI']
            else:
                uri = self._config['SQLALCHEMY_BINDS'][bind_key]
            if uri is None:
                return None
            engine = self.create_engine(uri)
            self._engines[bind_key] = engine
            return engine

    @property
    def engine(self):
        return self.get_engine(bind_key=None)

    def reflect_model(self, table_name, bind_key=None):
        """ 反向生成 ORM 的 Model
        :param table_name:
        :param bind_key:
        :return: ORMClass
        """
        with self._reflect_lock:
            if table_name in self._models:
                return self._models[table_name]

            engine = self.get_engine(bind_key)
            meta = MetaData(bind=engine)
            meta.reflect(only=[table_name])

            table = meta.tables[table_name]
            self._tables[table_name] = table

            Base = automap_base(metadata=meta)
            Base.prepare()

            model = getattr(Base.classes, table_name)
            model.__table__.metadata = None
            self._models[table_name] = model

            return model

    def reflect_table(self, table_name, bind_key=None):
        with self._reflect_lock:
            if table_name in self._tables:
                return self._tables[table_name]

            engine = self.get_engine(bind_key)
            meta = MetaData(bind=engine)
            meta.reflect(only=[table_name])

            table = meta.tables[table_name]
            table.metadata = None
            self._tables[table_name] = table

            return table


#
# 横向拆分缺省实现
# 返回 _DEFAULT_SHARD_ID 表示使用缺省连接（SQLALCHEMY_DATABASE_URI），兼容非分片的实现
#

def _shard_chooser(mapper, instance, clause=None):
    """ A callable which, passed a Mapper, a mapped
        instance, and possibly a SQL clause, returns a shard ID.  This id
        may be based off of the attributes present within the object, or on
        some round-robin scheme. If the scheme is based on a selection, it
        should set whatever state on the instance to mark it in the future as
        participating in that shard.
    """
    return _DEFAULT_SHARD_ID


def _id_chooser(query, ident):
    """ A callable, passed a query and a tuple of identity
        values, which should return a list of shard ids where the ID might
        reside.  The databases will be queried in the order of this listing.
    """
    return [_DEFAULT_SHARD_ID]


def _query_chooser(query):
    """ For a given Query, returns the list of shard_ids
      where the query should be issued.  Results from all shards returned
      will be combined together into a single listing.
    """
    return [_DEFAULT_SHARD_ID]

    shard_ids = []
    for column, operator, value in _get_query_comparisons(query):
        pass

    if len(shard_ids) == 0:
        return [_DEFAULT_SHARD_ID]
    else:
        return shard_ids


# http://docs.sqlalchemy.org/en/latest/_modules/examples/sharding/attribute_shard.html
def _get_query_comparisons(query):
    """Search an orm.Query object for binary expressions.

    Returns expressions which match a Column against one or more
    literal values as a list of tuples of the form
    (column, operator, values).   "values" is a single value
    or tuple of values depending on the operator.

    """
    binds = {}
    clauses = set()
    comparisons = []

    def visit_bindparam(bind):
        # visit a bind parameter.

        # check in _params for it first
        if bind.key in query._params:
            value = query._params[bind.key]
        elif bind.callable:
            # some ORM functions (lazy loading)
            # place the bind's value as a
            # callable for deferred evaluation.
            value = bind.callable()
        else:
            # just use .value
            value = bind.value

        binds[bind] = value

    def visit_column(column):
        clauses.add(column)

    def visit_binary(binary):
        # special handling for "col IN (params)"
        if binary.left in clauses and \
                binary.operator == operators.in_op and \
                hasattr(binary.right, 'clauses'):
            comparisons.append(
                (binary.left, binary.operator,
                    tuple(binds[bind] for bind in binary.right.clauses)
                )
            )
        elif binary.left in clauses and binary.right in binds:
            comparisons.append(
                (binary.left, binary.operator,binds[binary.right])
            )

        elif binary.left in binds and binary.right in clauses:
            comparisons.append(
                (binary.right, binary.operator,binds[binary.left])
            )

    # here we will traverse through the query's criterion, searching
    # for SQL constructs.  We will place simple column comparisons
    # into a list.
    if query._criterion is not None:
        visitors.traverse_depthfirst(query._criterion, {},
                    {'bindparam':visit_bindparam,
                        'binary':visit_binary,
                        'column':visit_column
                    }
        )
    return comparisons
