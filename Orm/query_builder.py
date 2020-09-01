# -*- coding:utf-8 -*-
from Orm.tool import str_f, BaseEnum, list_diff


class WhereType(BaseEnum):
    TYPE_SIMPLE = 'simple'
    TYPE_IN = 'in'
    TYPE_NOT_IN = 'not_in'
    TYPE_NULL = 'null'
    TYPE_NOT_NULL = 'not_null'
    TYPE_BETWEEN = 'between'
    TYPE_NEST = 'nest'


class WhereArgStructErr(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, args, kwargs)


class QueryBuilder(object):
    operator = [
        "=", '<>', "in", "not in"
    ]

    TYPE_SIMPLE = 'simple'
    TYPE_IN = 'in'
    TYPE_NOT_IN = 'not_in'
    TYPE_NULL = 'null'
    TYPE_NOT_NULL = 'not_null'
    TYPE_BETWEEN = 'between'
    TYPE_NEST = 'nest'

    JOIN_INNER = 'INNER'
    JOIN_LEFT = 'LEFT'
    JOIN_RIGHT = 'RIGHT'

    SQL_TYPE_QUERY = "query"
    SQL_TYPE_UPDATE = "update"
    SQL_TYPE_INSERT = "insert"
    SQL_TYPE_DELETE = "delete"

    def __init__(self):
        self._model = None
        self._table = None
        self._field = ['*']
        self._join = None
        self._where = []
        self._group = None
        self._order = None
        self._limit = None
        self._update_field = None
        self._insert_data = None
        self._is_debug = False
        self._sql_type = self.SQL_TYPE_QUERY

    def set_debug(self, value):
        self._is_debug = value
        return self

    @classmethod
    def _get_where_type(cls):
        return [cls.TYPE_SIMPLE, cls.TYPE_IN, cls.TYPE_NOT_IN, cls.TYPE_NULL, cls.TYPE_NOT_NULL, cls.TYPE_BETWEEN,
                cls.TYPE_NEST]

    @classmethod
    def _get_join_type(cls):
        return [cls.JOIN_INNER, cls.JOIN_LEFT, cls.JOIN_RIGHT, ]

    def set_model(self, model):
        self._model = model
        return self

    def table(self, table):
        self._table = table
        return self

    @classmethod
    def process_where(cls, args):
        """
        处理where
        结构说明：
            simple:
                is_and, field, value, operator
        :param args:
        :return:
        """
        if not isinstance(args, list) or len(args) < 3:
            raise WhereArgStructErr(str_f("where 入参结构异常: {}", args))

        where_type = args[0]
        is_and, = args[-1],
        params = args[1:-1]
        if not isinstance(is_and, bool):
            raise WhereArgStructErr(str_f("and 类型必须为bool, 入参: {} and:{}", args, is_and))

        result = {
            "type": where_type,
            "is_and": is_and,
        }

        if where_type == cls.TYPE_NEST:
            if not params or not isinstance(params[0], list):
                raise WhereArgStructErr(
                    str_f("嵌套类型，值必须是数组 入参: {} value:{} 类型：{}", args, params[0], str(type(params[0]))))
            result['value'] = []
            for x in params[0]:
                value = cls.process_where(x)
                if value:
                    result['value'].append(value)
        elif where_type == cls.TYPE_SIMPLE:
            if len(params) != 3:
                raise WhereArgStructErr(args)

            field, operator, value = params
            if not cls.check_field(field) or operator not in cls.operator:
                raise WhereArgStructErr(str_f("字段类型异常或操作符异常 入参: {} 字段:{} 操作符：{}", args, field, operator))
            result.update({
                "field": field,
                "operator": operator,
                "value": value,
            })
        elif where_type in [cls.TYPE_BETWEEN, cls.TYPE_IN, cls.TYPE_NOT_IN]:
            if len(params) != 2:
                raise WhereArgStructErr(str_f("value结构异常 入参: {} value:{}", args, params))
            field, value = params

            if not cls.check_field(field) or not isinstance(value, list):
                raise WhereArgStructErr(str_f("字段类型异常或值类型异常value结构异常 入参: {} 字段：{} value:{}", args, field, params))
            if value:
                if where_type == cls.TYPE_BETWEEN:
                    if len(value) < 2:
                        raise WhereArgStructErr(str_f("between value结构异常 入参: {} value:{}", args, value))
                    value = value[0:2]
                result.update({
                    "field": field,
                    "value": value,
                })
            else:
                result = None
        elif where_type in [cls.TYPE_NULL, cls.TYPE_NOT_NULL]:
            if len(params) != 1:
                raise WhereArgStructErr(str_f("value结构异常 入参: {} value:{}", args, params))
            field = params[0]
            if not cls.check_field(field):
                raise WhereArgStructErr(str_f("字段结构异常 入参: {} value:{}", args, field))
            result.update({
                "field": field,
            })
        else:
            raise WhereArgStructErr(str_f("错误的where类型 入参: {} 类型:{}", args, where_type))

        return result

    @classmethod
    def check_field(cls, field):
        if not isinstance(field, list):
            field = [field]
        for x in field:
            if not isinstance(x, (str,)):
                raise WhereArgStructErr(str_f('数据格式异常，无效的字段：{}', x))
        return True

    @classmethod
    def check_value(cls, value):
        if not isinstance(value, list):
            value = [value]
        for x in value:
            if not isinstance(x, (str, float, int)):
                raise WhereArgStructErr(str_f('数据格式异常，无效的值：{}', x))
        return True

    def where(self, *args):
        if len(args) == 1:
            for x in args[0]:
                where = self.process_where(x)
                if where:
                    self._where.append(where)
        else:
            args = list(args)
            if len(args) < 3:
                raise WhereArgStructErr(args)
            if len(args) == 3:
                args.append(True)
            field = args[0]
            operator = args[1]
            value = args[2]
            is_and = args[3]

            where = self.process_where([self.TYPE_SIMPLE, field, operator, value, is_and])
            if where:
                self._where.append(where)
        return self

    def where_nest(self, where, is_and=True):
        where = self.process_where([self.TYPE_NEST, where, is_and])
        if where:
            self._where.append(where)
        return self

    def where_in(self, field, value, is_and=True):
        where = self.process_where([self.TYPE_IN, field, value, is_and])
        if where:
            self._where.append(where)
        return self

    def where_not_in(self, field, value, is_and=True):
        where = self.process_where([self.TYPE_NOT_IN, field, value, is_and])
        if where:
            self._where.append(where)
        return self

    def where_between(self, field, value, is_and=True):
        where = self.process_where([self.TYPE_BETWEEN, field, value, is_and])
        if where:
            self._where.append(where)
        return self

    def where_null(self, field, is_and=True):
        where = self.process_where([self.TYPE_NULL, field, is_and])
        if where:
            self._where.append(where)
        return self

    def where_not_null(self, field, is_and=True):
        where = self.process_where([self.TYPE_NOT_NULL, field, is_and])
        if where:
            self._where.append(where)
        return self

    def select(self, args):
        if args and isinstance(args, list):
            self._field = args
        return self

    def group(self, args):
        if args and isinstance(args, list):
            self._group = args
        return self

    def limit(self, args):
        if args and isinstance(args, list):
            self._limit = args[0:2]
        return self

    def order(self, args):
        if args and isinstance(args, dict):
            self._order = args
        return self

    def join(self, join_table, where, join_type="INNER"):
        where_res = self.process_where(where)
        if not where_res:
            raise WhereArgStructErr(str_f('join 条件异常，{}', where))
        if not join_table:
            raise WhereArgStructErr(str_f('join table异常，table {}', join_table))

        if join_type not in self._get_join_type():
            raise WhereArgStructErr(str_f('join 类型异常，join_type {}', join_type))

        self._join = [join_table, where_res, join_type]
        return self

    def first(self):
        self.limit([1])
        return self.get()

    def get(self):
        self._sql_type = self.SQL_TYPE_QUERY
        return self.__run()

    def update(self, data):
        if not data or not isinstance(data, dict):
            raise WhereArgStructErr(str_f('更新数据为空或结构异常 {}', data))
        self.check_value(data.values())
        self.check_field(data.keys())

        self._sql_type = self.SQL_TYPE_UPDATE
        self._update_field = data
        return self.__run()

    def insert(self, data):
        if not isinstance(data, list):
            data = [data]
        fields = None
        for item in data:
            if not isinstance(item, dict):
                raise WhereArgStructErr(str_f('新增数据异常 {}', item))

            this_field = item.keys()
            if fields is None:
                fields = this_field
            elif list_diff(fields, this_field) or list_diff(this_field, fields):
                raise WhereArgStructErr('新增数据格式异常，列必须一致')
            else:
                fields = this_field
            self.check_value(item.values())

            self.check_field(item.keys())

        self._sql_type = self.SQL_TYPE_INSERT
        self._insert_data = data
        return self.__run()

    def delete(self):
        self._sql_type = self.SQL_TYPE_DELETE
        return self.__run()

    def exec_sql(self, sql, params):
        conn = self._model.get_connector().set_debug(self._is_debug)
        return conn.exec_sql(sql, params)

    def __run(self):
        conn = self._model.get_connector().set_debug(self._is_debug)
        sb = conn.get_sql_builder().set_query_builder(self)
        return conn.exec_sql(sb.to_sql(), sb.get_params())

    def g_sql_type(self):
        return self._sql_type

    def g_table(self):
        return self._table

    def g_field(self):
        return self._field

    def g_group(self):
        return self._group

    def g_where(self):
        return self._where

    def g_limit(self):
        return self._limit

    def g_order(self):
        return self._order

    def g_join(self):
        return self._join

    def g_update_field(self):
        return self._update_field

    def g_insert_data(self):
        return self._insert_data
