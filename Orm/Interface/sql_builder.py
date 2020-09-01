# -*- coding:utf-8 -*-
import abc


class SqlBuilderError(Exception):
    pass


# sql构造器接口类
class SqlBuilder(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self._query_builder = None
        self._params = []

    def set_query_builder(self, query_builder):
        self._query_builder = query_builder
        return self

    @abc.abstractmethod
    def to_sql(self):
        pass

    def get_params(self):
        return self._params
