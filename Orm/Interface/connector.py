# -*- coding:utf-8 -*-
import abc


# 数据库接口类
class Connector(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self._sql_builder = None
        self._is_debug = False

    def set_debug(self, value):
        self._is_debug = value
        return self

    def set_sql_builder(self, sql_builder):
        self._sql_builder = sql_builder
        return self

    def get_sql_builder(self):
        return self._sql_builder

    @abc.abstractmethod
    def get_connect(self):
        pass

    @abc.abstractmethod
    def exec_sql(self, sql, params):
        pass

    @abc.abstractmethod
    def execute(self):
        pass
