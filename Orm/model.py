# -*- coding:utf-8 -*-
from Orm.Mysql.mysql_connector import MysqlConnector
from Orm.query_builder import QueryBuilder

model_config = {
    "connector_class": MysqlConnector,
    "prefix": "tb_"
}


class Model(object):
    table = ''

    def __init__(self):
        self._connector = None
        self._query_builder = None
        self.boot()

    def boot(self):
        self._connector = model_config.get('connector_class')()
        self._query_builder = QueryBuilder().set_model(self).table(model_config.get('prefix', '') + self.table)

    def get_connector(self):
        return self._connector

    def get_query_builder(self):
        return self._query_builder

    def __getattr__(self, item):
        if hasattr(self._query_builder, item):
            return getattr(self._query_builder, item)
