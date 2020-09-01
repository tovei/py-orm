# -*- coding:utf-8 -*-
from Orm.Interface.connector import Connector
from Orm.Mysql.mysql_builder import MysqlBuilder
from mysql import connector

mysql_config = {
    "host": "127.0.0.1",
    "port": "3306",
    "user": "root",
    "password": "123456",
    "database": "test",
    "charset": "utf8",
}


class MysqlConnector(Connector):
    def __init__(self):
        Connector.__init__(self)
        self._sql_builder = MysqlBuilder()
        self._conn = None

    def get_connect(self):
        if not self._conn:
            self._conn = connector.connect(**mysql_config)
        return self._conn

    def exec_sql(self, sql, params):
        if self._is_debug:
            return sql, params
        else:
            pass

    def execute(self):
        pass
