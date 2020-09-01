# -*- coding:utf-8 -*-
from Orm.Interface.sql_builder import SqlBuilder, SqlBuilderError
from Orm.query_builder import QueryBuilder
from Orm.tool import str_f


class MysqlBuilder(SqlBuilder):

    def to_query_sql(self):
        tpl = "SELECT {} FROM {} {} {} {} {} {}"

        return str_f(tpl, self.compose_field(), self._query_builder.g_table(), self.compose_join(),
                     self.compose_where(),
                     self.compose_order(), self.compose_group(), self.compose_limit())

    def to_update_sql(self):
        tpl = "UPDATE {} SET {} {}"
        return str_f(tpl, self._query_builder.g_table(), self.compose_update_data(), self.compose_where())

    def to_insert_sql(self):
        tpl = "INSERT INTO {} {} VALUES {}"
        fields, data_str = self.compose_insert_data()
        return str_f(tpl, self._query_builder.g_table(), fields, data_str)

    def to_delete_sql(self):
        tpl = "DELETE FROM {} {}"
        return str_f(tpl, self._query_builder.g_table(), self.compose_where())

    def compose_field(self):
        if self._query_builder.g_field():
            return ','.join(self._query_builder.g_field())
        else:
            return '*'

    def compose_where(self):
        where = self.parse_where(self._query_builder.g_where())
        return 'WHERE ' + where if where else ''

    def compose_join(self):
        join = self._query_builder.g_join()
        if join:
            return str_f("{} JOIN {} ON {}", join[2].upper(), join[0], self.parse_where(join[1]))
        else:
            return ''

    def compose_limit(self):
        limit = self._query_builder.g_limit()
        if limit:
            self._params = self._params + limit
            return str_f('LIMIT {}', ','.join(["%s" for x in limit]))
        else:
            return ''

    def compose_order(self):
        order = self._query_builder.g_order()
        if order:
            order_list = []
            for field, sort in order.items():
                order_list.append(str_f("{} {}", field, sort))
            return str_f('ORDER BY {}', ','.join(order_list))
        else:
            return ''

    def compose_group(self):
        group = self._query_builder.g_group()
        if group:
            return 'GROUP BY ' + ','.join(group)
        else:
            return ''

    def compose_update_data(self):
        update_field = self._query_builder.g_update_field()
        if update_field:
            result_list = []
            for field, value in update_field.items():
                self._params.append(value)
                result_list.append(str_f("{}=%s", field))
            return ','.join(result_list)
        else:
            return ''

    def compose_insert_data(self):
        g_insert_data = self._query_builder.g_insert_data()
        if g_insert_data:
            fields = []
            inserts = []
            for item in g_insert_data:
                data = []
                if not fields:
                    fields = item.keys()
                for field, value in item.items():
                    data.append('%s')
                    self._params.append(value)
                inserts.append("(" + ','.join(data) + ")")
            return "(" + ','.join(fields) + ")", ','.join(inserts)
        else:
            return None, None

    def parse_where(self, where):
        if not where:
            return ''
        if not isinstance(where, list):
            where = [where]

        where_str = ''
        for x in where:
            where_type = x.get('type')
            if where_type == QueryBuilder.TYPE_SIMPLE:
                self._params.append(x.get('value'))
                where_item_str = str_f("{} {} %s", x.get('field'), x.get('operator'))
            elif where_type == QueryBuilder.TYPE_NEST:
                nest_where = self.parse_where(x.get('value'))
                where_item_str = '(' + nest_where + ')'
            elif where_type == QueryBuilder.TYPE_BETWEEN:
                where_item_str = str_f("{} BETWEEN %s,%s", x.get('field'))
                self._params = self._params + x.get('value')
            elif where_type == QueryBuilder.TYPE_NOT_IN:
                where_item_str = str_f("{} NOT IN ({})", ','.join(['%s' for i in x.get('value')]))
                self._params = self._params + x.get('value')
            elif where_type == QueryBuilder.TYPE_IN:
                where_item_str = str_f("{} IN ({})", ','.join(['%s' for i in x.get('value')]))
                self._params = self._params + x.get('value')
            elif where_type == QueryBuilder.TYPE_NULL:
                where_item_str = str_f("{} IS NULL", x.get('field'))
            elif where_type == QueryBuilder.TYPE_NOT_NULL:
                where_item_str = str_f("{} IS NOT NULL", x.get('field'))
            else:
                raise SqlBuilderError(str_f('where type can\'t parse, type:{}', where_type))

            and_str = ' '
            if where_str:
                and_str = ' AND ' if x.get('is_and') else ' OR '
            where_str = where_str + and_str + where_item_str

        return where_str

    def to_sql(self):
        if hasattr(self, 'to_' + self._query_builder.g_sql_type() + '_sql'):
            return getattr(self, 'to_' + self._query_builder.g_sql_type() + '_sql')()
        else:
            raise SqlBuilderError('无效的查询类型:', self._query_builder.g_sql_type())
