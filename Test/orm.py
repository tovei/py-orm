# encoding:utf-8
import sys
import json

sys.path.append("..")
from Orm.model import Model
from Orm.query_builder import QueryBuilder


class TestModel(Model):
    table = 'test'

    def __init__(self):
        Model.__init__(self)


def test_query():
    return TestModel().where("name", '=', 3).where('type', '<>', 'teacher').where([
        [QueryBuilder.TYPE_NULL, 'id', False],
        [QueryBuilder.TYPE_BETWEEN, 'age', [18, 20], False],
        [QueryBuilder.TYPE_NEST, [
            [QueryBuilder.TYPE_NULL, 'id', False],
            [QueryBuilder.TYPE_BETWEEN, 'age', [18, 20], False],
        ], False]
    ]).join("user", [QueryBuilder.TYPE_SIMPLE, "test.user_id", '=', 'user.id', True]) \
        .select(['id', 'name', 'key']) \
        .order({"ctime": 'desc'}) \
        .set_debug(True) \
        .first()


if __name__ == '__main__':
    print test_query()

    print TestModel().where("name", '=', 3).where('type', '<>', 'teacher').where([
        [QueryBuilder.TYPE_NULL, 'id', False],
        [QueryBuilder.TYPE_BETWEEN, 'age', [18, 20], False],
        [QueryBuilder.TYPE_NEST, [
            [QueryBuilder.TYPE_NULL, 'id', False],
            [QueryBuilder.TYPE_BETWEEN, 'age', [18, 20], False],
        ], False]
    ]).join("user", [QueryBuilder.TYPE_SIMPLE, "test.user_id", '=', 'user.id', True]) \
        .select(['id', 'name', 'key']) \
        .order({"ctime": 'desc'}) \
        .set_debug(True) \
        .update({"xxxx": "bbb", "bbbb": 2})

    print TestModel().where("name", '=', 3).where('type', '<>', 'teacher').where([
        [QueryBuilder.TYPE_NULL, 'id', False],
        [QueryBuilder.TYPE_BETWEEN, 'age', [18, 20], False],
        [QueryBuilder.TYPE_NEST, [
            [QueryBuilder.TYPE_NULL, 'id', False],
            [QueryBuilder.TYPE_BETWEEN, 'age', [18, 20], False],
        ], False]
    ]).join("user", [QueryBuilder.TYPE_SIMPLE, "test.user_id", '=', 'user.id', True]) \
        .select(['id', 'name', 'key']) \
        .order({"ctime": 'desc'}) \
        .set_debug(True) \
        .delete()

    print TestModel().where("name", '=', 3).where('type', '<>', 'teacher').where([
        [QueryBuilder.TYPE_NULL, 'id', False],
        [QueryBuilder.TYPE_BETWEEN, 'age', [18, 20], False],
        [QueryBuilder.TYPE_NEST, [
            [QueryBuilder.TYPE_NULL, 'id', False],
            [QueryBuilder.TYPE_BETWEEN, 'age', [18, 20], False],
        ], False]
    ]).join("user", [QueryBuilder.TYPE_SIMPLE, "test.user_id", '=', 'user.id', True]) \
        .select(['id', 'name', 'key']) \
        .order({"ctime": 'desc'}) \
        .set_debug(True) \
        .insert({"xxxx": "bbb", "bbbb": 2})
