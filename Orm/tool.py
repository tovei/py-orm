# -*- coding:utf-8 -*-
from enum import Enum
import json


class BaseEnum(Enum):

    @classmethod
    def members(cls):
        member_list = []
        for name, value in vars(cls).items():
            if name not in ['__module__', '__doc__']:
                member_list.append(value)
        return member_list


def str_f(string, *params):
    tmp_params = []
    for x in params:
        if isinstance(x, (int, float, str,)):
            tmp_params.append(str(x))
        else:
            tmp_params.append(json.dumps(x))
    if isinstance(string, str):
        return string.format(*tmp_params)
    return ''


def list_diff(list1, list2):
    return list(set(list1).difference(set(list2)))
