# coding:utf-8
# python3

import json
from datetime import datetime, date


class ComplexJsonEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        # 可以继续添加逻辑处理其他特殊数据类型
        else:
            return json.JSONEncoder.default(self, obj)
