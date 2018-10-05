"""
数据库引擎
"""

from sql_v1.core.database import Database
import json
import base64
import os
from sql_v1.parser import SQLParser
import prettytable  # 画表的库
from sql_v1.core.field import Field, FieldKey, FieldType


# 为了不明文保存转码保存
def _decode_db(content):
    content = base64.decodebytes(content)
    return content.decode()[::-1]


def _encode_db(content):
    content = content[::-1].encode()
    return base64.encodebytes(content)


class Engine:
    def __init__(self, db_name=None, format_type='dict', path='db.data'):
        self._database_objs = {}  # 创建数据库与数据库引擎映射表
        self._database_names = []  # 数据库名字集合
        self._current_db = None  # 选择当前使用的数据库
        self._format_type = format_type  # 确定数据返回格式（数据默认返回格式为dict）
        if db_name is not None:
            self.select_db(db_name)
        self.path = path
        self._load_databases()
        self._action_map = {  # 方法映射
            'create': self._create,
            'insert': self._insert,
            'update': self._update,
            'search': self._search,
            'delete': self._delete,
            'drop': self._drop,
            'show': self._show,
            'use': self._use,
            'exit': self._exit
        }

    def _dump_databases(self):  # 存储数据到本地
        with open(self.path, 'wb') as f:
            content = _encode_db(self.serialized())  # 将序列化好的东西进行编码
            f.write(content)  # 保存到本地

    def _load_databases(self):  # 从本地加载数据
        if not os.path.exists(self.path):  # 如果路径不存在，就返回
            return
        with open(self.path, 'rb') as f:
            content = f.read()

        if content:
            self.deserialized(_decode_db(content))  # 将解码的东西，送去反序列化

    def _get_table(self, table_name):
        """
        获取当前选择数据库表
        """
        self._check_is_choose()  # 判断当前是否选择有数据库
        table = self._current_db.get_table_obj(table_name)  # 获取当前table对象

        if table is None:
            raise Exception('this table has not existed')

        return table

    def _check_is_choose(self):
        """
        判断当前有没有选择数据库
        """
        if not self._current_db or not isinstance(self._current_db, Database):
            raise Exception('no any database choose')

    def _create(self, action):
        kind = action['kind']
        if kind == 'table':
            table = action['name']  # 获取表名
            data = action['data']
            field = {}
            for key in data:
                if isinstance(data[key], list):  # 获取要创建的字段及数据
                    if data[key][1].upper() == 'NONE':  # 这个位置装的是默认值
                        data[key][1] = None
                    if data[key][2] == []:  # 这个位置装的是键值
                        data[key][2] = FieldKey.NULL
                    field[key] = Field(
                        FieldType(data[key][0]), data[key][2], data[key][1])
                else:
                    field[key] = Field(FieldType(data[key]))
            return self.create_table(table, **field)
        elif kind == 'database':
            name = action['name']
            return self.create_database(name)

    def _insert(self, action):
        table = action['table']
        data = action['data']
        return self.insert(table, data=data)

    def _update(self, action):
        table = action['table']
        data = action['data']
        conditions = action['conditions']
        return self.update(table, data, conditions=conditions)

    def _delete(self, action):
        table = action['table']
        conditions = action['conditions']
        return self.delete(table, conditions=conditions)

    def _search(self, action):
        table = action['table']
        fields = action['fields']
        conditions = action['conditions']
        return self.search(table, fields=fields, conditions=conditions)

    def _drop(self, action):
        if action['kind'] == 'database':
            return self.drop_database(action['name'])
        return self.drop_table(action['name'])

    def _show(self, action):
        if action['kind'] == 'databases':
            return self.get_database(format_type='dict')
        return self.get_table(format_type='dict')

    def _use(self, action):
        return self.select_db(action['database'])

    def _exit(self, _):
        return 'exit'

    def create_database(self, database_name):
        if database_name in self._database_objs:
            raise Exception('database has exist')
        self._database_names.append(database_name)  # 加数据库名字
        self._database_objs[database_name] = Database(
            database_name)  # 数据库呢名与对象的映射

    def drop_database(self, database_name):
        if database_name not in self._database_objs:
            raise Exception('database is not existed')
        self._database_names.remove(database_name)
        self._database_objs.pop(database_name, True)

    def drop_table(self, table_name):
        self._check_is_choose()
        self._current_db.drop_tables(table_name)

    def select_db(self, db_name):
        if db_name not in self._database_objs:
            raise Exception('this database has been existed')
        self._current_db = self._database_objs[db_name]

    def serialized(self):
        return json.dumps([
            database.serialized() for database in self._database_objs.values()
        ])

    def deserialized(self, content):  # content为解码好的接送对象
        data = json.loads(content)
        for obj in data:
            database = Database.deserialized(obj)
            db_name = database.get_name()
            self._database_names.append(db_name)  # 把每个数据库绑定到这个引擎上
            self._database_objs[db_name] = database

    def commit(self):
        self._dump_databases()  # 提交就保存数据

    def rollback(self):
        self._load_databases()  # 回滚就重新加载，原来内存的东西就会初始化为原来的值

    def search(self, table_name, fields='*', sort='ASC', **conditions):
        return self._get_table(table_name).search(
            fields=fields,
            sort=sort,
            format_type=self._format_type,
            **conditions)

    def insert(self, table_name, **data):
        return self._get_table(table_name).insert(**data)  # 调用table类暴露的方法

    def update(self, table_name, data, **conditions):
        self._get_table(table_name).update(data, **conditions)

    def delete(self, table_name, **conditions):
        return self._get_table(table_name).delete(**conditions)

    def create_table(self, name, **options):
        self._check_is_choose()
        self._current_db.create_table(name, **options)

    def get_database(self, format_type='list'):
        """
        获取所有数据库
        """
        databases = self._database_names
        if format_type == 'dict':
            tmp = []
            for database in databases:
                tmp.append({'name': database})

            databases = tmp

        return databases

    def get_table(self, format_type='list'):
        """
        获取数据库下所有数据表
        """
        self._check_is_choose()
        tables = self._current_db.get_table()
        if format_type == 'dict':
            tmp = []
            for table in tables:
                tmp.append({'name': table})
            tables = tmp
        return tables

    def execute(self, statement):
        action = SQLParser().parse(statement)  # 全部的处理条件
        ret = None
        if action['type'] in self._action_map:
            ret = self._action_map.get(action['type'])(action)  # 执行第一个单词对应的函数
            if action['type'] in [
                    'insert', 'update', 'delete', 'create', 'drop'
            ]:
                self.commit()  # 自动提交
            return ret

    def run(self):
        while True:
            statement = input('sql>')
            try:
                ret = self.execute(statement)
                if ret in ['exit', 'quit']:
                    print('bye')
                    return
                if ret:
                    pt = prettytable.PrettyTable(ret[0].keys())
                    pt.align = 'l'
                    for line in ret:
                        pt.align = 'r'
                        pt.add_row(line.values())
                    print(pt)
            except Exception as exc:
                print(str(exc))
