from sql_v1.core.table import Table
import json


class Database():
    def __init__(self, name):
        self._name = name  # 数据库名字
        self._table_names = []  # 所有数据表名
        self._table_objs = {}  # 数据库名与表对象映射

    def create_table(self, table_name, **options):
        """
        创建数据表
        """
        if table_name in self._table_objs:  # 判断该数据库是否已经存在
            raise Exception('This table has existed')

        self._table_names.append(table_name)

        self._table_objs[table_name] = Table(**options)

    def drop_tables(self, table_name):
        """
        删除对应的表
        """
        if table_name not in self._table_names:
            raise Exception('Table is not existed')

        self._table_names.remove(table_name)

        self._table_objs.pop(table_name, True)

    def get_table(self, index=None):
        length = len(self._table_names)
        if isinstance(
                index, int
        ) and -index < length > index:  # 连环比较，保证index在范围内（index有可能为负）
            return self._table_names[index]
        return self._table_names

    def get_table_obj(self, name):
        """
        获取数据表对象
        """
        return self._table_objs.get(name, None)  # 如果没有就返回空对象

    def get_name(self):
        """
        获取数据库名字
        """
        return self._name

    def add_table(self, table_name, table):
        if table_name not in self._table_objs:
            self._table_names.append(table_name)
            self._table_objs[table_name] = table

    def serialized(self):
        """
        序列化数据
        """
        data = {'name': self._name, 'tables': []}

        for tb_name, tb_data in self._table_objs.items():
            data['tables'].append([tb_name, tb_data.serialized()])  # 把每个表都序列化
        return json.dumps(data)

    def deserialized(obj):
        data = json.loads(obj)
        obj_tmp = Database(data['name'])
        for table_name, table_obj in data['tables']:
            obj_tmp.add_table(table_name,
                              Table.deserialized(table_obj))  # 把每一个table都反序列化
        return obj_tmp  # 返回Database对象
