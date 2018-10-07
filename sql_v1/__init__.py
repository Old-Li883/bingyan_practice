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
from sql_v1.case import IsCase
from sql_v1.log import Log

# 为了不明文保存转码保存


def _decode_db(content):
    content = base64.decodebytes(content)
    return content.decode()[::-1]


def _encode_db(content):
    content = content[::-1].encode()
    return base64.encodebytes(content)


class Engine:
    def __init__(self, user, db_name=None, format_type='dict'):
        self._database_objs = {}  # 创建数据库与数据库引擎映射表
        self._database_names = []  # 数据库名字集合
        self._current_db = None  # 选择当前使用的数据库
        self._format_type = format_type  # 确定数据返回格式（数据默认返回格式为dict）
        self._user = user
        self.path = self._user.name + ".data"
        if self._user.name == 'root':
            self.root_flag = 1
        else:
            self.root_flag = 0
        if db_name is not None:
            self.select_db(db_name)
        self._load_databases()
        if self._user.name == 'root':
            self.select_db('user')
        self._action_map = {  # 方法映射
            'create': self._create,
            'insert': self._insert,
            'update': self._update,
            'search': self._search,
            'delete': self._delete,
            'drop': self._drop,
            'show': self._show,
            'use': self._use,
            'exit': self._exit,
            'change': self._change,
            'commit': self._commit,
            'begin': self._begin,
            'rollback': self._rollback,
            'select_d': self._search_d,
            'grant': self._grant
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
            raise Exception('Table  %s has not existed' (table, ))

        return table

    def _check_is_choose(self):
        """
        判断当前有没有选择数据库
        """
        if not self._current_db or not isinstance(self._current_db, Database):
            raise Exception('No any database is choosed')

    def _create(self, action):
        kind = action['kind']
        if kind == 'table':
            table = action['name']  # 获取表名
            data = action['data']
            field = {}
            for key in data:
                if isinstance(data[key], list):  # 获取要创建的字段及数据
                    if data[key][1] == 'NONE':  # 这个位置装的是默认值
                        data[key][1] = None
                    if data[key][2] == 'NONE':  # 这个位置装的是键值
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

    def _search_d(self, action):
        """
        二次查询
        """
        table = action['table']
        fields = action['fields']
        conditions = action['condition']
        result1 = []
        condition = {}
        if action['way'] != 'left join':
            for i in self.search(
                    table[1], fields=conditions[table[1]], conditions={}):
                case = IsCase(i[conditions[table[1]][0]])

                condition = {conditions[table[0]][0]: case}
                result = self.search(
                    table[0], fields=fields[table[0]], conditions=condition)
                if result:
                    result1.append(result[0])
                elif action['way'] == 'right join':
                    tmp = {}
                    for field in fields[table[0]]:
                        tmp.update({field: 'NONE'})
                    result1.append(tmp)
        else:
            result1 = self.search(
                table[0], fields=fields[table[0]], conditions={})
        result2 = []
        if action['way'] != 'right join':
            for i in self.search(
                    table[0], fields=conditions[table[0]], conditions={}):
                case = IsCase(i[conditions[table[0]][0]])
                if action['way'] != 'right join':
                    condition = {conditions[table[1]][0]: case}
                result = self.search(
                    table[1], fields=fields[table[1]], conditions=condition)
                if result:
                    result2.append(result[0])
                elif action['way'] == 'left join':
                    tmp = {}
                    for field in fields[table[1]]:
                        tmp.update({field: 'NONE'})
                    result2.append(tmp)
        else:
            result2 = self.search(
                table[1], fields=fields[table[1]], conditions={})
        result = []
        for i in range(len(result2)):
            result.append({**result1[i], **result2[i]})

        return result

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

    def _commit(self, _):
        return 'commit'

    def _begin(self, _):
        return 'begin'

    def _rollback(self, _):
        return 'rollback'

    def _change(self, action):
        user = action['user']
        if self._user.name == 'root':
            if self.root_flag == 1:
                self.table = self._current_db.get_table_obj('user')
                user_flag = 0
                self.users = self.table.search([
                    'u_name',
                ])
                self.root_flag = 0
            for i in range(self.table.len()):
                if user in self.users[i]['u_name']:
                    user_flag = 1
            if user_flag == 0:
                raise Exception("User %s has not been exist" % (user, ))
            self._database_names = []
            self._database_objs = {}
            self._current_db = None
            self.path = user + ".data"
            self._load_databases()
            if user == 'root':
                self.select_db('user')
        else:
            return {"type": "change", "user": user}

    def _grant(self, action):
        grant = action['grant']
        user = action['user']
        return self.grant(user, grant)

    def create_database(self, database_name):
        if database_name in self._database_objs:
            raise Exception('Database %s has exist' % (database_name, ))
        self._database_names.append(database_name)  # 加数据库名字
        self._database_objs[database_name] = Database(
            database_name)  # 数据库呢名与对象的映射

    def drop_database(self, database_name):
        if database_name not in self._database_objs:
            raise Exception('Database %s is not existed' % (database_name, ))
        self._database_names.remove(database_name)
        self._database_objs.pop(database_name, True)

    def drop_table(self, table_name):
        self._check_is_choose()
        self._current_db.drop_tables(table_name)

    def select_db(self, db_name):
        if db_name not in self._database_objs:
            raise Exception('Database %s has not  existed' % (db_name, ))
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
        self._database_names = []
        self._database_objs = {}
        self._load_databases()  # 回滚就重新加载，原来内存的东西就会初始化为原来的值

    def search(self, table_name, fields='*', sort='ASC', **conditions):
        return self._get_table(table_name).search(
            fields=fields,
            sort=sort,
            format_type=self._format_type,
            **conditions)

    def insert(self, table_name, **data):
        if self._user.name == 'root' and self.path == 'root.data':
            text_name = data['data']['u_name'] + '.data'
            os.system("touch " + text_name)
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

    def get_database_obj(self, name):
        return self._database_objs[name]

    def grant(self, user, grant):
        if self._user.name != 'root':
            raise Exception("Only root can operate grant")
        statement = "update user set grant = %s where u_name = %s" % (
            grant,
            user,
        )
        self.execute(statement)

    def logging(self, action, statement, user):
        log = Log()
        if action['type'] in ['create', 'insert', 'delete', 'drop']:
            log.write("%s: %s" % (
                user,
                statement,
            ))
        elif action['type'] == 'update':
            table = action['table']
            fields = action['fields']
            conditions = action['conditions']
            data = self.search(table, fields=fields, conditions=conditions)
            log.write("%s: statement original data is %s" % (
                user,
                data,
            ))

    def execute(self, statement):
        global autocommit
        action = SQLParser().parse(statement)  # 全部的处理条件
        ret = None
        if autocommit == 0:
            self.logging(action, statement, self._user.name)  # 开始事务记录日志
        if action['type'] not in self._user.grant:
            raise Exception("sorry this user can not operate " +
                            action['type'])
        if action['type'] in self._action_map:
            ret = self._action_map.get(action['type'])(action)  # 执行第一个单词对应的函数
            if autocommit == 1:
                if action['type'] in [
                        'insert', 'update', 'delete', 'create', 'drop'
                ]:
                    self.commit()  # 自动提交
            if ret == 'begin':
                autocommit = 0
            if ret == 'commit':
                self.commit()
                autocommit = 1
            if ret == 'rollback':
                self.rollback()
        return ret

    def run(self):
        global autocommit
        autocommit = 1
        while True:
            statement = input('sql>')
            try:
                ret = self.execute(statement)
                if ret in ['exit', 'quit']:
                    print('bye')
                    return
                if isinstance(ret, dict):
                    return ret['user']  # 切换用户
                if ret not in ['begin', 'commit', 'rollback'] and ret:
                    pt = prettytable.PrettyTable(ret[0].keys())
                    pt.align = 'l'
                    for line in ret:
                        pt.align = 'r'
                        pt.add_row(line.values())
                    print(pt)
            except Exception as exc:
                print(str(exc))
                if autocommit == 0:
                    log = Log()
                    log.write(
                        "%s : %s,   %s" % (
                            self._user.name,
                            statement,
                            exc,
                        ),
                        'error',
                    )
                    self.rollback()
