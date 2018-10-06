import re
from sql_v1.case import *
from sql_v1.core import FieldKey


class SQLParser:
    def __init__(self):  # 定义语句类型
        self._pattern_map = {
            'SELECT':
            r'(SELECT|select) (.*) (FROM|from) (.*)',
            'UPDATE':
            r'(UPDATE|update) (.*) (SET|set) (.*)',
            'INSERT':
            r"(INSERT|insert) (INTO|into) (.*) (.*) (VALUES|values) (.*)",
            'CREATE_T':
            r"(CREATE|create) (TABLE|table) (.*) (.*)",
            'CREATE_D':
            r"(CREATE|create) (DATABASE|database) (.*)"
        }
        self._action_map = {
            'CREATE': self._create,
            'SELECT': self._select,
            'UPDATE': self._update,
            'DELETE': self._delete,
            'INSERT': self._insert,
            'USE': self._use,
            'EXIT': self._exit,
            'QUIT': self._exit,
            'SHOW': self._show,
            'DROP': self._drop,
            'CHANGE': self._change,
            'BEGIN': self._begin,
            'COMMIT': self._commit,
            'ROLLBACK': self._rollback
        }
        self.SYMBOL_MAP = {
            'IN': InCase,
            'NOT_IN': NotInCase,
            '>': GreaterCase,
            '<': LessCase,
            '=': IsCase,
            '!=': IsNotCase,
            '>=': GAECase,
            '<=': LAECase,
            'LIKE': LikeCase,
            'RANGE': RangeCase
        }

    def _filter_space(self, obj):
        ret = []
        for x in obj:
            if x.strip() == '' or x.strip() == 'AND':  # 切分字符串
                continue
            ret.append(x)
        return ret

    def _get_comp(self, action):
        return re.compile(self._pattern_map[action])  # 将正则字符窜编译成正则对象

    """
    从这里开始按照不同的方式处理前半部分
    type：操作
    data：插入或更新的数据
    fields：查询时的字段
    table：表名
    """

    def _select(self, statement):
        comp = self._get_comp('SELECT')
        ret = comp.findall(" ".join(statement))
        if ret and len(ret[0]) == 4:
            fields = ret[0][1].strip()
            table = ret[0][3].strip()
            if fields != '*':
                fields = [field.strip() for field in fields.split(",")]
            return {'type': 'search', 'fields': fields, 'table': table}
        return None

    def _update(self, statement):
        statement = ' '.join(statement)
        comp = self._get_comp('UPDATE')
        ret = comp.findall(statement)  # 寻找所有能够匹配的字符串,双层列表形式
        if ret and len(ret[0]) == 4:
            data = {'type': 'update', 'table': ret[0][1].strip(), 'data': {}}
            set_statement = ret[0][3].split(",")
            for s in set_statement:
                s = s.split("=")
                field = s[0].strip()  # 寻找要更新的字段
                value = s[1].strip()  # 寻找要更新的值
                if "'" in value or '"' in value:
                    value = value.replace('"', '').replace(
                        "'", '').strip()  # 单双引号替换成空格
                else:
                    try:
                        value = int(value.strip())
                    except Exception as e:
                        return None
                data['data'][field] = value  # 加入要修改的数据
            return data
        return None

    def _delete(self, statement):
        return {'type': 'delete', 'table': statement[2]}

    def _insert(self, statement):
        comp = self._get_comp('INSERT')
        ret = comp.findall(" ".join(statement))

        if ret and len(ret[0]) == 6:  # insert语句是六个句子
            ret_tmp = ret[0]
            data = {
                'type': 'insert',
                'table': ret_tmp[2],
                'data': {}
            }  # 按照insert句子的结构特点切分句子
            fields = ret_tmp[3].split(",")
            values = ret_tmp[5].split(",")

            for i in range(0, len(fields)):
                field = fields[i].replace('(', ' ').replace(')', ' ').strip()
                value = values[i].replace('(', ' ').replace(')', ' ').strip()
                if "'" in value or '"' in value:
                    value = value.replace('"', ' ').replace(
                        "'", ' ').strip()  # 切除括号
                else:
                    try:
                        value = int(value.strip())
                    except Exception as e:
                        return None
                data['data'][field] = value
            return data
        return None

    def _create(self, statement):
        if statement[1] == 'table':
            comp = self._get_comp('CREATE_T')
            ret = comp.findall(" ".join(statement))
            if ret and len(ret[0]) == 4:
                ret_tmp = ret[0]
                data = {
                    'type': 'create',
                    'kind': 'table',
                    'name': ret_tmp[2].strip(),
                    'data': {}
                }
                data_tmp = ret_tmp[3].split(",")
                for i in range(0, len(data_tmp)):
                    data_tmp[i] = data_tmp[i].replace('(', ' ').replace(
                        ')', ' ').strip()
                    if ";" in data_tmp[i]:
                        tmp = []
                        tmp_key = []
                        for ele in data_tmp[i].split(";"):
                            try:
                                ele = FieldKey(ele)
                                tmp_key.append(ele)
                            except Exception as e:
                                tmp.append(ele.strip())  # 将主键信息，默认值解析
                        tmp.append(tmp_key)
                        data_tmp[i] = tmp
                for i in range(0, len(data_tmp), 2):
                    data['data'][data_tmp[i]] = data_tmp[i + 1]
                return data
        elif statement[1] == 'database':
            comp = self._get_comp('CREATE_D')
            ret = comp.findall(" ".join(statement))
            if ret and len(ret[0]) == 3:
                ret_tmp = ret[0]
                data = {
                    'type': 'create',
                    'kind': 'database',
                    'name': ret_tmp[2].strip()
                }
                return data

    def _use(self, statement):
        """
        选择数据库
        """
        return {'type': 'use', 'database': statement[1]}

    def _exit(self, _):
        return {'type': 'exit'}

    def _commit(self, _):
        return {'type': 'commit'}

    def _rollback(self, _):
        return {'type': 'rollback'}

    def _begin(self, _):
        return {'type': 'begin'}

    def _show(self, statement):
        """
        查看数据
        """
        kind = statement[1]
        if kind.upper() == 'DATABASES':
            return {'type': 'show', 'kind': 'databases'}
        if kind.upper() == 'TABLES':
            return {'type': 'show', 'kind': 'tables'}

    def _drop(self, statement):
        """
        删除数据表
        """
        kind = statement[1]  # 看要删除什么
        if kind.upper() == 'DATABASE':
            return {'type': 'drop', 'kind': 'database', 'name': statement[2]}
        if kind.upper() == 'TABLE':
            return {'type': 'drop', 'kind': 'table', 'name': statement[2]}

    def _change(self, statement):
        return {'type': 'change', 'user': statement[1]}

    def parse(self, statement):
        # tmp_s = statement
        tmp_group = None
        if 'group' in statement:
            statement = statement.split("group")
            tmp_group = statement[1].strip()
            tmp_group_condition = None
            if 'having' in tmp_group:
                tmp_group = tmp_group.split("having")
                tmp_group_condition = tmp_group[1].strip()
                tmp_group = tmp_group[0].strip()
            statement = statement[0]
        tmp_limit = None
        if 'limit' in statement:
            statement = statement.split("limit")
            tmp_limit = int(statement[1].strip())
            statement = statement[0]
        tmp_order = None
        if 'order by' in statement:
            statement = statement.split("order by")
            tmp_order = statement[1].strip()
            statement = statement[0]
        if 'where' in statement:
            statement = statement.split("where")
        else:
            statement = statement.split("WHERE")

        bases_statement = self._filter_space(
            statement[0].split(" "))  # 把多余的空格去掉
        if len(bases_statement) < 2 and bases_statement[0] not in [
                'exit', 'quit', 'begin', 'commit', 'rollback'
        ]:
            raise Exception('syntax error')  # 语句太少（小于1句）报错
        action_type = bases_statement[0].upper()  # 转换为大写字母
        if action_type not in self._action_map:
            raise Exception('syntax error')  # 传入的操作符不在可选的操作符上
        action = self._action_map[action_type](
            bases_statement)  # 根据字典执行函数得到对应的值
        action['conditions'] = {}  # conditions放查找条件
        conditions = None
        if len(statement) == 2:
            statement[1] = statement[1].strip()
            conditions = self._filter_space(
                statement[1].split(" "))  # 处理where后面的条件
        if conditions:
            for index in range(0, len(conditions), 3):
                field = conditions[index]
                symbol = conditions[index + 1].upper()
                condition = conditions[index + 2]
                if symbol == 'RANGE':  # 解析range条件
                    condition_tmp = condition.replace("(", '').replace(
                        ")", '').split(",")
                    start = condition_tmp[0]
                    end = condition_tmp[1]
                    case = self.SYMBOL_MAP[symbol](start, end)
                elif symbol == 'IN' or symbol == 'NOT_IN':  # 解析in，not_in条件
                    condition_tmp = condition.replace("(", '').replace(
                        ")", '').replace(" ", '').split(",")
                    condition = condition_tmp
                    case = self.SYMBOL_MAP[symbol](condition)
                else:
                    case = self.SYMBOL_MAP[symbol](condition)  # 返回条件对象
                action['conditions'][field] = case  # 加入case对象
        if tmp_limit is not None:
            action['conditions']['limit'] = tmp_limit
        if tmp_order is not None:
            action['conditions']['order'] = tmp_order
        if tmp_group is not None:
            action['conditions']['group'] = tmp_group
            if tmp_group_condition is not None:
                conditions = []
                tmp_group_condition = tmp_group_condition.split(" ")
                for condition in tmp_group_condition:  # 把二次查询条件加入
                    conditions.append(condition)  # 把条件变成列表
                field = conditions[0]
                symbol = conditions[1].upper()
                condition = conditions[2]
                if symbol == 'RANGE':  # 解析range条件
                    condition_tmp = condition.replace("(", '').replace(
                        ")", '').split(",")
                    start = condition_tmp[0]
                    end = condition_tmp[1]
                    case = self.SYMBOL_MAP[symbol](start, end)
                elif symbol == 'IN' or symbol == 'NOT_IN':  # 解析in，not_in条件
                    condition_tmp = condition.replace("(", '').replace(
                        ")", '').replace(" ", '').split(",")
                    condition = condition_tmp
                    case = self.SYMBOL_MAP[symbol](condition)
                else:
                    case = self.SYMBOL_MAP[symbol](condition)  # 返回条件对象
                action['conditions']['group_condition'] = case
        return action
