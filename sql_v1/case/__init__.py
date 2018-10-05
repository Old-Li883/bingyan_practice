from sql_v1.core import TYPE_MAP

LIKE_SYMBOL = '%'


# 定义判断条件
def _is(data, condition):
    return data == condition


def _is_not(data, condition):
    return data != condition


def _in(data, condition):
    return data in condition


def _not_in(data, condition):
    return data not in condition


def _greater(data, condition):
    return data > condition


def _less(data, condition):
    return data < condition


def _greater_and_equal(data, condition):
    return data >= condition


def _less_and_equal(data, condition):
    return data <= condition


def _like(data, condition):
    tmp = condition.split(LIKE_SYMBOL)  # 以%为标志切割
    length = len(tmp)
    if length == 3:
        condition = tmp[1]
    elif length == 2:
        raise Exception('syntax error')
    elif length == 1:
        condition = tmp[0]
    return condition in data  # 看一下解析条件是否在数据里面


def _range(data, condition):
    return condition[0] <= data <= condition[1]


# 建立字典与函数映射
SYMBOL_MAP = {
    'IN': _in,
    'NOT_IN': _not_in,
    '>': _greater,
    '<': _less,
    '=': _is,
    '!=': _is_not,
    '>=': _greater_and_equal,
    '<=': _less_and_equal,
    'LIKE': _like,
    'RANGE': _range
}


# 条件基类
class BaseCase:
    def __init__(self, condition, symbol):
        self.condition = condition
        self.symbol = symbol  # 条件标志

    def __call__(self, data, data_type):  # 直接调用对象便可执行函数
        self.condition = TYPE_MAP[data_type.value](
            self.condition)  # 强制转换value成data_type类型

        if isinstance(self.condition, str):
            self.condition = self.condition.replace("'", '').replace('"', '')

        return SYMBOL_MAP[self.symbol](data,
                                       self.condition)  # 执行symbol_map中映射的函数方法


# 创建各个标志类
class IsCase(BaseCase):
    def __init__(self, condition):
        super().__init__(condition, symbol='=')


class IsNotCase(BaseCase):
    def __init__(self, condition):
        super().__init__(condition, symbol='!=')


class InCase(BaseCase):
    def __init__(self, condition):
        super().__init__(condition, symbol='IN')


class NotInCase(BaseCase):
    def __init__(self, condition):
        super().__init__(condition, symbol='NOT_IN')


class GreaterCase(BaseCase):
    def __init__(self, condition):
        super().__init__(condition, symbol='>')


class LessCase(BaseCase):
    def __init__(self, condition):
        super().__init__(condition, symbol='<')


class GAECase(BaseCase):
    def __init__(self, condition):
        super().__init__(condition, symbol='>=')


class LAECase(BaseCase):
    def __init__(self, condition):
        super().__init__(condition, symbol='<=')


class LikeCase(BaseCase):
    def __init__(self, condition):
        super().__init__(condition, symbol='LIKE')

    def __call__(self, data, data_type):
        self.condition = TYPE_MAP[data_type.value](self.condition)
        return SYMBOL_MAP[self.symbol](str(data), self.condition)  # 调用函数解析条件


class RangeCase(BaseCase):
    """
    判断在不在range范围内
    """

    def __init__(self, start, end):
        super().__init__((int(start), int(end)), symbol='RANGE')

    def __call__(self, data, data_type):
        if not isinstance(self.condition, tuple):
            raise TypeError('type error')

        return SYMBOL_MAP[self.symbol](data, self.condition)
