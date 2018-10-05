from enum import Enum


class FieldType(Enum):
    """
    枚举类型，方便取值，找变量
    定义数据库数据类型
    """
    INT = int = 'int'
    VARCHAR = varchar = 'str'
    Float = float = 'float'


"""
定义字典映射
"""
TYPE_MAP = {
    'int': int,
    'float': float,
    'str': str,
    'INT': int,
    'FLOAT': float,
    'VARCHAR': str
}
"""
定义‘键’类型
"""


class FieldKey(Enum):
    PRIMARY = 'PRIMARY_KEY'  # 主键
    INCREMENT = 'AUTO_INCREMENT'  # 自增
    UNIQUE = 'UNIQUE'  # 唯一约束
    NOT_NULL = 'NOT_NULL'  # 非空
    NULL = 'NULL'  # 可空
