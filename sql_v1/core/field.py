from sql_v1.core import FieldKey, FieldTpye, TYPE_MAP
import json


class Field():
    """
    一列的字段类型及值
    """

    def __init__(self, data_type, keys=FieldKey.NULL, default=None):
        self._type = data_type  # 字段类型
        self._keys = keys  # 该字段的键值，默认为空
        self._default  # 该字段的默认值
        self._value = []  # 该字段的所有值的集合
        self._rows = 0  # 该字段的列值

        # 判断5个属性合法性
        if not isinstance(self._keys, list):  # 将单键转化成序列对象，与多键的情形统一
            self._keys = [self._keys]

        if not isinstance(self._type, FieldTpye):  # 判断传来的字段类型是否在规定的字段类型内
            raise TypeError("data-type is not in fieldtype")

        for key in self._keys:
            if not isinstance(key, FieldKey):  # 判断键类型是否在规定的键内
                raise TypeError('data-type is not in fieldkey')

        if FieldKey.INCREMENT in self._keys:  # 自增类型一定要为整型
            if self._type != FieldTpye.INT:
                raise TypeError('Increment key must be int')

            if FieldTpye.PRIMARY not in self._keys:  # 自增类型为主键
                raise Exception('Increment key must be needed')

        if self._default is not None and FieldKey.UNIQUE in self._keys:  # 唯一约束值不能为默认
            raise Exception('Unique key must be not default value')

    def _check_type(self, value):
        """
        判断类传入数据类型是否符合之前定义的条件
        """
        if value is not None and not isinstance(value,
                                                TYPE_MAP[self._type.value]):
            raise TypeError('data type error')

    def _check_index(self, index):
        """
        查看该元素是在里面
        """
        if not isinstance(index, int) or not index < self._rows:
            raise Exception('not this element')

        return True

    def _check_keys(self, value):
        """
        判断键类型与所传入的值是否冲突
        """
        if FieldKey.INCREMENT in self._keys:
            if value is None:
                value = self._rows + 1  # 自增类型加一

            if value in self._value:  # 自增类型的值存在则抛出异常
                raise Exception('value is in')

        if FieldKey.PRIMARY in self._keys or FieldKey.UNIQUE in self._keys:
            if value in self._value:  # 主键和唯一键，判断插入的值是否在已存在的值中
                raise Exception('value is in')

        if (FieldKey.PRIMARY in self._keys
                or FieldKey.NOT_NULL in self._keys) and value is None:
            raise Exception('value must be not null')

            return value

    def get_data(self, index=None):
        if index is not None and self._check_index(
                index):  # 如果index值存在，且index不为空，则返回index值
            return self._values[index]
        return self._values  # 如果index值为NULL则返回所有值

    def add(self, value):
        if value is None:  # 如果传入值为None，则将值赋值为默认值
            value = self._default

        value = self._check_keys(value)  # 查看value值与该列键的关系

        self._check_type(value)  # 查看value类型是否本列指定类型

        self._value.append(value)

        self._rows += 1  # 该列数加一

    def delete(self, index):
        self._check_index(index)  # 判断index是否合法

        self._value.pop(index)

        self._rows -= 1  # 该列数减一

    def modify(self, index, value):

        self._check_index(index)

        value = self._check_keys(value)

        self._check_type(value)

        self._value[index] = value

    def get_keys(self):
        return self._keys

    def get_type(self):
        return self._type

    def length(self):
        return self._rows

    def serialized(self):
        """
        序列化该列对象
        """
        return json.dumps({
            'key': [key.value for key in self._keys],
            'type': self._type.value,
            'values': self._value,
            'default': self._default
        })

    def deserialized(data):
        """
        反序列化成Field对象
        """
        json_data = json.loads(data)

        keys = [FieldKey(key) for key in json_data['key']]  # 转化成FieldKey中的属性
        # 将json对象转化为一个Field对象，初始化一列
        obj = Field(
            FieldTpye(json_data['type']), keys, default=json_data['default'])

        for value in json_data['values']:
            obj.add(value)

        return obj