from sql_v1.core.field import Field
import json


class Table():
    def __init__(self, **options):
        self._field_names = []  # 数据表所有字段名
        self._field_objs = {}  # 实现所有字段名与字段对象相映射，前面的字段对象没有名字
        self._rows = 0  # 表的行数

        # 将列名及类的对象加入到表中
        for field_name, field_obj in options.items():
            self.add_field(field_name, field_obj)

    def _get_field(self, field_name):
        """
        用这个字段名，找到其所映射的字段对象
        """
        if field_name not in self._field_names:
            raise Exception('no this field')
        return self._field_objs[field_name]

    def _get_field_data(self, field_name, index=None):
        """
        返回对应的字段对象
        """
        field = self._get_field(field_name)
        return field.get_data()  # 调用field对象的方法返回此对象的数据

    def _parse_conditions(self, **conditions):
        match_index = range(0, self._rows)
        return match_index

    def _get_field_length(self, field_name):
        field = self._get_field(field_name)
        return field.length()

    def _get_name_tmp(self, **options):
        # 解析参数中包含的字段名
        name_tmp = []
        params = options

        for field_name in params.keys():
            if field_name not in self._field_names:  # 看要找的字段在不在已有字段内
                raise Exception('no this field')
            name_tmp.append(field_name)
        return name_tmp

    def add_field(self, field_name, field_obj, value=None):

        if field_name in self._field_names:  # 判断字段名是否已经存在
            raise Exception('Field Exists')

        if not isinstance(field_obj, Field):
            raise TypeError('no this type of field')

        self._field_names.append(field_name)

        self._field_objs[field_name] = field_obj

        # 新加入字段长度应与先前加入的字段长度，表长一致
        if len(self._field_names) > 1:

            length = self._rows

            field_obj_length = field_obj.length()

            if field_obj_length != 0:
                if field_obj_length == length:
                    return
                raise Exception('field data length is not equal')

            for index in range(0, length):
                if value:
                    self._get_field(field_name).add(
                        value)  # 如果有默认值就将默认值全部加在这个字段上
                else:
                    self._get_field(field_name).add(
                        None)  # 调用Field对象的add方法，在相应字段中加入默认值

        else:
            self._rows = field_obj.length()

    def search(self, fields, sort, format_type, **conditions):
        """
        参数：
        要查询的字段对象
        排序方式
        返回数据形式
        查询条件
        """
        if fields == '*':  # 查询所有对象
            fields = self._field_names
        else:
            # 返回所有要查询的对象
            for field in fields:
                if field not in self._field_names:
                    raise Exception("no these fields")

        rows = []

        # 在表的查询中，解析查询条件,返回一个列表包含要的删除的行的索引
        match_index = self._parse_conditions(**conditions)

        for index in match_index:
            if format_type == 'list':
                row = [
                    self._get_field_data(field_name, index)
                    for field_name in fields
                ]
            elif format_type == 'dict':  # 字段名与字段对象的映射
                row = {}
                for field_name in fields:
                    row[field_name] = self._get_field_data(field_name, index)

            else:
                raise Exception('no this format')
            rows.append(row)

        if sort == 'DESC':  # 倒序排列索要查找的对象
            rows = rows[::-1]

        return rows

    def delete(self, **conditions):
        """
        删除字段
        """
        match_index = self._parse_conditions(**conditions)  # 解析要删除行的索引
        for field_name in self._field_name:
            count = 0  # 当前删除次数
            match_index.sort()  # 排序匹配的索引
            tmp_index = match_index[0]
            # 删除了第一个索引后，字段的其他索引都会一次减一，为了表中的index与字段中index相同需要做减法
            for index in match_index:
                if index > tmp_index:
                    index = index - count
                self._get_field(field_name).delete(index)

                count += 1
        self._rows = self._get_field_length(
            self._field_name[0])  # 表中的长度，与字段的长度要统一

    def update(self, data, **conditions):
        """
        更新数据操作
        """
        match_index = self._parse_conditions(**conditions)  # 找到需要操作的索引
        name_tmp = self._get_name_tmp(**data)  # 找到需要修改的字段
        for field_name in name_tmp:
            for index in match_index:
                self._get_field(field_name).modify(index, data[field_name])

    def insert(self, **data):
        """
        插入操作
        """
        if 'data' in data:
            data = data['data']  # 去除data
        name_tmp = self._get_name_tmp(**data)  # 取出要操作的字段
        for field_name in self._field_names:  # 遍历一个表中的所有字段
            value = None  # 先预设value值为None
            if field_name in name_tmp:  # 如果这个字段是要修改的字段
                value = data[field_name]  # 就将value赋值给value
            try:
                self._get_field(field_name).add(value)
            except Exception as e:
                # 如果不存在这个字段，抛出异常
                raise Exception(field_name, str(e))
        self._rows += 1

    def serialized(self):
        """
        序列化表对象
        """
        data = {}
        for field in self._field_names:
            data[field] = self._field_objs[
                field].serialized()  # 把每个字段分别序列化加入字典对象
        return json.dumps(data)

    def deserialized(data):
        """
        反序列化对象
        """
        json_data = json.loads(data)
        table_obj = Table()  # 实例化一个table对象
        field_names = [field_name
                       for field_name in json_data.keys()]  # 获取所有的字段名
        for field_name in field_names:
            field_obj = Field.deserialized(
                json_data[field_name])  # 取出字段所对应的数据，将所有字段反序列化
            table_obj.add_field(field_name, field_obj)  # 将字段加入表中

        return table_obj