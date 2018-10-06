import run
from sql_v1 import Engine

a = Engine('root')
table = a.get_database_obj('user').get_table_obj('user')
user_info = table.search('*')
data = {}
for user in user_info:
    data[user['u_name']] = user['pwd']
user = None
while True:
    if user is None:
        user = input("please choose user:")
    if user not in data:
        print("no this user")
    else:
        flag = 0
        while True:
            pwd = input("password:(p re-choose user)")
            if pwd != data[user]:
                print("password wrong")
            else:
                user = run.run(user)
                break
            if pwd == 'p':
                flag = 1
                user = None
                break
        if flag == 0 and user is None:
            break
