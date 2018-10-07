import run
from user import Client
from sql_v1 import Engine

root = Client('root', '123456')
a = Engine(root)
table = a.get_database_obj('user').get_table_obj('user')
user_info = table.search('*')
data = {}
for user in user_info:
    data[user['u_name']] = user['pwd']
    data[user['u_name'] + 'grant'] = user['grant']
user = None
while True:
    if user is None:
        user = input("please choose user:")
    if user not in data:
        print("no this user")
        user = None
    else:
        flag = 0
        grant = data[user + 'grant']
        if grant == 'all':
            user = Client(user, data[user])
        else:
            user = Client(user, data[user], grant)
        while True:
            pwd = input("password:(Press p re-choose user)")
            if pwd != user.pwd:
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
