from sql_v1 import Engine
from user import Client


def run(user):
    a = Engine(user)
    return a.run()


if __name__ == '__main__':
    root = Client('root', '123456')
    run(root)
