from sql_v1 import Engine


def run(user):
    a = Engine(user)
    return a.run()


if __name__ == '__main__':
    run('root')
