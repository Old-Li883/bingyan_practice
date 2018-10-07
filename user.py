class Client:
    def __init__(self,
                 name,
                 pwd=None,
                 grant=[
                     'create', 'search', 'show', 'delete', 'drop', 'begin',
                     'insert', 'update', 'use', 'exit', 'change', 'commit',
                     'rollback', 'select_d', 'grant'
                 ]):
        self.name = name
        self.pwd = pwd
        self.grant = grant

    def __call__(self):
        return self.name