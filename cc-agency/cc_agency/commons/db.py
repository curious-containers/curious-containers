import pymongo


class Mongo:
    def __init__(self, conf):
        host = conf.d['mongo'].get('host', 'localhost')
        port = conf.d['mongo'].get('port', 27017)
        db = conf.d['mongo']['db']
        username = conf.d['mongo']['username']
        password = conf.d['mongo']['password']

        self.client = pymongo.MongoClient('mongodb://{username}:{password}@{host}:{port}/{db}'.format(
            username=username,
            password=password,
            host=host,
            port=port,
            db=db
        ))

        self.db = self.client[db]
