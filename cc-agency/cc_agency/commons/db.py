import gridfs
from gridfs.grid_file import GridOut
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

    def write_file(self, filename, content):
        """
        Writes a file into the GridFS of the mongo db. This file has the given filename and contains the given content.

        :param filename: The filename of the file to create
        :type filename: str
        :param content: The binary data to write into the file
        :type content: bytes
        """
        gfs = gridfs.GridFS(self.db)
        with gfs.new_file(filename=filename) as f:
            f.write(content)

    def write_file_from_file(self, filename, source_file):
        """
        Writes a file into the GridFS of the mongo db. This file has the given filename and contains the content
        retrieved from the source file.

        :param filename: The filename of the file to create
        :type filename: str
        :param source_file: An iterable yielding the data to the content
        """
        gfs = gridfs.GridFS(self.db)
        with gfs.new_file(filename=filename) as f:
            for line in source_file.readlines():
                f.write(line)

    def read_file(self, filename):
        """
        Returns the content of the given filename as bytes object.

        :param filename: The filename to retrieve
        :type filename: str
        :return: A bytes object containing the content of the requested file
        :rtype: bytes
        """
        gfs = gridfs.GridFS(self.db)
        with gfs.find_one({'filename': filename}) as f:
            return f.read()

    def get_file(self, filename):
        """
        Returns an iterable over the content of the given filename.

        :param filename: The filename to retrieve
        :type filename: str
        :return: An iterable iterating over the content of the requested file. If the requested file could not be found
                 None is returned
        :rtype: GridOut or None
        """
        gfs = gridfs.GridFS(self.db)
        return gfs.find_one({'filename': filename})
