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
    
    def add_user(self, user: dict):
        """
        Adds or updates a user in the 'users' collection.

        :param user: User object to be added or updated.
        :type user: dict
        :return: The result of the update operation.
        :rtype: pymongo.results.UpdateResult
        """
        return self.db['users'].update_one({'username': user.username}, {'$set': user}, upsert=True)
    
    def find_user_by_name(self, username: str):
        """
        Finds a user in the 'users' collection by username.

        :param username: The username of the user to find.
        :type username: str
        :return: The found user document or None if not found.
        :rtype: dict or None
        """
        return self.db['users'].find_one({'username': username})
    
    def find_user_id_by_name(self, username: str):
        """
        Finds the user ID in the 'users' collection by username.

        :param username: The username of the user to find.
        :type username: str
        :return: The user ID or None if the user is not found.
        :rtype: ObjectId or None
        """
        user = self.find_user_by_name(username)
        if user is not None:
            return user['_id']
        return None
    
    def add_block_entry_by_username(self, username: str, time: float):
        """
        Adds a block entry for a user with the given username.

        :param username: The username of the user to add a block entry for.
        :type username: str
        :param time: The timestamp for the block entry.
        :type time: float
        :return: The result of the block entry addition.
        :rtype: pymongo.results.InsertOneResult
        """
        user_id = self.find_user_id_by_name(username)
        return self.add_block_entry(user_id, time)
    
    def add_block_entry(self, user_id: str, time: float):
        """
        Adds a block entry for a user with the given user ID.

        :param user_id: The user ID for the user to add a block entry for.
        :type user_id: ObjectId
        :param time: The timestamp for the block entry.
        :type time: float
        :return: The result of the block entry addition.
        :rtype: pymongo.results.InsertOneResult
        """
        return self.db['block_entries'].insert_one({
            'user_id': user_id,
            'timestamp': time
        })
    
    def find_block_entries_by_username(self, username: str):
        """
        Finds block entries for a user with the given username.

        :param username: The username of the user to find block entries for.
        :type username: str
        :return: A cursor to the block entries for the user.
        :rtype: pymongo.cursor.Cursor
        """
        user_id = self.find_user_id_by_name(username)
        return self.find_block_entries(user_id)
    
    def find_block_entries(self, user_id: str):
        """
        Finds block entries for a user with the given user ID.

        :param user_id: The user ID for the user to find block entries for.
        :type user_id: ObjectId
        :return: A cursor to the block entries for the user.
        :rtype: pymongo.cursor.Cursor
        """
        return self.db['block_entries'].find({'user_id': user_id})
    
    def delete_block_entries_before_time(self, time: float):
        """
        Deletes block entries with timestamps earlier than the given time.

        :param time: The timestamp to compare against for deletion.
        :type time: float
        :return: The result of the block entries deletion operation.
        :rtype: pymongo.results.DeleteResult
        """
        return self.db['block_entries'].delete_many({'timestamp': {'$lt': time}})
    
    def add_token_by_username(self, username: str, ip: str, salt: str, token: str, timestamp: float):
        """
        Adds a token for a user with the given username.

        :param username: The username of the user to add a token for.
        :type username: str
        :param ip: The IP address associated with the token.
        :type ip: str
        :param salt: The salt value for the token.
        :type salt: str
        :param token: The token value.
        :type token: str
        :param timestamp: The timestamp for the token.
        :type timestamp: float
        :return: The result of the token addition.
        :rtype: pymongo.results.InsertOneResult
        """
        user_id = self.find_user_id_by_name(username)
        return self.add_token(self, user_id, ip, salt, token, timestamp)
    
    def add_token(self, user_id: str, ip: str, salt: str, token: str, timestamp: float):
        """
        Adds a token for a user with the given user ID.

        :param user_id: The user ID for the user to add a token for.
        :type user_id: ObjectId
        :param ip: The IP address associated with the token.
        :type ip: str
        :param salt: The salt value for the token.
        :type salt: str
        :param token: The token value.
        :type token: str
        :param timestamp: The timestamp for the token.
        :type timestamp: float
        :return: The result of the token addition.
        :rtype: pymongo.results.InsertOneResult
        """
        return self.db['tokens'].insert_one({
            'user_id': user_id,
            'ip': ip,
            'salt': salt,
            'token': token,
            'timestamp': timestamp
        })
    
    def find_token_by_username(self, username: str, ip: str):
        """
        Finds tokens for a user with the given username and IP.

        :param username: The username of the user to find tokens for.
        :type username: str
        :param ip: The IP address associated with the tokens.
        :type ip: str
        :return: A cursor to the tokens for the user and IP.
        :rtype: pymongo.cursor.Cursor
        """
        user_id = self.find_user_id_by_name(username)
        return self.find_token(user_id, ip)
    
    def find_token(self, user_id: str, ip: str):
        """
        Finds tokens for a user with the given user ID and IP.

        :param user_id: The user ID for the user to find tokens for.
        :type user_id: ObjectId
        :param ip: The IP address associated with the tokens.
        :type ip: str
        :return: A cursor to the tokens for the user and IP.
        :rtype: pymongo.cursor.Cursor
        """
        return self._mongo.db['tokens'].find(
            {'user_id': user_id, 'ip': ip},
            {'token': 1, 'salt': 1}
        )
    
    def delete_token_by_username_ip(self, username: str, ip: str):
        """
        Deletes tokens for a user with the given username and IP.

        :param username: The username of the user to delete tokens for.
        :type username: str
        :param ip: The IP address associated with the tokens.
        :type ip: str
        :return: The result of the tokens deletion operation.
        :rtype: pymongo.results.DeleteResult
        """
        user_id = self.find_user_id_by_name(username)
        return self.delete_token_by_username_ip(user_id, ip)
    
    def delete_token_by_userid_ip(self, user_id: str, ip: str):
        """
        Deletes tokens for a user with the given user ID and IP.

        :param user_id: The user ID for the user to delete tokens for.
        :type user_id: ObjectId
        :param ip: The IP address associated with the tokens.
        :type ip: str
        :return: The result of the tokens deletion operation.
        :rtype: pymongo.results.DeleteResult
        """
        return self.db['tokens'].delete_many({'user_id': user_id, 'ip': ip})
    
    def delete_token_before_time(self, time: float):
        """
        Deletes tokens with timestamps earlier than the given time.

        :param time: The timestamp to compare against for deletion.
        :type time: float
        :return: The result of the tokens deletion operation.
        :rtype: pymongo.results.DeleteResult
        """
        return self.db['tokens'].delete_many({'timestamp': {'$lt': time}})
    
    def add_experiment(self, experiment: dict):
        return self.db['experiments'].insert_one(experiment)
    
    def add_batches(self, batches: dict):
        return self.db['batches'].insert_many(batches)
    
    def find_batch(self, match: dict):
        return self.db['batches'].find_one(match)
    
    def find_batch_state(self, match: dict):
        return self.db['batches'].find_one(match, {'state': 1})
    
    def find_batches(self, match: dict, projection: dict):
        return self.db['batches'].find(match, projection)
    
    def update_batch(self, match: dict, update: dict):
        return self.db['batches'].update_one(match, update)
    
    def find_all_nodes(self):
        return self.db['nodes'].find()
    
    def find_experiments(self, match: dict, projection: dict):
        return self.db['experiments'].find(match, projection)

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
