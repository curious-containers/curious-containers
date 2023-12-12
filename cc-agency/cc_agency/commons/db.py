import gridfs
from gridfs.grid_file import GridOut
import pymongo
from bson.objectid import ObjectId


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
        return self.db['users'].update_one({'username': user['username']}, {'$set': user}, upsert=True)
    
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
    
    def rename_user(self, old_username: str, new_username: str):
        """
        Renames a user in the 'users' collection by username.

        :param old_username: The current username of the user to be renamed.
        :type old_username: str
        :param new_username: The new username to assign to the user.
        :type new_username: str
        :return: The result of the user renaming operation.
        :rtype: pymongo.results.UpdateResult
        """
        return self.db['users'].update_one({'username': old_username}, {'$set': {'username': new_username}}, upsert=True)
    
    def delete_user(self, username: str):
        """
        Deletes a user from the 'users' collection based on the provided username.

        :param username: The username of the user to be deleted.
        :type username: str
        :return: The result of the user deletion operation.
        :rtype: pymongo.results.DeleteResult
        """
        return self.db['users'].delete_one({'username': username})
    
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
        return self.add_token(user_id, ip, salt, token, timestamp)
    
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
        return self.db['tokens'].find(
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
        return self.delete_token_by_userid_ip(user_id, ip)
    
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
        """
        Adds an experiment to the 'experiments' collection in the MongoDB.

        :param experiment: The experiment data to be added.
        :type experiment: dict
        :return: The result of the experiment addition.
        :rtype: pymongo.results.InsertOneResult
        """
        return self.db['experiments'].insert_one(experiment)
    
    def find_experiment(self, match: dict, projection: dict = None):
        """
        Finds a single experiment in the 'experiments' collection based on the provided match criteria.

        :param match: The criteria to match experiments.
        :type match: dict
        :param projection: The fields to include or exclude from the result.
        :type projection: dict or None
        :return: The found experiment document or None if not found.
        :rtype: dict or None
        """
        return self.db['experiments'].find_one(match, projection)
    
    def find_experiment_by_id(self, id: str, projection: dict = None):
        """
        Finds an experiment in the 'experiments' collection by its ID.

        :param id: The ID of the experiment.
        :type id: str
        :param projection: The fields to include or exclude from the result.
        :type projection: dict or None
        :return: The found experiment document or None if not found.
        :rtype: dict or None
        """
        return self.find_experiment({'_id': ObjectId(id)}, projection)
    
    def find_experiments(self, match: dict, projection: dict = None):
        """
        Finds experiments in the 'experiments' collection based on the provided match criteria.

        :param match: The criteria to match experiments.
        :type match: dict
        :param projection: The fields to include or exclude from the result.
        :type projection: dict or None
        :return: A cursor to the matching experiments.
        :rtype: pymongo.cursor.Cursor
        """
        return self.db['experiments'].find(match, projection)
    
    def find_destinct_experiment_values(self, key: str):
        """
        Finds distinct values for a given key in the 'experiments' collection.

        :param key: The key for which to find distinct values.
        :type key: str
        :return: A list of distinct values.
        :rtype: list
        """
        return self.db['experiments'].distinct(key)
    
    def update_experiment(self, match: dict, update: dict):
        """
        Updates an experiment in the 'experiments' collection based on the provided match criteria.

        :param match: The criteria to match experiments.
        :type match: dict
        :param update: The update to apply to matching experiments.
        :type update: dict
        :return: The result of the experiment update.
        :rtype: pymongo.results.UpdateResult
        """
        return self.db['experiments'].update_one(match, update)
    
    def add_batches(self, batches: dict):
        """
        Adds multiple batches to the 'batches' collection in the MongoDB.

        :param batches: The batches to be added.
        :type batches: dict
        :return: The result of the batch addition.
        :rtype: pymongo.results.InsertManyResult
        """
        return self.db['batches'].insert_many(batches)
    
    def find_batch(self, match: dict, projection: dict = None):
        """
        Finds a single batch in the 'batches' collection based on the provided match criteria.

        :param match: The criteria to match batches.
        :type match: dict
        :param projection: The fields to include or exclude from the result.
        :type projection: dict or None
        :return: The found batch document or None if not found.
        :rtype: dict or None
        """
        return self.db['batches'].find_one(match, projection)
    
    def find_batch_by_id(self, id: str, projection: dict = None):
        """
        Finds a batch in the 'batches' collection by its ID.

        :param id: The ID of the batch.
        :type id: str
        :param projection: The fields to include or exclude from the result.
        :type projection: dict or None
        :return: The found batch document or None if not found.
        :rtype: dict or None
        """
        return self.find_batch({'_id': id}, projection)
    
    def find_batches(self, match: dict, projection: dict = None):
        """
        Finds batches in the 'batches' collection based on the provided match criteria.

        :param match: The criteria to match batches.
        :type match: dict
        :param projection: The fields to include or exclude from the result.
        :type projection: dict or None
        :return: A cursor to the matching batches.
        :rtype: pymongo.cursor.Cursor
        """
        return self.db['batches'].find(match, projection)
    
    def count_batches(self, match: dict):
        """
        Counts batches in the 'batches' collection based on the provided match criteria.

        :param match: The criteria to match batches.
        :type match: dict
        :return: The count of matching batches.
        :rtype: int
        """
        return self.db['batches'].count(match)
    
    def aggregate_batches(self, pipeline: list):
        """
        Aggregates batches in the 'batches' collection using the provided pipeline.

        :param pipeline: The aggregation pipeline.
        :type pipeline: list
        :return: The result of the batch aggregation.
        :rtype: pymongo.command_cursor.CommandCursor
        """
        return self.db['batches'].aggregate(pipeline)
    
    def update_batch(self, match: dict, update: dict):
        """
        Updates a batch in the 'batches' collection based on the provided match criteria.

        :param match: The criteria to match batches.
        :type match: dict
        :param update: The update to apply to matching batches.
        :type update: dict
        :return: The result of the batch update.
        :rtype: pymongo.results.UpdateResult
        """
        return self.db['batches'].update_one(match, update)
    
    def update_batches(self, match: dict, update: dict):
        """
        Updates multiple batches in the 'batches' collection based on the provided match criteria.

        :param match: The criteria to match batches.
        :type match: dict
        :param update: The update to apply to matching batches.
        :type update: dict
        :return: The result of the batch updates.
        :rtype: pymongo.results.UpdateResult
        """
        return self.db['batches'].update_many(match, update)
    
    def find_cloud_user(self, user_id: str, projection: dict = None):
        """
        Finds a cloud user in the 'cloud_users' collection by user ID.

        :param user_id: The ID of the cloud user.
        :type user_id: str
        :param projection: The fields to include or exclude from the result.
        :type projection: dict or None
        :return: The found cloud user document or None if not found.
        :rtype: dict or None
        """
        return self.db['cloud_users'].find_one({'user_id': user_id}, projection)
    
    def add_node(self, node_name, state: str = None, history: list = [], ram: int = None, cpus: int = None, gpus: int = None):
        """
        Adds a node to the 'nodes' collection in the MongoDB.

        :param node_name: The name of the node.
        :type node_name: str
        :param state: The state of the node (optional).
        :type state: str or None
        :param history: The history of the node (optional).
        :type history: list
        :param ram: The RAM capacity of the node in megabytes (optional).
        :type ram: int or None
        :param cpus: The number of CPUs in the node (optional).
        :type cpus: int or None
        :param gpus: The number of GPUs in the node (optional).
        :type gpus: int or None
        :return: The result of the node addition.
        :rtype: pymongo.results.InsertOneResult
        """
        return self.db['nodes'].insert_one({
            'nodeName': node_name,
            'state': state,
            'history': history,
            'ram': ram,
            'cpus': cpus,
            'gpus': gpus
        })
    
    def find_nodes(self, match: dict = {}, projection: dict = None):
        """
        Finds nodes in the 'nodes' collection based on the provided match criteria.

        :param match: The criteria to match nodes.
        :type match: dict
        :param projection: The fields to include or exclude from the result.
        :type projection: dict or None
        :return: A cursor to the matching nodes.
        :rtype: pymongo.cursor.Cursor
        """
        return self.db['nodes'].find(match, projection)
    
    def update_node(self, match: dict, update: dict):
        """
        Updates a node in the 'nodes' collection based on the provided match criteria.

        :param match: The criteria to match nodes.
        :type match: dict
        :param update: The update to apply to matching nodes.
        :type update: dict
        :return: The result of the node update.
        :rtype: pymongo.results.UpdateResult
        """
        return self.db['nodes'].update_one(match, update)
    
    def drope_nodes(self):
        """
        Drops the 'nodes' collection, removing all nodes.
        """
        self.db['nodes'].drop()

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
