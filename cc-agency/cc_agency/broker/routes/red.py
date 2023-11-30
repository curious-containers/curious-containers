import json
from time import time
from functools import wraps

from flask import request, jsonify, Response
from flask_jwt_extended import create_access_token, create_refresh_token, get_jwt, get_jwt_identity, \
    jwt_required, set_access_cookies, current_user
from red_val.red_validation import red_validation
from red_val.red_variables import get_variable_keys
from werkzeug.exceptions import BadRequest, NotFound, InternalServerError
from bson.objectid import ObjectId

from cc_core.commons.engines import engine_validation
from cc_core.commons.red_secrets import get_secret_values, normalize_keys
from cc_core.commons.exceptions import exception_format
from cc_core.commons.red_to_restricted_red import convert_red_to_restricted_red
from cc_core.commons.schemas.engines.container import container_engines

from cc_agency.commons.helper import str_to_bool, create_flask_response, USER_SPECIFIED_STDOUT_KEY, \
    USER_SPECIFIED_STDERR_KEY, get_gridfs_filename, create_file_flask_response, STDOUT_FILE_KEY, STDERR_FILE_KEY
from cc_agency.commons.secrets import separate_secrets_batch, separate_secrets_experiment
from cc_agency.commons.db import Mongo
from cc_agency.broker.auth import Auth
from cc_agency.version import VERSION as AGENCY_VERSION

from red_val.schemas.red import red_schema


def _prepare_red_data(data, user, disable_retry, disable_connector_validation):
    timestamp = time()

    experiment = {
        'username': user.username,
        'registrationTime': timestamp,
        'redVersion': data['redVersion'],
        'cli': data['cli'],
        'container': data['container'],
        'protectedKeysVoided': False
    }

    if 'execution' in data:
        stripped_settings = {}

        # add settings to experiment
        for key, val in data['execution']['settings'].items():
            if key == 'access':
                continue

            stripped_settings[key] = val

        # add retry if failed to experiment settings
        stripped_settings['retryIfFailed'] = not disable_retry
        stripped_settings['disableConnectorValidation'] = disable_connector_validation

        experiment['execution'] = {
            'engine': data['execution']['engine'],
            'settings': stripped_settings
        }

    experiment, secrets = separate_secrets_experiment(experiment)

    if 'batches' in data:
        raw_batches = data['batches']
    else:
        raw_batches = [{
            'inputs': data['inputs'],
            'outputs': data['outputs']
        }]
        if 'cloud' in data:
            raw_batches[0]['cloud'] = data['cloud']

    batches = []

    for i, rb in enumerate(raw_batches):
        batch = {
            'username': user.username,
            'registrationTime': timestamp,
            'state': 'registered',
            'batchesListIndex': i,
            'protectedKeysVoided': False,
            'notificationsSent': False,
            'node': None,
            'history': [{
                'state': 'registered',
                'time': timestamp,
                'debugInfo': None,
                'node': None,
                'ccagent': None,
                # 'dockerStats': None
            }],
            'attempts': 0,
            'inputs': rb['inputs'],
            'outputs': rb['outputs'],
            USER_SPECIFIED_STDOUT_KEY: False,
            USER_SPECIFIED_STDERR_KEY: False,
            STDOUT_FILE_KEY: None,
            STDERR_FILE_KEY: None
        }
        if 'cloud' in rb:
            batch['cloud'] = rb['cloud']
        batch, additional_secrets = separate_secrets_batch(batch)
        secrets.update(additional_secrets)
        batches.append(batch)
    
    return experiment, batches, secrets


def red_routes(app, jwt, mongo, auth, controller, trustee_client, cloud_proxy):
    """
    Creates the red broker endpoints.

    :param app: The flask app to attach to
    :param mongo: The mongo client
    :type mongo: Mongo
    :param auth: The authorization module to use
    :type auth: Auth
    :param controller: The controller to communicate with the scheduler
    :param trustee_client: The trustee client
    """
    
    def jwt_or_basic(func):
        """
        A decorator that combines JWT (JSON Web Token) authentication with Basic authentication.

        This decorator is used to protect routes with authentication. It first checks for a valid
        JWT token in the request headers. If a valid token is found, it allows access to the route.
        If no JWT token is present, it falls back to Basic authentication using the 'Authorization'
        header. If valid Basic authentication credentials are provided, it allows access to the route.

        :param func: The function or route to be protected.
        :type func: callable
        :return: The decorated function.
        :rtype: callable
        """
        @jwt_required(optional=True)
        def wrapper(*args, **kwargs):
            current_user = get_jwt_identity()
            if current_user:
                return func(*args, **kwargs)
            else:
                return Response('Unauthorized', 401, {'WWW-Authenticate': 'Basic realm="Please fill in username and password"'})
        return wrapper
    
    @app.before_request
    def authenticate_user():
        """
        Authenticate the user before processing a request.

        This function is executed before each request is processed by the application.
        It checks for Basic authentication credentials in the request headers. If valid
        credentials are provided, it generates and adds a JWT token to the request's
        environment, allowing the user to access protected routes.

        :return: None
        """
        if request.authorization:
            user = auth.verify_user(request.authorization, request.cookies, request.remote_addr)
            if user:
                access_token = create_access_token(identity=user)
                request.environ['HTTP_AUTHORIZATION'] = f'Bearer {access_token}'
    
    @app.route("/login", methods=["POST"])
    def login():
        """
        Authenticate a user and return a JWT access token.

        This route handles user authentication. If valid credentials are provided, it generates
        and returns a JWT access token in JSON format. If the credentials are invalid, it
        returns a JSON response indicating unauthorized access.

        :return: JSON response with an access token or an error message.
        :rtype: Response
        """
        user = auth.verify_user(request.authorization, request.cookies, request.remote_addr)
        if user is None:
            return jsonify({"msg": "Bad username or password"}), 401
        
        access_token = create_access_token(identity=user)
        refresh_token = create_refresh_token(identity=user)
        return jsonify(username=user.username, access_token=access_token, refresh_token=refresh_token)
    
    @app.route("/refreshtoken", methods=["POST"])
    @jwt_required(refresh=True)
    def refresh():
        identity = get_jwt_identity()
        access_token = create_access_token(identity=identity)
        return jsonify(access_token=access_token)
    
    @jwt.user_identity_loader
    def user_identity_lookup(user):
        """
        User identity loader for JWT authentication.

        This function is used to determine the identity of the user when creating a JWT token.
        It extracts the username from the user object.

        :param user: The user object.
        :type user: Auth.User
        :return: The username of the user.
        :rtype: str
        """
        if (type(user) == str):
            return user
        else:
            return user.username
    
    @jwt.user_lookup_loader
    def user_lookup_callback(_jwt_header, jwt_data):
        """
        User lookup callback for JWT authentication.

        This function is used to look up a user based on the 'sub' (subject) claim in the JWT data.
        It retrieves the user from the database based on the username stored in the JWT data.

        :param _jwt_header: The JWT header.
        :type _jwt_header: dict
        :param jwt_data: The JWT data containing the 'sub' claim.
        :type jwt_data: dict
        :return: The user object.
        :rtype: Auth.User
        """
        username = jwt_data["sub"]
        db_user = auth._mongo.db['users'].find_one({'username': username})
        user = Auth.User(username, db_user['is_admin'])
        return user

    @app.errorhandler(BadRequest)
    def bad_request_handler(e):
        """
        An exception handler for bad requests, that puts the exception text into json.

        :param e: The BadRequest exception, that was thrown
        :type e: BadRequest
        :return: A response with json describing the error and the bad requests return code
        """
        response = e.get_response()
        response.data = json.dumps(str(e))
        response.content_type = 'application/json'

        return response, 400

    @app.route('/red', methods=['POST'])
    @jwt_or_basic
    def post_red():
        if not request.json:
            raise BadRequest('Did not send RED data as JSON.')

        data = request.json
        disable_retry = str_to_bool(request.args.get('disableRetry', default=False))
        disable_connector_validation = str_to_bool(request.args.get('disableConnectorValidation', default=False))

        try:
            red_validation(data, False)
        except Exception as e:
            raise BadRequest(
                'Given RED data is invalid. Consider using the FAICE commandline tools for local validation.\n{}'
                .format(str(e))
            )

        template_keys = get_variable_keys(data)
        if template_keys:
            raise BadRequest(
                'The given red data contains the following variables: "{}". Please resolve them before submitting'
                ' to agency. Consider using CC-FAICE (faice exec).'.format(', '.join(map(str, template_keys)))
            )

        secret_values = get_secret_values(data)

        if 'batches' in data:
            for batch in data['batches']:
                if 'outputs' not in batch:
                    raise BadRequest(
                        'CC-Agency requires all batches to have outputs defined. At least one batch does not comply.'
                    )

        elif 'outputs' not in data:
            raise BadRequest('CC-Agency requires outputs to be defined in RED data.')

        try:
            engine_validation(data, 'container', ['docker'])
        except Exception:
            raise BadRequest('\n'.join(exception_format(secret_values=secret_values)))

        if 'ram' not in data['container']['settings']:
            raise BadRequest('CC-Agency requires \'ram\' to be defined in the container settings.')

        try:
            engine_validation(data, 'execution', ['ccagency'], optional=True)
            normalize_keys(data)
            _ = convert_red_to_restricted_red(data)
        except Exception:
            raise BadRequest('\n'.join(exception_format(secret_values=secret_values)))
        
        if 'cloud' in data and data['cloud'].get('enable'):
            if not cloud_proxy.is_available():
                raise BadRequest('CC-Cloud is not available.')
            data = cloud_proxy.complete_cloud_red_data(data, current_user.username)
        
        experiment, batches, secrets = _prepare_red_data(data, current_user, disable_retry, disable_connector_validation)

        response = trustee_client.store(secrets)
        if response['state'] == 'failed':
            raise InternalServerError('Trustee service failed:\n{}'.format(response['debug_info']))

        bson_experiment_id = mongo.db['experiments'].insert_one(experiment).inserted_id
        experiment_id = str(bson_experiment_id)

        for batch in batches:
            batch['experimentId'] = experiment_id
        
        mongo.db['batches'].insert_many(batches)

        controller.send_json({'destination': 'scheduler'})

        return create_flask_response({'experimentId': experiment_id}, auth, current_user.authentication_cookie)

    @app.route('/batches/<object_id>', methods=['DELETE'], endpoint='delete_batches')
    @jwt_or_basic
    def delete_batches(object_id):
        try:
            bson_id = ObjectId(object_id)
        except Exception:
            raise BadRequest('Not a valid BSON ObjectId.')

        match = {'_id': bson_id}
        match_with_state = {'_id': bson_id, 'state': {'$nin': ['succeeded', 'failed', 'cancelled']}}

        if not current_user.is_admin:
            match['username'] = current_user.username
            match_with_state['username'] = current_user.username

        o = mongo.db['batches'].find_one(match, {'state': 1})
        if not o:
            raise NotFound('Could not find Object.')

        mongo.db['batches'].update_one(
            match_with_state,
            {
                '$set': {
                    'state': 'cancelled'
                },
                '$push': {
                    'history': {
                        'state': 'cancelled',
                        'time': time(),
                        'debugInfo': None,
                        'node': None,
                        'ccagent': None,
                        # 'dockerStats': None
                    }
                }
            })

        o = mongo.db['batches'].find_one(match)
        o['_id'] = str(o['_id'])

        controller.send_json({'destination': 'scheduler'})

        return create_flask_response(o, auth, current_user.authentication_cookie)

    @app.route('/experiments/count', methods=['GET'])
    def get_experiments_count():
        return get_collection_count('experiments')

    @app.route('/experiments', methods=['GET'])
    def get_experiments():
        return get_collection('experiments')

    @app.route('/experiments/<object_id>', methods=['GET'])
    def get_experiments_id(object_id):
        return get_collection_id('experiments', object_id)

    @app.route('/batches/count', methods=['GET'])
    def get_batches_count():
        return get_collection_count('batches')

    @app.route('/batches', methods=['GET'])
    def get_batches():
        return get_collection('batches')

    @app.route('/batches/<object_id>', methods=['GET'])
    def get_batches_id(object_id):
        return get_collection_id('batches', object_id)

    @app.route('/batches/<batch_id>/<filename>', methods=['GET'], endpoint='get_batches_id_file')
    @jwt_or_basic
    def get_batches_id_file(batch_id, filename):
        try:
            bson_id = ObjectId(batch_id)
        except Exception:
            raise BadRequest('Not a valid batch ID.')

        match = {'_id': bson_id}

        if not current_user.is_admin:
            match['username'] = current_user.username

        o = mongo.db['batches'].find_one(match)
        if not o:
            raise NotFound('Could not find batch with id "{}".'.format(batch_id))

        if filename not in ('stdout', 'stderr'):
            raise BadRequest('Could not transfer "{}". Use "stdout" or "stderr" as filename.'.format(filename))

        db_filename = get_gridfs_filename(batch_id, filename)

        data = mongo.get_file(db_filename)
        if data is None:
            raise NotFound('Could not find "{}" of batch "{}"'.format(filename, batch_id))

        return create_file_flask_response(data, auth, current_user.authentication_cookie)

    @jwt_or_basic
    def get_collection_id(collection, object_id):
        try:
            bson_id = ObjectId(object_id)
        except Exception:
            raise BadRequest('Not a valid BSON ObjectId.')

        match = {'_id': bson_id}

        if not current_user.is_admin:
            match['username'] = current_user.username

        o = mongo.db[collection].find_one(match)
        if not o:
            raise NotFound('Could not find Object.')

        o['_id'] = str(o['_id'])
        return create_flask_response(o, auth, current_user.authentication_cookie)

    @jwt_or_basic
    def get_collection_count(collection):
        username = request.args.get('username', default=None, type=str)
        node = None
        experiment_id = None
        state = None

        if collection == 'batches':
            node = request.args.get('node', default=None, type=str)
            state = request.args.get('state', default=None, type=str)
            experiment_id = request.args.get('experimentId', default=None, type=str)

            if experiment_id:
                try:
                    _ = ObjectId(experiment_id)
                except Exception:
                    raise BadRequest('Experiment is not a valid BSON ObjectId.')

        aggregate = []

        if not current_user.is_admin:
            aggregate.append({'$match': {'username': current_user.username}})

        match = {}

        if username:
            match['username'] = username

        if node:
            match['node'] = node

        if experiment_id:
            match['experimentId'] = experiment_id

        if state:
            match['state'] = state

        aggregate.append({'$match': match})
        aggregate.append({'$count': 'count'})

        cursor = mongo.db[collection].aggregate(aggregate)
        cursor = list(cursor)
        
        if not cursor:
            return create_flask_response({'count': 0}, auth, current_user.authentication_cookie)

        return create_flask_response(cursor[0], auth, current_user.authentication_cookie)

    @jwt_or_basic
    def get_collection(collection):
        skip = request.args.get('skip', default=None, type=int)
        limit = request.args.get('limit', default=None, type=int)
        username = request.args.get('username', default=None, type=str)
        ascending = str_to_bool(request.args.get('ascending', default=None, type=str))

        node = None
        experiment_id = None
        state = None

        if collection == 'batches':
            node = request.args.get('node', default=None, type=str)
            state = request.args.get('state', default=None, type=str)

            states = ['registered', 'scheduled', 'processing', 'succeeded', 'failed', 'cancelled']
            if state and state not in states:
                raise BadRequest('Given state is not valid. Must be one of {}.'.format(states))

            experiment_id = request.args.get('experimentId', default=None, type=str)

            if experiment_id:
                try:
                    _ = ObjectId(experiment_id)
                except Exception:
                    raise BadRequest('Experiment is not a valid BSON ObjectId.')

        aggregate = []

        if not current_user.is_admin:
            aggregate.append({'$match': {'username': current_user.username}})

        match = {}

        if username:
            match['username'] = username

        if node:
            match['node'] = node

        if experiment_id:
            match['experimentId'] = experiment_id

        if state:
            match['state'] = state

        aggregate.append({'$match': match})

        aggregate.append({'$project': {
            'username': 1,
            'registrationTime': 1,
            'state': 1,
            'experimentId': 1,
            'node': 1,
            'batchesListIndex': 1,
            'history': {
                'node': 1,
                'state': 1,
                'time': 1
            }
        }})

        aggregate.append({'$sort': {'registrationTime': 1 if ascending else -1}})

        if skip is not None:
            if skip < 0:
                raise BadRequest('skip cannot be lower than 0.')
            aggregate.append({'$skip': skip})

        if limit is not None:
            if limit < 1:
                raise BadRequest('limit cannot be lower than 1.')
            aggregate.append({'$limit': limit})

        cursor = mongo.db[collection].aggregate(aggregate)

        result = []
        for e in cursor:
            e['_id'] = str(e['_id'])
            result.append(e)

        return create_flask_response(result, auth, current_user.authentication_cookie)
    
    @app.route('/version', methods=['GET'], endpoint='get_version')
    @jwt_or_basic
    def get_version():
        return create_flask_response({'agencyVersion': AGENCY_VERSION}, auth, current_user.authentication_cookie)
    
    @app.route('/schema/red', methods=['GET'])
    def get_schema():
        return red_schema, 200
    
    @app.route('/schema/engines/container', methods=['GET'])
    def get_container_engines():
        return container_engines, 200
    
    
    @app.route('/nodes', methods=['GET'], endpoint='get_nodes')
    @jwt_or_basic
    def get_nodes():
        cursor = mongo.db['nodes'].find()

        nodes = list(cursor)
        node_names = [node['nodeName'] for node in nodes]

        cursor = mongo.db['batches'].find(
            {
                'node': {'$in': node_names},
                'state': {'$in': ['scheduled', 'processing']}
            },
            {'experimentId': 1, 'node': 1}
        )
        batches = list(cursor)
        experiment_ids = list(set([ObjectId(b['experimentId']) for b in batches]))

        cursor = mongo.db['experiments'].find(
            {'_id': {'$in': experiment_ids}},
            {'container.settings.ram': 1}
        )
        experiments = {str(e['_id']): e for e in cursor}

        for node in nodes:
            batches_ram = [
                {
                    'batchId': str(b['_id']),
                    'ram': experiments[b['experimentId']]['container']['settings']['ram']
                }
                for b in batches
                if b['node'] == node['nodeName']
            ]
            node['currentBatches'] = batches_ram
            del node['_id']

        return create_flask_response(nodes, auth, current_user.authentication_cookie)
    
