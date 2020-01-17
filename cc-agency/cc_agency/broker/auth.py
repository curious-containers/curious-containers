from os import urandom
from time import time

from cryptography.exceptions import InvalidKey
from werkzeug.datastructures import WWWAuthenticate
from werkzeug.exceptions import Unauthorized

from cc_agency.commons.helper import generate_secret, create_kdf, decode_authentication_cookie, \
    encode_authentication_cookie

AUTHORIZATION_COOKIE_KEY = 'authorization_cookie'
DEFAULT_REALM = 'Please fill in username and password'


class Auth:
    class User:
        """
        Defines a authenticated user
        """
        def __init__(self, username, is_admin):
            """
            Creates a authenticated user

            :param username: The username of the user
            :param is_admin: Whether the user is an admin
            """
            self.username = username
            self.authentication_cookie = None
            self.verified_by_credentials = False
            self.is_admin = is_admin

        def set_authentication_cookie(self, value):
            """
            Sets the authentication cookie to a tuple containing (key, value) with the given value.

            :param value: The value the key should be set to
            :type value: bytes
            """
            self.authentication_cookie = (AUTHORIZATION_COOKIE_KEY, value)

    def __init__(self, conf, mongo):
        self._num_login_attempts = conf.d['broker']['auth']['num_login_attempts']
        self._block_for_seconds = conf.d['broker']['auth']['block_for_seconds']
        self.tokens_valid_for_seconds = conf.d['broker']['auth']['tokens_valid_for_seconds']

        self._mongo = mongo

    def create_user(self, username, password, is_admin):
        salt = urandom(16)
        kdf = create_kdf(salt)
        user = {
            'username': username,
            'password': kdf.derive(password.encode('utf-8')),
            'salt': salt,
            'is_admin': is_admin
        }
        self._mongo.db['users'].update_one({'username': username}, {'$set': user}, upsert=True)

    @staticmethod
    def _create_unauthorized(description, realm=DEFAULT_REALM):
        www_authenticate = WWWAuthenticate()
        www_authenticate.set_basic(realm=realm)
        return Unauthorized(description=description, www_authenticate=www_authenticate.to_header())

    def verify_user(self, auth, cookies, ip):
        """
        Checks if a http request with the given auth is authorized. If it is authorized a AuthUser object is returned,
        otherwise an Unauthorized Exception is raised, containing the www_authenticate header.

        :param auth: The authorization header of a http request
        :type auth: werkzeug.datastructures.Authorization
        :param cookies: The cookies of the request, to check against the authorization cookie
        :param ip: The ip address of the request
        :type ip: str
        :return: If authentication was successful: An Auth.User object containing the username and other information
                                                   about the authorized user
                 If authentication failed: None
        :rtype: Auth.User

        :raise Unauthorized: Raises an Unauthorized exception, if authorization failed.
        """
        # check authorization information
        auth_password = None
        cookie_token = None
        if auth:
            username = auth.username
            auth_password = auth.password
        else:  # check authentication cookie, only if auth is not supplied
            authorization_cookie = cookies.get(AUTHORIZATION_COOKIE_KEY)

            if authorization_cookie:
                username, cookie_token = decode_authentication_cookie(authorization_cookie)
            else:
                raise Auth._create_unauthorized(description='Missing Authentication information')

        db_user = self._mongo.db['users'].find_one({'username': username})  # type: dict

        if not db_user:
            raise Auth._create_unauthorized(description='Could not find user "{}".'.format(username))

        user = Auth.User(username, db_user['is_admin'])

        salt = db_user['salt']
        del db_user['salt']

        if self._is_blocked_temporarily(username):
            raise Auth._create_unauthorized(
                'The user "{}" is currently blocked due to invalid login attempts.'.format(username)
            )

        if self._verify_user_by_credentials(db_user['password'], auth_password, salt):
            user.verified_by_credentials = True
            # create authorization cookie
            if cookie_token is None:
                token = self._issue_token(username, ip)
                user.set_authentication_cookie(encode_authentication_cookie(username, str(token)).encode('utf-8'))
            else:
                # do not create new cookie if one is present
                user.set_authentication_cookie(encode_authentication_cookie(username, cookie_token).encode('utf-8'))
            return user

        if self._verify_user_by_cookie(username, cookie_token, ip):
            user.set_authentication_cookie(encode_authentication_cookie(username, cookie_token).encode('utf-8'))
            return user

        self._add_block_entry(username)
        raise Auth._create_unauthorized('Invalid username/password combination for user "{}".'.format(username))

    def _is_blocked_temporarily(self, username):
        """
        Returns whether the given username is blocked at the moment, because of an invalid login attempt.

        :param username: The username to check against
        :type username: str
        :return: True, if the username is blocked, otherwise False
        :rtype: bool
        """
        self._mongo.db['block_entries'].delete_many({'timestamp': {'$lt': time() - self._block_for_seconds}})
        block_entries = list(self._mongo.db['block_entries'].find({'username': username}))

        if len(block_entries) > self._num_login_attempts:
            return True

        return False

    def _add_block_entry(self, username):
        self._mongo.db['block_entries'].insert_one({
            'username': username,
            'timestamp': time()
        })
        print('Unverified login attempt: added block entry!')

    def _issue_token(self, username, ip):
        """
        Creates a token in the mongo token db with the fields: [username, ip, salt, token, timestamp] and returns it.

        :param username: The user for which a token should be created
        :type username: str
        :param ip: The ip address of the user request
        :type ip: str
        :return: The created token
        :rtype: bytes
        """
        # first remove old tokens of this user and this ip
        self._mongo.db['tokens'].delete_many({'username': username, 'ip': ip})

        salt = urandom(16)
        kdf = create_kdf(salt)
        token = generate_secret()
        self._mongo.db['tokens'].insert_one({
            'username': username,
            'ip': ip,
            'salt': salt,
            'token': kdf.derive(token.encode('utf-8')),
            'timestamp': time()
        })
        return token

    def _verify_user_by_cookie(self, username, cookie_token, ip):
        """
        Returns whether the given user is authorized by the token, given by the received cookies.

        :param username: The username to check for
        :type username: str
        :param cookie_token: The authorization token of the cookie given by the user request
        :type cookie_token: str
        :param ip: The ip address of the user request
        :type ip: str
        :return: True, if the given user could be authorized by an authorization cookie
        :rtype: bool
        """
        # delete old tokens
        self._mongo.db['tokens'].delete_many({'timestamp': {'$lt': time() - self.tokens_valid_for_seconds}})

        # get authorization cookie
        if cookie_token is None:
            return False

        cursor = self._mongo.db['tokens'].find(
            {'username': username, 'ip': ip},
            {'token': 1, 'salt': 1}
        )
        for c in cursor:
            kdf = create_kdf(c['salt'])
            try:
                kdf.verify(cookie_token.encode('utf-8'), c['token'])
                return True
            except InvalidKey:  # if token does not fit, try the next
                pass

        return False

    @staticmethod
    def _verify_user_by_credentials(db_password, request_password, salt):
        """
        Checks if the given user/password combination is authorized

        :param db_password: The user password as stored in the db
        :type db_password: str
        :param request_password: The password string of the user given by the authorization data of the user request
        :param salt: The salt value of the user from the db
        :return:
        """
        if request_password is None:
            return False

        kdf = create_kdf(salt)
        try:
            kdf.verify(request_password.encode('utf-8'), db_password)
        except InvalidKey:
            return False

        return True
