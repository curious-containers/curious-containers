#!/usr/bin/env python3


from os import urandom, environ
from cc_agency.commons.helper import create_kdf
from cc_agency.commons.conf import Conf
from cc_agency.commons.db import Mongo


username = environ.get('AGENCY_USER', 'testuser')
password = environ.get('AGENCY_PASSWORD', 'testpassword')

conf = Conf(None)
mongo = Mongo(conf)

salt = urandom(16)
kdf = create_kdf(salt)
user = {
    'username': username,
    'password': kdf.derive(password.encode('utf-8')),
    'salt': salt,
    'is_admin': True,
}
mongo.add_user(user)

print('created broker user')
