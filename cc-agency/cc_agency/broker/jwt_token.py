import secrets
import os
from datetime import timedelta

from filelock import FileLock


JWT_TEMP_FILE = '/tmp/cc_jwt_secret'
JWT_TEMP_FILE_LOCK = JWT_TEMP_FILE + '.lock'

DEFAULT_ACCESS_TOKEN_EXPIRES = 900
DEFAULT_REFRESH_TOKEN_EXPIRES = 43200


def generate_jwt_secret_key():
    return secrets.token_urlsafe(32)


def save_jwt_secret_key(secret_key):
    try:
        with FileLock(JWT_TEMP_FILE_LOCK):
            if os.path.exists(JWT_TEMP_FILE):
                return False
            with open(JWT_TEMP_FILE, 'a') as f:
                f.write(secret_key)
            os.chmod(JWT_TEMP_FILE, 0o640)
        return True
    except Exception:
        return False


def read_jwt_file():
    try:
        with open(JWT_TEMP_FILE, 'r') as f:
            return f.read()
    except Exception:
        return ''


def get_jwt_secret_key(jwt_config):
    secret_key = jwt_config.get('secret_key', read_jwt_file())
    
    if secret_key == '':
        secret_key = generate_jwt_secret_key()
        is_saved = save_jwt_secret_key(secret_key)
        
        if not is_saved:
            secret_key = read_jwt_file()
    
    return secret_key


def configure_jwt(app, conf):
    jwt_config = conf.d['broker']['auth'].get('jwt', {})
    
    app.config["JWT_SECRET_KEY"] = get_jwt_secret_key(jwt_config)
    
    access_token_expires = jwt_config.get('access_token_expires', DEFAULT_ACCESS_TOKEN_EXPIRES)
    refresh_token_expires = jwt_config.get('refresh_token_expires', DEFAULT_REFRESH_TOKEN_EXPIRES)
    
    app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(seconds=access_token_expires)
    app.config["JWT_REFRESH_TOKEN_EXPIRES"] = timedelta(seconds=refresh_token_expires)