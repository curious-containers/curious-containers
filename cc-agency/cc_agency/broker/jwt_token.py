import secrets
import os

from filelock import FileLock


JWT_TEMP_FILE = '/tmp/cc_jwt_secret'
JWT_TEMP_FILE_LOCK = JWT_TEMP_FILE + '.lock'


def generate_jwt_secret_key():
    return secrets.token_urlsafe(32)


def save_jwt_secret_key(secret_key):
    try:
        with FileLock(JWT_TEMP_FILE_LOCK):
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


def get_jwt_secret_key():
    secret_key = read_jwt_file()
    
    if secret_key == '':
        secret_key = generate_jwt_secret_key()
        is_saved = save_jwt_secret_key(secret_key)
        
        if not is_saved:
            secret_key = read_jwt_file()
    
    return secret_key
