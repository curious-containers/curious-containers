import random
import string
from flask import request
from cc_agency.commons.helper import create_flask_response

def admin_routes(app, auth):
    
    @app.route('/admin/create_user', methods=['GET'])
    def create_user():
        user = auth.verify_user(request.authorization, request.cookies, request.remote_addr)
        random_password = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(24))
        create_username = request.args.get('username')
        create_password = request.args.get('password', random_password)
        
        response_string = 'not authorized'
        if user.is_admin:
            auth.create_user(create_username, create_password, False)
            response_string = 'ok'
        
        return create_flask_response(response_string, auth, user.authentication_cookie)
    
    
    @app.route('/admin/remove_user', methods=['GET'])
    def remove_user():
        user = auth.verify_user(request.authorization, request.cookies, request.remote_addr)
        remove_username = request.args.get('username')
        
        response_string = 'not authorized'
        if user.is_admin:
            auth.remove_user(remove_username)
            response_string = 'ok'
        
        return create_flask_response(response_string, auth, user.authentication_cookie)
    
    
    @app.route('/admin/set_password', methods=['GET'])
    def set_password():
        user = auth.verify_user(request.authorization, request.cookies, request.remote_addr)
        ch_username = request.args.get('username')
        ch_password = request.args.get('password')
        
        response_string = 'not authorized'
        if user.is_admin:
            try:
                auth.set_user_password(ch_username, ch_password)
                response_string = 'ok'
            except TypeError:
                response_string = f"user '{ch_username}' does not exist"
        
        return create_flask_response(response_string, auth, user.authentication_cookie)