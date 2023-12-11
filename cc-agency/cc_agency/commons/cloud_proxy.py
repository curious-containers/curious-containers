import socket
import requests
from requests.auth import HTTPBasicAuth
from contextlib import closing
from werkzeug.exceptions import InternalServerError
from urllib.parse import urlparse

class CloudProxy():
    """
    Provides functionality for interacting with a cloud proxy.

    :ivar bool enable: Indicates if cc-cloud is enabled.
    :ivar str internal_url: The internel url address of the cc-cloud service.
    :ivar str admin_username: The username for the a admin user, that will be used for the communication with the cc-cloud service.
    :ivar str admin_password: The password for the a admin user, that will be used for the communication with the cc-cloud service.
    :ivar str sshPort: The SSH port used by the cc-cloud service.
    :ivar str upload_directory_name: The name of the upload directory on the cc-cloud service.
    :ivar bool disableStrictHostKeyChecking: Indicates if strict host key checking is disabled when mounting via sshfs.
    :ivar str publicsshKey: The public SSH key associated with the cc-cloud server.
    :ivar mongo: The MongoDB connection object.
    :ivar auth: The authentication object for creating cloud users.
    """
    
    CREATE_USER_ENDPOINT = 'create_user'
    
    def __init__(self, conf, mongo, auth):
        """
        Initializes a CloudProxy object.

        :param conf: The configuration object.
        :type conf: object
        :param mongo: The MongoDB connection object.
        :type mongo: object
        :param auth: The authentication object for creating cloud users.
        :type auth: object
        """
        if 'cloud' in conf.d:
            self.enable = conf.d['cloud']['enable']
            self.internal_url = conf.d['cloud']['internal_url']
            self.admin_username = conf.d['cloud']['username']
            self.admin_password = conf.d['cloud']['password']
            
            self.sshPort = conf.d['cloud'].get('sshPort', '22')
            self.upload_directory_name = conf.d['cloud'].get('upload_directory_name', 'cloud')
            self.disableStrictHostKeyChecking = conf.d['cloud'].get('disableStrictHostKeyChecking', False)
            self.publicsshKey = conf.d['cloud'].get('publicsshKey', '')
            
            self.mongo = mongo
            self.auth = auth
            self.update_admin_cloud_user()
        else:
            self.enable = False
    
    def update_admin_cloud_user(self):
        """
        Creates or updates the admin users in the database
        for interacting with the cc-cloud service.
        """
        self.auth.create_user(self.admin_username, self.admin_password, True)
    
    def is_available(self):
        """
        Checks if the cc-cloud service is available.

        :return: True if the cc-cloud service is available, False otherwise.
        :rtype: bool
        """
        if not self.enable:
            return False
        
        parsed_url = urlparse(self.internal_url)
        check_ports = [
            self.sshPort,
            parsed_url.port
        ]
        
        host = urlparse(self.internal_url).hostname
        for port in check_ports:
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                if sock.connect_ex((host, port)) != 0:
                    return False
        return True
    
    def create_cloud_user(self, user_id):
        """
        Creates a cloud user for cc-cloud.

        :param user_id: The user ID of the cloud user to create.
        :type user_id: str
        :return: True if the user was created successfully, False otherwise.
        :rtype: bool
        """        
        parameter = f"user_id={user_id}"
        url = f"{self.internal_url.rstrip('/')}/{self.CREATE_USER_ENDPOINT}?{parameter}"
        
        response = requests.get(
            url,
            auth=HTTPBasicAuth(self.admin_username, self.admin_password),
            verify=True
        )
        return response.ok
    
    def complete_cloud_red_data(self, red_data, user_id):
        """
        Completes the cc-cloud related red data in the provided data dictionary.

        :param red_data: The data dictionary to complete with cloud-related information.
        :type red_data: dict
        :param user_id: The user ID associated with the cloud data.
        :type user_id: str
        :return: The updated red data dictionary.
        :rtype: dict
        :raise InternalServerError: If the cloud user cannot be retrieved from the database.
        """
        cloud_user = self.mongo.find_cloud_user(user_id)
        if cloud_user is None:
            raise InternalServerError('Cloud service failed. Could not retrieve cloud user.')
        
        red_data['cloud']['auth'] = {
            'ssh_user': cloud_user['ssh_user'],
            'password':  cloud_user['ssh_password']
        }
        red_data['cloud']['host'] = urlparse(self.internal_url).hostname
        red_data['cloud']['sshPort'] = self.sshPort
        red_data['cloud']['upload_directory_name'] = self.upload_directory_name
        red_data['cloud']['disableStrictHostKeyChecking'] = self.disableStrictHostKeyChecking
        red_data['cloud']['publicsshKey'] = self.publicsshKey
        
        return red_data