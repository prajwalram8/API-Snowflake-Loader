# credentials/credential_manager.py
from configparser import ConfigParser
import requests

class CredentialManager:
    def __init__(self, config_path = 'config.ini'):
        self.config_path = config_path


    def get_credentials(self, **kwargs):
        """
        Retrieve the stored credentials securely.
        
        :return: dict - The credentials (e.g., API keys, database credentials).
        """
        parser = ConfigParser()
        parser.read(self.config_path)

        credential_dict = {}
        for k, v in kwargs.items():
            try:
                credential_dict[k] = parser[v][k]
            except KeyError:
                raise KeyError("The provided key do not exist in the config file. Please check the placement of the config file or the keys defined")

        return credential_dict