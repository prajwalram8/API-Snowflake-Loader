# api/api_handler.py
import requests
from requests.exceptions import HTTPError
from utils.logger import setup_logging

logger = setup_logging(__name__)

class APIHandler:
    def __init__(self, base_url, auth=None):
        """
        Initialize the API Handler with a base URL and optional authentication details.
        
        :param base_url: str - The base URL for the API.
        :param auth: dict or tuple - A dictionary or tuple containing authentication details (optional).
        """
        self.base_url = base_url
        self.auth = auth
        self.session = requests.Session()

    def make_request(self, endpoint, method='GET', params=None, data=None, headers=None):
        """
        Make an API request to the specified endpoint using the given HTTP method.
        
        :param endpoint: str - The API endpoint to call.
        :param method: str - The HTTP method (e.g., 'GET', 'POST', 'PUT', 'DELETE').
        :param params: dict - Query parameters for the API call.
        :param data: dict - Data to be sent in the body of the request (for POST/PUT).
        :param headers: dict - HTTP headers to send with the request.
        :return: dict - The parsed JSON response from the API, or None if an error occurred.
        """
        url = f"{self.base_url}/{endpoint}"
        try:
            response = self.session.request(method=method, url=url, headers=headers, params=params, json=data, auth=self.auth)
            response.raise_for_status()
            return response.json()
        except HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
            return None
        except Exception as err:
            logger.error(f"An error occurred: {err}")
            return None

    def call_api_with_pagination(self, endpoint, method, params, payload_template, break_condition):
        """
        Calls the API and paginates through the results until a break condition is met.

        :param endpoint: str - The endpoint to make the request to.
        :param method: str - The HTTP method to use.
        :param params: dict - The parameters to include with the request.
        :param payload_template: callable - A function that returns the payload for the request.
        :param break_condition: callable - A function that accepts the response and returns True if the loop should be broken.
        :return: list - A list of all the collected responses.
        """
        responses = []
        page = 1

        while True:
            payload = payload_template(page)
            response = self.make_request(endpoint, method=method, params=params, data=payload)
            if response is None or break_condition(response):
                break
            responses.append(response)
            page += 1

        return responses

# Example usage
# def create_payload(page):
#     return {
#         "getArticles": {
#             "articleCountry": "AE",
#             "provider": "22610",
#             "searchQuery": "",
#             "searchType": 1,
#             "lang": "en",
#             "perPage": 100,
#             "page": page,
#             "includeAll": 'false',
#             "imcludeImages": 'false',
#             "includeGenericArticles": 'true',
#             "includeOEMNumbers": 'false'
#         }
#     }

# def break_condition(response):
#     # Define your custom break condition
#     # For example, break if no more articles are returned
#     return not response['getArticles']['articles']

# Initialize the API handler with the base URL and any required authentication
# api_key = 'your_api_key'
# base_url = 'https://webservice.tecalliance.services/pegasus-3-0/services/TecdocToCatDLB.jsonEndpoint'
# api_handler = APIHandler(base_url, auth={'api_key': api_key})

# # Make paginated API calls with the search endpoint and POST method
# responses = api_handler.call_api_with_pagination(
#     endpoint='searchQuery',
#     method='POST',
#     params={'api_key': api_key},
#     payload_template=create_payload,
#     break_condition=break_condition
# )
