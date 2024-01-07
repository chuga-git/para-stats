import requests
import logging
from typing import List
from json import JSONDecodeError
from .exceptions import RateLimitError, RoundNotFoundError
# https://api.paradisestation.org/stats/


class SessionAdapter:
    """
    Adapter for Session connection pooling
    """

    def __init__(self, base_url: str = "https://api.paradisestation.org", logger: logging.Logger = None) -> None:
    
        #logging = logger or logging.getLogger(__name__)
        self.base_url = base_url
        self._session = requests.Session()
        
        # TODO: implement cached request counting for automated rate limiting
        self._rate_limit_min = 500
        self._rate_limit_hour_max = 3600
        self._rate_limit_hour_remaining = 0

    def get(self, endpoint: str) -> List:
        """Returns deserialized json response from endpoint"""
        full_url = self.base_url + endpoint


        if self._rate_limit_hour_remaining < 200:
            print("WARNING::RATE LIMIT APPROACHING >>", self._rate_limit_hour_remaining)
        # can't get anything useful
        if self._rate_limit_hour_remaining < 50:
            # so we shit ourselves and die instead of giving the database null values
            raise RateLimitError

        try:
            response = self._session.get(full_url)
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            match err.response.status_code:
                case 429:
                    raise RateLimitError
                case 404:
                    raise RoundNotFoundError
                case _:
                    # idk about this man
                    SystemExit(err)

        self._rate_limit_hour_remaining = int(response.headers['X-Rate-Limit-Remaining'])

        try:
            data_json = response.json()
        except (ValueError, TypeError, JSONDecodeError) as err:
            logging.critical("Handled bad JSON with body", err, exc_info=1)
            data_json = None

        print(f"ADAPTER::Successful GET and deserialize of endpoint:\t{endpoint}\t{response.elapsed.total_seconds()} sec")

        return data_json