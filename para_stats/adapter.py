import requests
import logging
import time
from typing import List
from json import JSONDecodeError
from .exceptions import RateLimitError, RoundNotFoundError


class SessionAdapter:
    """
    Adapter for Session connection pooling
    """

    def __init__(self, base_url: str = "https://api.paradisestation.org/stats", throttle_time = 6) -> None:
        self._log = logging.getLogger(__name__)
        self.base_url = base_url
        self._session = requests.Session()

        self._rate_limit_min = 500
        self._rate_limit_hour_max = 3600
        self._throttle_time = 1/throttle_time # TODO: make this a config.ini var

    def get(self, endpoint: str) -> List:
        """Returns deserialized json response from endpoint"""
        full_url = self.base_url + endpoint

        response = self._session.get(full_url)
        
        try:
            response.raise_for_status()

        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                self._log.exception("Caught rate limit exception:", e, exc_info=1)
                raise RateLimitError from e
            
            elif response.status_code == 404:
                self._log.critical(f"Received 404 from endpoint {endpoint}")

                return None
            
            else:
                self._log.exception("Unhandled HTTP error occurred:", e, exc_info=1)
                raise e

        try:
            data_json = response.json()

        except (ValueError, TypeError, JSONDecodeError) as e:
            self._log.exception("Error in decoding JSON response:", e, exc_info=1)

            return None

        self._log.info(f"Successful GET/deserialize of endpoint\t{endpoint}\t{response.elapsed.total_seconds()} sec\tratelimit remaining: {int(response.headers['X-Rate-Limit-Remaining'])}")

        return data_json

    def concurrent_get(self, session: requests.Session, endpoint: str):
        """DEBUG GET method for thread pooling testing. FIXME: Uses a passed down session for testing purposes. Should be able to keep this instance's session in theory."""
        full_url = self.base_url + endpoint

        # we're too FAST for exception handling, baby
        response = self._session.get(full_url)
        response.raise_for_status()

        data_json = response.json()

        self._log.info(f"Successful GET/deserialize of endpoint\t{endpoint}\t{response.elapsed.total_seconds()} sec\tratelimit remaining: {int(response.headers['X-Rate-Limit-Remaining'])}")
        time.sleep(self._throttle_time)

        return data_json