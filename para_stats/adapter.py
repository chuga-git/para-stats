import requests
import logging
from typing import List
from json import JSONDecodeError
from .exceptions import RateLimitError, RoundNotFoundError


class SessionAdapter:
    """
    Adapter for Session connection pooling
    """

    def __init__(self, base_url: str = "https://api.paradisestation.org/stats") -> None:
        self._log = logging.getLogger(__name__)
        self.base_url = base_url
        self._session = requests.Session()

        # TODO: implement cached request counting for automated rate limiting
        self._rate_limit_min = 500
        self._rate_limit_hour_max = 3600
        self._rate_limit_hour_remaining = 0

    def get(self, endpoint: str) -> List:
        """Returns deserialized json response from endpoint"""
        full_url = self.base_url + endpoint

        response = self._session.get(full_url)
        response.raise_for_status()

        self._rate_limit_hour_remaining = int(response.headers["X-Rate-Limit-Remaining"])

        data_json = response.json()

        self._log.info(f"Successful GET/deserialize of endpoint\t{endpoint}\t{response.elapsed.total_seconds()} sec\tratelimit remaining: {int(response.headers['X-Rate-Limit-Remaining'])}")

        return data_json
