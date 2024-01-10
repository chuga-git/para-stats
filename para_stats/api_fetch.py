import logging
import requests
import time
from json import JSONDecodeError
from concurrent.futures import ThreadPoolExecutor
from .exceptions import RateLimitError, RoundNotFoundError


class APIFetch:
    def __init__(
        self,
        base_url: str = "https://api.paradisestation.org/stats",
        throttle_time: int = 7,
        should_throttle: bool = True,
        max_connections: int = 2,
    ) -> None:
        """REST adapter for connection pooling

        Args:
            base_url (str, optional): Endpoint to query, should not need to change from default. Defaults to "https://api.paradisestation.org/stats".
            throttle_time (int, optional): Rate to throttle requests. To keep under 500/min limit, it should not go above 8 (8.33/sec max). Defaults to 7.
            should_throttle (bool, optional): Whether or not to throttle requests for this instance. Defaults to True.
            max_connections(int, optional): Number of threads to use with concurrent requests.
        """

        self._log = logging.getLogger(__name__)
        self.base_url = base_url

        # TODO: these are useless now but need to be used later for intelligent backing-off
        self._rate_limit_min = 500
        self._rate_limit_hour_max = 3600
        self.CONNECTIONS = 2
        self._throttle_time = 1 / throttle_time  # TODO: make this a config.ini var
        self.should_throttle = should_throttle
        self._session = requests.Session()

    def __fetch_roundlist_paged(self, offset_start: int, offset_end: int) -> list:
        """Generator for paginated retrieval of roundlist endpoint. Will return overlapping data."""

        def fetch_single_page(offset: int) -> list:
            return self._get(f"/roundlist?offset={offset}")

        result = fetch_single_page(offset_start)
        yield result

        while result[-1]["round_id"] > offset_end:
            result = fetch_single_page(result[-1]["round_id"])
            yield result

    def fetch_roundlist_to_offset(self, offset_end: int) -> list:
        """Fetches list of rounds up to a specified offset"""
        metadata_list = []

        for result in self.__fetch_roundlist_paged(0, offset_end):
            # list concatenation appends each page to the master list
            metadata_list += result

        return metadata_list
    
    def fetch_all_metadata(self) -> list:
        """Hacky debug method to scrape all round metadata"""
        metadata_list = []
        
        result = self._get(f"/roundlist?offset={0}")
        metadata_list += result

        # response.json() != []
        while result != []:
            last_id = result[-1]["round_id"]
            result = self._get(f"/roundlist?offset={last_id}")
            metadata_list += result

        return metadata_list

    def fetch_most_recent_round_id(self) -> int:
        """Gets the most recently completed round from the API"""
        return self._get("/roundlist?offset=0")[0]["round_id"]

    def fetch_round_data_bulk(self, round_id_list: list[int]) -> tuple:
        """Fetches playercount and blackbox data from provided round id list.

        Args:
            round_id_list (list[int]): list of round ids to query

        Returns:
            tuple: playercount_list and raw_blackbox_list responses
        """
        self._log.info("Starting concurrent get...")

        # FIXME: this tuple unpack is SO unsafe and is going to lead to misery
        playercount_list, raw_blackbox_list = self.__fetch_endpoints(
            round_id_list,
            ['/playercount/', '/blackbox/']
        )

        self._log.info(
            f"Successfully got playercount and blackbox lists with lens: {len(playercount_list)}, {len(raw_blackbox_list)}"
        )

        return playercount_list, raw_blackbox_list

    def __fetch_endpoints(
        self, round_id_list: list[int], endpoint_list: list[str]
    ) -> list[list]:
        """Builds urls from provided round_id_list and then concurrently fetches endpoints.

        Args:
            round_id_list (list[int]): _description_
            endpoint_list (list[str]): list of endpoint strings. ex: `['/abc/, '/def/']`

        Returns:
            list[list]: list of responses in the order provided in `endpoint_list`. 
        """

        # build a list of endpoints/id from each base endpoint
        full_endpoint_list = [
            [endpoint + str(rnd_id) for rnd_id in round_id_list]
            for endpoint in endpoint_list
        ]

        # iteratively map the iterable of iterables
        # this is such a bad idea in both conception and execution
        with ThreadPoolExecutor(max_workers=self.CONNECTIONS) as pool:
            responses = [
                list(pool.map(self._get, endpoint_iter))
                for endpoint_iter in full_endpoint_list
            ]

        return responses

    def _get(self, endpoint: str) -> dict | list:
        """
        Session HTTP GET wrapper. Returns json response.
        Args:
            session (requests.Session): Session object for keep-alive connection pooling. Make sure this is defined at the highest level possible in the case of iterative functions.
            endpoint (str): full endpoint (+ round id) to query.
                ex: /metadata/12345

        Raises:
            RateLimitError: HTTP status code 429: per-minute or per-hour rate limit has been exceeded and will be locked out. TODO: return the hour reset time on this exception
            RoundNotFoundError: Either the URL queried was incorrect or the round is still ongoing.

        Returns:
            dict | list: deserialized json response
        """

        full_url = self.base_url + endpoint

        response = self._session.get(full_url)

        try:
            response.raise_for_status()
        # TODO: PLEASE!!!!! learn how exceptions work!!!
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                self._log.exception("Caught rate limit exception:", e, exc_info=1)
                raise RateLimitError from e
            elif response.status_code == 404:
                self._log.critical(f"Received 404 from endpoint {endpoint}")
                raise RoundNotFoundError from e
            else:
                self._log.exception("Unhandled HTTP error occurred:", e, exc_info=1)
                raise requests.exceptions.HTTPError from e

        try:
            data_json = response.json()
        except (ValueError, TypeError, JSONDecodeError) as e:
            self._log.exception("Error in decoding JSON response:", e, exc_info=1)
            data_json = None

        # TODO: clean this formatting up it's gross and ugly
        self._log.info(
            f"Successful GET/deserialize of endpoint\t{endpoint}\t{response.elapsed.total_seconds()} sec\tratelimit remaining: {int(response.headers['X-Rate-Limit-Remaining'])}"
        )

        # this kinda sucks
        if self.should_throttle:
            time.sleep(self._throttle_time)

        return data_json
