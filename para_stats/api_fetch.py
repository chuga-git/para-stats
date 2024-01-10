import logging
import requests
import time
from functools import partial
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

    def ___fetch_all_metadata(self) -> list:
        metadata_list = []

        with requests.Session() as session:
            for result in self.__page_all_metadata(session):
                metadata_list += result

        return metadata_list

    def fetch_roundlist_batch(self, offset_start: int, offset_end: int) -> list:
        rounds_list = []

        for result in self.__fetch_roundlist_paged(offset_start, offset_end):
            rounds_list += result

        return rounds_list

    def fetch_blackbox(self, round_id: int) -> list:
        """Fetch blackbox stats for a single round. `raw_data` is processed on hand."""
        result = self._get(f"/blackbox/{round_id}")

        return result

    def fetch_playercounts(self, round_id: int) -> dict[str, int]:
        """Returns playercount timestamps for `round_id`"""
        result = self._get(f"/playercounts/{round_id}")

        return result

    def fetch_metadata(self, round_id: int) -> dict:
        """Returns metadata stats for `round_id`. This returns the same information as the roundlist endpoint."""
        result = self._get(f"/metadata/{round_id}")

        return result

    def fetch_single_round(self, round_id: int) -> dict:
        """Debug method, needs refactoring in the future."""
        round_metadata = self.fetch_metadata(round_id)
        round_blackbox = self.fetch_blackbox(round_id)
        round_playercounts = self.fetch_playercounts(round_id)

        round_metadata["playercounts"] = round_playercounts
        round_metadata["stats"] = round_blackbox

        return [round_metadata]

    def fetch_whole_round_batch(self, offset_start: int, offset_end: int) -> tuple:
        """Get all queryable information from the most recent to the target `offset_end` round."""

        self._log.info("Starting whole round batch fetch")

        # grab the rounds we'll need to query and then get the rest of their info after
        round_metadata_list = self.fetch_roundlist_batch(offset_start, offset_end)
        valid_round_ids = [r["round_id"] for r in round_metadata_list]
        self._log.info(
            f"Metadata retrieved successfully with length {len(round_metadata_list)}"
        )

        blackbox_list = [self.fetch_blackbox(r) for r in valid_round_ids]
        self._log.info(
            f"Blackbox data retrieved successfully with length {len(blackbox_list)}"
        )

        playercount_list = [self.fetch_playercounts(r) for r in valid_round_ids]
        self._log.info(
            f"Playercount data retrieved successfully with length {len(playercount_list)}"
        )

        return (round_metadata_list, playercount_list, blackbox_list)

    def get_most_recent_round_id(self):
        return self._get("/roundlist?offset=0")[0]["round_id"]

    def concurrent_fetch_rounds(self, round_id_list: list[int]) -> tuple:
        self._log.info("Starting concurrent get...")
        # TODO: pretty sure a generator expression is going to shit itself if we try to multithread it - have to test
        round_metadata_list = self.fetch_roundlist_batch(offset_start, offset_end)

        self._log.info(f"Got metadata list of len {len(round_metadata_list)}")

        round_id_list = [r["round_id"] for r in round_metadata_list]
        playercount_list, raw_blackbox_list = self.__concurrent_fetch_list(
            round_id_list
        )

        self._log.info(
            f"Successfully got playercount and blackbox lists with lens: {len(playercount_list)}, {len(raw_blackbox_list)}"
        )

        return (round_metadata_list, playercount_list, raw_blackbox_list)

    def __concurrent_fetch_list(self, round_id_list):
        playercount_endpoints = ["/playercounts/" + str(i) for i in round_id_list]
        blackbox_endpoints = ["/blackbox/" + str(i) for i in round_id_list]

        self._log.info(
            f"Starting concurrent session pool with endpoint lists of lens: {len(playercount_endpoints)}, {len(blackbox_endpoints)}"
        )

        with ThreadPoolExecutor(max_workers=self.CONNECTIONS) as pool:
            playercount_list = list(
                pool.map(
                    partial(self._get),
                    playercount_endpoints,
                )
            )
            raw_blackbox_list = list(
                pool.map(
                    partial(self._get),
                    blackbox_endpoints,
                )
            )

        return playercount_list, raw_blackbox_list


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
