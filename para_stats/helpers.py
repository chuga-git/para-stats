import logging
from requests import Response

logger = logging.getLogger(__name__)

def decode_json(response: Response):
    """Helper for safely decoding """
    try:
        data_json = response_json.json()
    except (ValueError, TypeError, JSONDecodeError) as e:
        logger.exception("Error in decoding JSON response:", e, exc_info=1)
        return None