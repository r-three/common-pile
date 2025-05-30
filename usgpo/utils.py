import time
import requests

from licensed_pile import logs


def api_query(endpoint, headers, params):
    logger = logs.get_logger("usgpo")
    response = requests.get(endpoint, headers=headers, params=params)
    if response.status_code == 429:
        # Sleep for an hour if we've hit the rate-limit
        logger.info("Exceeded rate-limit, sleeping for one hour")
        time.sleep(60 * 60)
        response = requests.get(endpoint, headers=headers, params=params)
    return response
