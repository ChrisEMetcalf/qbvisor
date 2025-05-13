import os
import time
import random
import requests
from typing import Callable, Optional, Any

from .log_runner import get_logger

logger = get_logger(__name__)

class QuickBaseTransport:
    """
    Handles all HTTP interactions with the QuickBase API, including retries and exponential backoff.
    """
    def __init__(self):
        # Load auth from environment
        self.realm_hostname = os.getenv('QB_REALM_HOSTNAME')
        self.auth_token = os.getenv('QB_REALM_API_KEY')
        if not self.realm_hostname or not self.auth_token:
            raise EnvironmentError(
                "Both QB_REALM_HOSTNAME and QB_REALM_API_KEY must be set in the environment variables."
            )
        self.base_url = 'https://api.quickbase.com/v1'
        self.headers = {
            'QB-Realm-Hostname': self.realm_hostname,
            'Authorization': f'{self.auth_token}',
            'Content-Type': 'application/json',
            'User-Agent': 'QuickBase Archiver'
        }

    def _make_request(
            self, 
            method: Callable[..., requests.Response], 
            path: str, 
            params: Optional[dict] = None, 
            json_body: Optional[Any] = None
    ) -> dict:
        url = f'{self.base_url}/{path.lstrip('/')}'
        max_attempts = 5
        delay = 1.0  # Start with a 1 second delay
        max_delay = 64.0  # Cap the delay at 64 seconds

        for attempt in range(max_attempts):
            try:
                resp = method(url, headers=self.headers, params=params, json=json_body)
                resp.raise_for_status()  # Raise an error for bad responses
                return resp.json()
            except requests.RequestException as e:
                if attempt == max_attempts - 1:
                    logger.error(f'Requests to {path} failed after {max_attempts} attempts: {e}')                    
                    raise
                backoff = min(max_delay, delay * (2 ** attempt))
                wait = backoff * random.uniform(0.5, 1.5)  # Add some jitter to the wait time
                logger.warning(f'Retrying {path} (#{attempt + 1}) in {wait:.1f}s')
                time.sleep(wait)

    def get(
            self, 
            path: str, 
            params: Optional[dict] = None, 
            json_body: Optional[Any] = None
    ) -> dict:
        return self._make_request(requests.get, path, params=params, json_body=json_body)
    
    def post(
            self, 
            path: str, 
            params: Optional[dict] = None, 
            json_body: Optional[Any] = None
    ) -> dict:
        return self._make_request(requests.post, path, params=params, json_body=json_body)
    
    def delete(
            self, 
            path: str, 
            params: Optional[dict] = None, 
            json_body: Optional[Any] = None
    ) -> dict:
        return self._make_request(requests.delete, path, params=params, json_body=json_body)

