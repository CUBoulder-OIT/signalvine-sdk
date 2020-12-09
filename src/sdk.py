import requests

from common import APIError, build_headers
from typing import List, Dict


class SignalVineSDK:
    def __init__(
        self,
        account_number: str,
        account_token: str,
        account_secret: str,
        api_hostname: str = "https://theseus-api.signalvine.com",
    ):

        self.account_number = account_number
        self.account_token = account_token
        self.account_secret = account_secret
        self.api_hostname = api_hostname

    def get_programs(self, include_active: bool = None) -> List:
        """
        Get the program info for a specific account.
        """
        participant_path = f"/v1/accounts/{self.account_number}/programs"

        if include_active:
            url = f"{self.api_hostname}{participant_path}?active=all"
        else:
            url = f"{self.api_hostname}{participant_path}"

        headers = build_headers(
            self.account_token, self.account_secret, "GET", participant_path
        )

        r = requests.get(url, headers=headers)

        if r.status_code == 200:
            return r.json()["items"]
        else:
            raise APIError(r.status_code, f"API reason: {r.reason}")
