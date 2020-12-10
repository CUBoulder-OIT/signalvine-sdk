import hmac
import hashlib
import base64
from typing import Dict
from datetime import datetime, timezone


class Error(Exception):
    """Base class for other exceptions"""

    pass


class APIError(Error):
    """Exception raised in the API call.

    Attributes:
        status_code -- HTTP status code
        message -- text message of the error
    """

    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message


def build_headers(
    token: str, secret: str, action: str, path_no_query: str, body: str = ""
) -> Dict:
    """
    Build out the headers for SignalVine

    Returns a dictionary of headers including authorization
    """

    now_date = datetime.now(timezone.utc).isoformat()

    auth_string = sign_request(token, secret, now_date, action, path_no_query)

    auth_header = f"SignalVine {token}:{auth_string}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": auth_header,
        "SignalVine-Date": now_date,
    }

    return headers


def sign_request(
    token: str,
    secret: str,
    timestamp: str,
    action: str,
    path_no_query: str,
    body: str = "",
) -> str:
    """
    Sign the request and return a bearer token for the SignalVine API

    Return a string with the encrypted request string
    """
    string_to_sign = f"{token}\n{action}\n{path_no_query}\n{body}\n{timestamp}".lower()

    digest = hmac.new(
        secret.encode(), msg=string_to_sign.encode(), digestmod=hashlib.sha256
    ).digest()
    signature = base64.b64encode(digest).decode()

    return signature
