import pytest
import os
from src.signalvine_sdk.sdk import SignalVineSDK
from src.signalvine_sdk.common import APIError


class TestClass:
    def test_get_programs(self):

        account_number = os.environ.get("ACCOUNT_NUMBER")
        account_token = os.environ.get("ACCOUNT_TOKEN")
        account_secret = os.environ.get("ACCOUNT_SECRET")

        # instantiate the API class using environment variables
        api = SignalVineSDK(
            account_number=account_number,
            account_token=account_token,
            account_secret=account_secret,
        )

        # use the class to get programs info
        items = api.get_programs(include_active=True)
        assert isinstance(items, list)

        # get just the keys
        keys = set().union(*(i.keys() for i in items))
        assert sorted(keys) == ["accountId", "active", "id", "name"]

    def test_get_programs_fail(self):

        # instantiate the API class using purposefully bad values
        api = SignalVineSDK(
            account_number="BOGUS_ACCOUNT",
            account_token="MADE_UP_TOKEN",
            account_secret="FAKE_SECRET",
        )

        # This should fail hard given the right hostname,
        # but bogus connect info
        with pytest.raises(APIError):
            api.get_programs()
