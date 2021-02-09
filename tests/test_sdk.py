from attr import fields
import pandas as pd
import pytest
import os, io, csv
from signalvine_sdk.sdk import SignalVineSDK
from signalvine_sdk.common import APIError
import logging

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def sdk_connection():

    # These need to be set or exported to work
    account_number = os.environ.get("ACCOUNT_NUMBER")
    account_token = os.environ.get("ACCOUNT_TOKEN")
    account_secret = os.environ.get("ACCOUNT_SECRET")
    assert account_secret, "Secrets are not set at the environment."

    # instantiate the API class using environment variables
    return SignalVineSDK(
        account_number=account_number,
        account_token=account_token,
        account_secret=account_secret,
    )


class TestClass:
    def test_get_programs(self, sdk_connection):

        # use the class to get programs info
        items = sdk_connection.get_programs(include_active=True)
        assert isinstance(items, list)

        # get just the keys
        keys = set().union(*(i.keys() for i in items))
        assert sorted(keys) == ["accountId", "active", "id", "name"]

    def test_get_participants_chunk(self, sdk_connection):

        program_id = os.environ.get("PROGRAM_ID")

        items = sdk_connection.get_participants_chunk(
            program_id=program_id, chunk_size=500, offset=0, include_active=True
        )

        assert isinstance(items, list)

        # get just the keys
        keys = set().union(*(i.keys() for i in items))
        assert "customerId" in keys

    def test_get_program_schema(self, sdk_connection):

        program_id = os.environ.get("PROGRAM_ID")

        fields_dict = sdk_connection.get_program_schema(program_id=program_id)

        assert isinstance(fields_dict, dict)
        assert fields_dict["first_name"] == "String"