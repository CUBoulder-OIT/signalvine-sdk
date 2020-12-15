import pandas as pd
import pytest
import os, io, csv
from signalvine_sdk.sdk import SignalVineSDK
from signalvine_sdk.common import APIError
import logging

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def sdk_connection():

    # It's expensive, but in this case noisy to keep
    # repeating this for every function.

    # These need to be set or exported to work
    account_number = os.environ.get("ACCOUNT_NUMBER")
    account_token = os.environ.get("ACCOUNT_TOKEN")
    account_secret = os.environ.get("ACCOUNT_SECRET")

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

    def test_get_participants(self, sdk_connection):

        program_id = os.environ.get("PROGRAM_ID")

        items = sdk_connection.get_participants(
            program_id=program_id, chunk_size=500, include_active=True
        )
        assert isinstance(items, list)

        # get just the keys
        keys = set().union(*(i.keys() for i in items))
        # The 'cooked' version doesn't have customerId, just the
        # profile field 'customer_id'
        assert "customer_id" in keys
        assert "customerId" not in keys

    def test_upsert_participants(self, sdk_connection):

        program_id = os.environ.get("PROGRAM_ID")
        assert program_id

        example_file = os.path.join(os.path.dirname(__file__), "Example-Insert1.csv")
        assert os.path.exists(example_file), "An example CSV file cannot be found."

        items_df = pd.read_csv(
            example_file,
            quoting=csv.QUOTE_MINIMAL,
            dtype=str,
            encoding="unicode_escape",
        )

        records = sdk_connection.upsert_participants(
            program_id=program_id, records_df=items_df, mode="add"
        )

        # TODO
        LOGGER.info(records)