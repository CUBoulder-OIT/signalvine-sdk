import pandas as pd
import pytest
import os, io, csv
from signalvine_sdk.sdk import SignalVineSDK
from signalvine_sdk.common import APIError
from box import Box
import logging

LOGGER = logging.getLogger(__name__)
TEST_GROUP = "Test Contacts"
TEST_NAME_FRED = "Flintstone, Fred"
TEST_EMAIL_FRED = "fred@flintstone.com"
TEST_PHONE_FRED = "+13035550100"


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
    def test_upsert_participants_1(self, sdk_connection):
        """
        - including the signalvine_id at all introduces meyhem; be in the habit
           of removing this column all the time before trying to upsert
        - if the customer_id is included, the record can be changed =>update
        - if the customer_id is not included, but the phone # already exists => error
        - if the customer_id is not included, and a new phone number is given => new record
        - if a new group appears in the semi-colon delimited group list is will be created
        """

        program_id = os.environ.get("PROGRAM_ID")
        assert program_id

        folks = [
            {
                "customer_id": "08c95df1-b233-4221-926c-5d19e61c12b2",
                "active": False,
                "first_name": "Fred",
                "last_name": "Flintstone",
                "group_list": TEST_GROUP,
                "target_group": None,
                "email": TEST_EMAIL_FRED,
                "phone": TEST_PHONE_FRED,
                "full_name": "Flintstone, Fred",
            },
            {
                "customer_id": "61ce1d03-9176-477e-90ab-b037c7613b6c",
                "active": False,
                "first_name": "Wilma",
                "last_name": "Flintstone",
                "group_list": "Test Contacts;People Who Put Up With Fred",
                "target_group": None,
                "email": "wilma@flintstone.com",
                "phone": "+13035550101",
                "full_name": "Flintstone, Wilma",
            },
        ]

        df = pd.DataFrame(folks)

        response = sdk_connection.upsert_participants(
            program_id=program_id, records_df=df, new_flag="add"
        )

        # should be nice and quiet
        assert response == None

    def test_upsert_participants_bad1(self, sdk_connection):

        program_id = os.environ.get("PROGRAM_ID")
        assert program_id

        # There are two issues in this, the first is a made-up new field,
        # the second is a field (honors) that is boolean.
        # The API seems to only catch the first one that happens.
        folks = [
            {
                "customer_id": "08c95df1-b233-4221-926c-5d19e61c12b2",
                "active": False,
                "first_name": "Fred",
                "last_name": "Flintstone",
                "group_list": "Test Contacts",
                "target_group": None,
                "email": TEST_EMAIL_FRED,
                "phone": TEST_PHONE_FRED,
                "full_name": TEST_NAME_FRED,
                "can_ride_tyrannosaurus": True,
                "honors": "sure why not",
            },
        ]

        df = pd.DataFrame(folks)

        response = sdk_connection.upsert_participants(
            program_id=program_id, records_df=df, new_flag="add"
        )

        assert "Unrecognized variable: can_ride_tyrannosaurus" in response

    def test_upsert_participants_bad2(self, sdk_connection):

        program_id = os.environ.get("PROGRAM_ID")
        assert program_id

        # honors field should be boolean but isn't here
        folks = [
            {
                "customer_id": "08c95df1-b233-4221-926c-5d19e61c12b2",
                "active": False,
                "first_name": "Fred",
                "last_name": "Flintstone",
                "group_list": TEST_GROUP,
                "target_group": None,
                "email": TEST_EMAIL_FRED,
                "phone": "+13035550100",
                "full_name": TEST_NAME_FRED,
                "honors": "sure why not",
            },
        ]

        df = pd.DataFrame(folks)

        response = sdk_connection.upsert_participants(
            program_id=program_id, records_df=df, new_flag="add"
        )

        assert "Could not parse 'sure why not' as Maybe (Boolean)" in response

    def test_upsert_participants_2(self, sdk_connection):
        """
        Using data from Slate, which makes this tricky to check in.
        """

        program_id = os.environ.get("PROGRAM_ID")
        assert program_id

        df = pd.read_csv("tests/.private/FY-20210121.csv", dtype=str)
        # remove duplicates across the whole set
        df = df.drop_duplicates(subset=["phone"], keep=False)
        df = df.fillna("")

        info_list = df.to_dict("records")

        status_dict = {}
        for row in info_list:

            LOGGER.info(f"Upserting for {row['customer_id']}")
            # create a DF from just the one row
            indiv_df = pd.DataFrame(row, dtype=str, index=[0])

            # Unlike some of these we're not looking for status codes...
            # The upsert_participants either returned a 202 or threw an error.
            # If it sent 202, then location_path should be useful.
            location_path = sdk_connection.upsert_participants(
                program_id=program_id, records_df=indiv_df, new_flag="add"
            )

            # Do this in a loop?
            status_complete, status_msg = sdk_connection.get_location_status(
                location_path
            )

            if status_complete:
                status_dict[row["customer_id"]] = "Complete"
                LOGGER.info(f"Complete with no message.")
            else:
                # TODO orchestrate a decent waiting test on this
                LOGGER.info(f"Not complete...{status_msg}.")
                LOGGER.info(row)
                status_dict[row["customer_id"]] = "Not Complete"

        LOGGER.info(status_dict)