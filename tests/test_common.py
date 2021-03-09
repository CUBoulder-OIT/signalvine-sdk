import logging
import csv
from datetime import datetime, timezone
import io
from attr import s

import pandas as pd
from signalvine_sdk.common import (
    build_headers,
    convert_participant_to_record,
    convert_participants_to_records,
    convert_sv_types,
    make_body,
    sign_request,
)

LOGGER = logging.getLogger(__name__)


class TestClass:
    def test_sign_request(self):

        test_date = datetime(1969, 7, 20, 20, 17, 0, 0, timezone.utc).isoformat()
        assert test_date == "1969-07-20T20:17:00+00:00"

        result = sign_request(
            token="INVENTED_TOKEN",
            secret="INVENTED_SECRET",
            timestamp=test_date,
            action="GET",
            path_no_query="/bogus",
        )

        assert result == "P1f0up2G0I6tJG4D3nRed/IlvvT2tqEQqbqEPXNQXDo="

    def test_build_headers(self):

        result = build_headers(
            token="INVENTED_TOKEN",
            secret="INVENTED_TOKEN",
            path_no_query="/bogus",
            action="GET",
        )

        # The cryptostring changes with the date from sign_request
        assert result["Authorization"].startswith("SignalVine INVENTED_TOKEN:")

        # The content-type is json
        assert result["Content-Type"] == "application/json"

        # A date should exist
        assert result["SignalVine-Date"]

    def test_convert_participant_to_record(self, include_agg=True):

        item = {
            "id": "0000-000-000-000-0000",
            "customerId": "123456789",
            "accountId": "1234-123-123-123-1234",
            "programId": "2345-234-234-234-2345",
            "programName": "Admissions-CU-Boulder",
            "programActive": True,
            "phone": "+5555555555",
            "programPhone": "+15555555556",
            "created": "2020-11-11T16:43:50.795Z",
            "active": False,
            "groups": ["Do Not Contact", "New Participants"],
            "profile": [
                {
                    "name": "first_name",
                    "type": "String",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": -2,
                    "value": "Steve",
                },
                {
                    "name": "last_name",
                    "type": "String",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": -1,
                    "value": "Taylor",
                },
                {
                    "name": "email",
                    "type": "String",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 28,
                    "value": "stta9820@colorado.edu",
                },
                {
                    "name": "act",
                    "type": "Maybe (Numeric)",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 2,
                    "value": "",
                },
                {
                    "name": "active",
                    "type": "Boolean",
                    "updated": "2020-11-11T16:43:50.579Z",
                    "sort": 0,
                    "value": "false",
                },
                {
                    "name": "aspgpa",
                    "type": "Maybe (Float)",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 6,
                    "value": "",
                },
                {
                    "name": "bin",
                    "type": "String",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 8,
                    "value": "",
                },
                {
                    "name": "citizenship",
                    "type": "String",
                    "updated": "2020-10-28T17:01:23.491Z",
                    "sort": 12,
                    "value": "",
                },
                {
                    "name": "city",
                    "type": "String",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 13,
                    "value": "",
                },
                {
                    "name": "college",
                    "type": "String",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 16,
                    "value": "",
                },
                {
                    "name": "country",
                    "type": "String",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 17,
                    "value": "",
                },
                {
                    "name": "county",
                    "type": "String",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 18,
                    "value": "",
                },
                {
                    "name": "created_at",
                    "type": "DateTime",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 0,
                    "value": "2020-10-28T14:36:57.000Z",
                },
                {
                    "name": "customer_id",
                    "type": "String",
                    "updated": "2020-10-28T16:58:12.978Z",
                    "sort": 19,
                    "value": "",
                },
                {
                    "name": "decision",
                    "type": "String",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 22,
                    "value": "",
                },
                {
                    "name": "decision_date",
                    "type": "Maybe (Date)",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 24,
                    "value": "",
                },
                {
                    "name": "decision_detail",
                    "type": "String",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 23,
                    "value": "",
                },
                {
                    "name": "entry_term",
                    "type": "String",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 32,
                    "value": "",
                },
                {
                    "name": "fafsa",
                    "type": "Maybe (Boolean)",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 33,
                    "value": "",
                },
                {
                    "name": "first_gen",
                    "type": "Maybe (Boolean)",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 34,
                    "value": "",
                },
                {
                    "name": "goldshirt",
                    "type": "Maybe (Boolean)",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 36,
                    "value": "",
                },
                {
                    "name": "group_list",
                    "type": "List",
                    "updated": "2020-10-28T17:04:55.121Z",
                    "sort": 37,
                    "value": "Do Not Contact;New Participants",
                },
                {
                    "name": "hispanic",
                    "type": "Maybe (Boolean)",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 39,
                    "value": "",
                },
                {
                    "name": "honors",
                    "type": "Maybe (Boolean)",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 40,
                    "value": "",
                },
                {
                    "name": "major",
                    "type": "String",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 3,
                    "value": "",
                },
                {
                    "name": "mcneill",
                    "type": "Maybe (Boolean)",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 47,
                    "value": "",
                },
                {
                    "name": "missing",
                    "type": "String",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 48,
                    "value": "",
                },
                {
                    "name": "oda_reporting_seq",
                    "type": "String",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 49,
                    "value": "",
                },
                {
                    "name": "perm_resident",
                    "type": "Maybe (Boolean)",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 52,
                    "value": "",
                },
                {
                    "name": "pgpa2",
                    "type": "Maybe (Float)",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 53,
                    "value": "",
                },
                {
                    "name": "phone",
                    "type": "Phone",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 0,
                    "value": "+15555555555",
                },
                {
                    "name": "phone_valid",
                    "type": "Boolean",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 54,
                    "value": "true",
                },
                {
                    "name": "piano",
                    "type": "Maybe (Boolean)",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 55,
                    "value": "",
                },
                {
                    "name": "program_phone",
                    "type": "Phone",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 0,
                    "value": "+15555555556",
                },
                {
                    "name": "ref",
                    "type": "String",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 70,
                    "value": "",
                },
                {
                    "name": "related_id",
                    "type": "UUID",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 0,
                    "value": "00000000-0000-0000-0000-000000000000",
                },
                {
                    "name": "residency",
                    "type": "String",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 77,
                    "value": "",
                },
                {
                    "name": "sat",
                    "type": "Maybe (Numeric)",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 58,
                    "value": "",
                },
                {
                    "name": "school_country",
                    "type": "String",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 61,
                    "value": "",
                },
                {
                    "name": "school_county",
                    "type": "String",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 62,
                    "value": "",
                },
                {
                    "name": "school_name",
                    "type": "String",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 59,
                    "value": "",
                },
                {
                    "name": "school_state",
                    "type": "String",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 64,
                    "value": "",
                },
                {
                    "name": "secondary_education",
                    "type": "Maybe (Boolean)",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 65,
                    "value": "",
                },
                {
                    "name": "sex",
                    "type": "String",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 68,
                    "value": "",
                },
                {
                    "name": "signalvine_id",
                    "type": "UUID",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 69,
                    "value": "0000-000-000-000-0000",
                },
                {
                    "name": "state",
                    "type": "String",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 71,
                    "value": "",
                },
                {
                    "name": "student_type",
                    "type": "String",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 72,
                    "value": "",
                },
                {
                    "name": "target_group",
                    "type": "String",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 73,
                    "value": "",
                },
                {
                    "name": "timezone",
                    "type": "Timezone",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 74,
                    "value": "US/Mountain",
                },
                {
                    "name": "veteran",
                    "type": "Maybe (Boolean)",
                    "updated": "2020-10-28T14:36:57.831Z",
                    "sort": 78,
                    "value": "false",
                },
            ],
            "receivedCount": 0,
            "sentCount": 0,
            "scheduledCount": 0,
        }

        record = convert_participant_to_record(item, include_agg=True)

        assert record == {
            "first_name": "Steve",
            "last_name": "Taylor",
            "email": "stta9820@colorado.edu",
            "act": None,
            "active": "false",
            "aspgpa": None,
            "bin": None,
            "citizenship": None,
            "city": None,
            "college": None,
            "country": None,
            "county": None,
            "created_at": "2020-10-28T14:36:57.000Z",
            "customer_id": None,
            "decision": None,
            "decision_date": None,
            "decision_detail": None,
            "entry_term": None,
            "fafsa": None,
            "first_gen": None,
            "goldshirt": None,
            "group_list": "Do Not Contact;New Participants",
            "hispanic": None,
            "honors": None,
            "major": None,
            "mcneill": None,
            "missing": None,
            "oda_reporting_seq": None,
            "perm_resident": None,
            "pgpa2": None,
            "phone": "+15555555555",
            "phone_valid": "true",
            "piano": None,
            "program_phone": "+15555555556",
            "ref": None,
            "related_id": "00000000-0000-0000-0000-000000000000",
            "residency": None,
            "sat": None,
            "school_country": None,
            "school_county": None,
            "school_name": None,
            "school_state": None,
            "secondary_education": None,
            "sex": None,
            "signalvine_id": "0000-000-000-000-0000",
            "state": None,
            "student_type": None,
            "target_group": None,
            "timezone": "US/Mountain",
            "veteran": "false",
            "agg_receivedCount": 0,
            "agg_scheduledCount": 0,
            "agg_sentCount": 0,
        }

    def test_convert_participants_to_records(self):

        items = [
            {
                "id": "0000-000-000-000-0000",
                "customerId": "123456789",
                "accountId": "1234-123-123-123-1234",
                "programId": "2345-234-234-234-2345",
                "programName": "Admissions-CU-Boulder",
                "programActive": True,
                "phone": "+5555555555",
                "programPhone": "+15555555556",
                "created": "2020-11-11T16:43:50.795Z",
                "active": False,
                "groups": ["Do Not Contact", "New Participants"],
                "profile": [
                    {
                        "name": "first_name",
                        "type": "String",
                        "updated": "2020-10-28T14:36:57.831Z",
                        "sort": -2,
                        "value": "Steve",
                    },
                    {
                        "name": "last_name",
                        "type": "String",
                        "updated": "2020-10-28T14:36:57.831Z",
                        "sort": -1,
                        "value": "Taylor",
                    },
                    {
                        "name": "email",
                        "type": "String",
                        "updated": "2020-10-28T14:36:57.831Z",
                        "sort": 28,
                        "value": "stta9820@colorado.edu",
                    },
                    {
                        "name": "act",
                        "type": "Maybe (Numeric)",
                        "updated": "2020-10-28T14:36:57.831Z",
                        "sort": 2,
                        "value": "",
                    },
                    {
                        "name": "active",
                        "type": "Boolean",
                        "updated": "2020-11-11T16:43:50.579Z",
                        "sort": 0,
                        "value": "false",
                    },
                ],
                "receivedCount": 0,
                "sentCount": 0,
                "scheduledCount": 0,
            }
        ]

        # including the agg info is optional, and defaults to False
        records = convert_participants_to_records(items)

        assert records == [
            {
                "first_name": "Steve",
                "last_name": "Taylor",
                "email": "stta9820@colorado.edu",
                "act": None,
                "active": "false",
            }
        ]

    def test_make_body(self):

        items = """
"group_list","target_group","opt_in","phone","email","first_name","customer_id","ref","full_name","last_name"
"Arts and Sciences","","True","+1 555-555-5555","stta9820@colorado.edu","Steve","1234-123-123-1234","123456780","Taylor, Steve","Taylor"
        """

        f = io.StringIO(items)

        items_df = pd.read_csv(
            f, quoting=csv.QUOTE_MINIMAL, dtype=str, encoding="unicode_escape",
        )

        records = make_body("1234-123-123-1234", items_df, "add")

        assert records == {
            "program": "1234-123-123-1234",
            "options": {
                "new": "add",
                "mode": "tx",
                "existing": [
                    "group_list",
                    "target_group",
                    "opt_in",
                    "phone",
                    "email",
                    "first_name",
                    "customer_id",
                    "ref",
                    "full_name",
                    "last_name",
                ],
                "absent": "ignore",
            },
            "participants": 'group_list,target_group,opt_in,phone,email,first_name,customer_id,ref,full_name,last_name\nArts and Sciences,,True,+1 555-555-5555,stta9820@colorado.edu,Steve,1234-123-123-1234,123456780,"Taylor, Steve",Taylor\n',
        }

    def test_convert_sv_types(self):

        sv_types = {
            "citizenship": "String",
            "mcneill": "Maybe (Boolean)",
            "goldshirt": "Maybe (Boolean)",
            "email": "String",
            "aspgpa": "Maybe (Float)",
            "veteran": "Maybe (Boolean)",
            "honors": "Maybe (Boolean)",
            "regent": "Maybe (Boolean)",
            "secondary_education": "Maybe (Boolean)",
            "state": "String",
            "county": "String",
            "school_ceeb": "String",
            "engineering_honor": "String",
            "first_name": "String",
            "bin": "String",
            "ah_oos": "Maybe (Boolean)",
            "country": "String",
            "dean_leadership": "Maybe (Boolean)",
            "hale": "Maybe (Boolean)",
            "engineering_merit_1year": "String",
            "sewall": "Maybe (Boolean)",
            "college": "String",
            "full_name": "String",
            "zip": "String",
            "legal_name": "String",
            "oda_reporting_seq": "String",
            "cmci4k": "Maybe (Boolean)",
            "perm_resident": "Maybe (Boolean)",
            "leeds_silvergold": "Maybe (Boolean)",
            "major": "String",
            "decision_detail": "String",
            "leeds_scholar_admit": "Maybe (Boolean)",
            "act": "Maybe (Numeric)",
            "sat": "Maybe (Numeric)",
            "piano": "Maybe (Boolean)",
            "school_county": "String",
            "school_state": "String",
            "transfer_instate": "Maybe (Boolean)",
            "sewell": "Maybe (Boolean)",
            "pgpa2": "Maybe (Float)",
            "school_name": "String",
            "missing": "String",
            "baker": "Maybe (Boolean)",
            "chancellor": "Maybe (Boolean)",
            "city": "String",
            "hispanic": "Maybe (Boolean)",
            "transfer_oos": "Maybe (Boolean)",
            "leeds_scholar_app": "Maybe (Boolean)",
            "last_name": "String",
            "school_geomarket": "String",
            "impact": "Maybe (Boolean)",
            "presidential": "Maybe (Boolean)",
            "entry_term": "String",
            "decision": "String",
            "ss": "Maybe (Boolean)",
            "zip_plusfour": "String",
            "residency": "String",
            "fafsa": "Maybe (Boolean)",
            "leeds_honors": "Maybe (Boolean)",
            "school_country": "String",
            "decision_date": "Maybe (Date)",
            "ah_instate": "Maybe (Boolean)",
            "sex": "String",
            "denver": "Maybe (Boolean)",
            "student_type": "String",
            "cmci1k": "Maybe (Boolean)",
            "dean": "Maybe (Boolean)",
            "target_group": "String",
            "engineering_merit": "String",
            "first_gen": "Maybe (Boolean)",
            "ref": "String",
            "birthdate": "Maybe (Date)",
        }

        new_types = convert_sv_types(sv_types)

        assert new_types == {
            "citizenship": {"type": "str", "required": True},
            "mcneill": {"type": "bool", "required": False},
            "goldshirt": {"type": "bool", "required": False},
            "email": {"type": "str", "required": True},
            "aspgpa": {"type": "float", "required": False},
            "veteran": {"type": "bool", "required": False},
            "honors": {"type": "bool", "required": False},
            "regent": {"type": "bool", "required": False},
            "secondary_education": {"type": "bool", "required": False},
            "state": {"type": "str", "required": True},
            "county": {"type": "str", "required": True},
            "school_ceeb": {"type": "str", "required": True},
            "engineering_honor": {"type": "str", "required": True},
            "first_name": {"type": "str", "required": True},
            "bin": {"type": "str", "required": True},
            "ah_oos": {"type": "bool", "required": False},
            "country": {"type": "str", "required": True},
            "dean_leadership": {"type": "bool", "required": False},
            "hale": {"type": "bool", "required": False},
            "engineering_merit_1year": {"type": "str", "required": True},
            "sewall": {"type": "bool", "required": False},
            "college": {"type": "str", "required": True},
            "full_name": {"type": "str", "required": True},
            "zip": {"type": "str", "required": True},
            "legal_name": {"type": "str", "required": True},
            "oda_reporting_seq": {"type": "str", "required": True},
            "cmci4k": {"type": "bool", "required": False},
            "perm_resident": {"type": "bool", "required": False},
            "leeds_silvergold": {"type": "bool", "required": False},
            "major": {"type": "str", "required": True},
            "decision_detail": {"type": "str", "required": True},
            "leeds_scholar_admit": {"type": "bool", "required": False},
            "act": {"type": "int", "required": False},
            "sat": {"type": "int", "required": False},
            "piano": {"type": "bool", "required": False},
            "school_county": {"type": "str", "required": True},
            "school_state": {"type": "str", "required": True},
            "transfer_instate": {"type": "bool", "required": False},
            "sewell": {"type": "bool", "required": False},
            "pgpa2": {"type": "float", "required": False},
            "school_name": {"type": "str", "required": True},
            "missing": {"type": "str", "required": True},
            "baker": {"type": "bool", "required": False},
            "chancellor": {"type": "bool", "required": False},
            "city": {"type": "str", "required": True},
            "hispanic": {"type": "bool", "required": False},
            "transfer_oos": {"type": "bool", "required": False},
            "leeds_scholar_app": {"type": "bool", "required": False},
            "last_name": {"type": "str", "required": True},
            "school_geomarket": {"type": "str", "required": True},
            "impact": {"type": "bool", "required": False},
            "presidential": {"type": "bool", "required": False},
            "entry_term": {"type": "str", "required": True},
            "decision": {"type": "str", "required": True},
            "ss": {"type": "bool", "required": False},
            "zip_plusfour": {"type": "str", "required": True},
            "residency": {"type": "str", "required": True},
            "fafsa": {"type": "bool", "required": False},
            "leeds_honors": {"type": "bool", "required": False},
            "school_country": {"type": "str", "required": True},
            "decision_date": {"type": "str", "required": False},
            "ah_instate": {"type": "bool", "required": False},
            "sex": {"type": "str", "required": True},
            "denver": {"type": "bool", "required": False},
            "student_type": {"type": "str", "required": True},
            "cmci1k": {"type": "bool", "required": False},
            "dean": {"type": "bool", "required": False},
            "target_group": {"type": "str", "required": True},
            "engineering_merit": {"type": "str", "required": True},
            "first_gen": {"type": "bool", "required": False},
            "ref": {"type": "str", "required": True},
            "birthdate": {"type": "str", "required": False},
        }
