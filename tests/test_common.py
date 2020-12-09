from datetime import datetime, timezone
from common import sign_request, build_headers


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
            action="GET",
            path_no_query="/bogus",
        )

        # The cryptostring changes with the date from sign_request
        assert result["Authorization"].startswith("SignalVine INVENTED_TOKEN:")

        # The content-type is json
        assert result["Content-Type"] == "application/json"

        # A date should exist
        assert result["SignalVine-Date"]
