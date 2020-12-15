# signalvine-sdk

Use the SignalVine API with Python

## Installation

    pip install git+https://github.com/CUBoulder-OIT/signalvine-sdk@master#egg=signalvine-sdk

## Building and Distribution

### Distribution Creation

    python setup.py sdist bdist_wheel

### Testing

To test, you'll have to configure account numbers, access token, and secrets

From the OS, set or export SignalVine variables for the tests:

    set ACCOUNT_NUMBER=1234-123-123-123-1234
    set ACCOUNT_TOKEN=12345567
    set ACCOUNT_SECRET=1234-123-123-123-1234
    set PROGRAM_ID=1234-123-123-123-1234

    pip install -e .
    python setup.py test

    # To test a single
    pytest tests\test_sdk.py -k "test_upsert_participants"
