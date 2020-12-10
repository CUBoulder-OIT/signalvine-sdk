# signalvine-sdk

Use the SignalVine API with Python

## Installation

    pip install git+https://github.com/CUBoulder-OIT/signalvine-sdk@master#signalvine-sdk

## Building and Distribution

### Distribution Creation

    python setup.py sdist bdist_wheel

### Testing

To test, you'll have to configure account numbers, access token, and secrets

From the OS, set or export SignalVine variables for the tests:

    set account_number=1234-123-123-123-1234
    set account_token=12345567
    set account_secret="1234-123-123-123-1234

    python setup.py test
