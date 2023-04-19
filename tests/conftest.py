import pytest
import os

# import os
# import json

# Import the fuctional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        "type": "odps",
        "endpoint": os.environ.get("ODPS_ENDPOINT"),
        "access_id": os.getenv("ODPS_ACCESS_ID"),
        "secret_access_key": os.getenv("ODPS_SECRET_ACCESS_KEY"),
        "database": os.environ.get("ODPS_PROJECT"),
        "schema": "public",
    }
