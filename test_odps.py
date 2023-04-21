import os
import re
from odps.dbapi import connect
from sqlalchemy import text
from decimal import Decimal
from sqlparams import SQLParams


def test_odps():
    conn = connect(
        endpoint=os.environ.get("ODPS_ENDPOINT"),
        access_id=os.environ.get("ODPS_ACCESS_ID"),
        secret_access_key=os.environ.get("ODPS_SECRET_ACCESS_KEY"),
        project=os.environ.get("ODPS_PROJECT"),
    )

    table = conn._odps.get_table("__test16818848236269209324_test_basic__swappable")
    pass


if __name__ == "__main__":
    test_odps()
