import unittest
import os
from unittest import mock
from dbt.adapters.odps import ODPSAdapter
from .utils import config_from_parts_or_dicts


def mock_connection(name, state="open"):
    conn = mock.MagicMock()
    conn.name = name
    conn.state = state
    return conn


class TestOdpsAdapter(unittest.TestCase):
    def setUp(self):
        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {
                "identifier": False,
                "schema": False,
            },
            "config-version": 2,
        }

        profile_cfg = {
            "outputs": {
                "test": {
                    "type": "odps",
                    "endpoint": os.environ.get("ODPS_ENDPOINT"),
                    "access_id": os.environ.get("ODPS_ACCESS_ID"),
                    "secret_access_key": os.environ.get("ODPS_SECRET_ACCESS_KEY"),
                    "database": os.environ.get("ODPS_PROJECT"),
                    "schema": "public",
                }
            },
            "target": "test",
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        self._adapter = None

    def test_schema_is_empty_always(self):
        assert self.config.credentials.schema == ""

    @mock.patch("dbt.adapters.odps.connections.ODPSConnection")
    def test_acquire_connection_validations(self, ODPSConnection):
        adapter = ODPSAdapter(self.config)
        connection = adapter.acquire_connection("dummy")
        self.assertEqual(connection.type, "odps")

        ODPSConnection.assert_not_called()
        connection.handle
        ODPSConnection.assert_called_once()

        self.assertEqual(connection.state, "open")
        self.assertNotEqual(connection.handle, None)

    def test_cancel_open_connections_empty(self):
        adapter = ODPSAdapter(self.config)
        self.assertEqual(len(list(adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_master(self):
        adapter = ODPSAdapter(self.config)
        key = adapter.connections.get_thread_identifier()
        adapter.connections.thread_connections[key] = mock_connection("master")
        self.assertEqual(len(list(adapter.cancel_open_connections())), 0)
