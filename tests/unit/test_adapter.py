import unittest
import os
from unittest import mock
from dbt.adapters.base.query_headers import MacroQueryStringSetter
from dbt.adapters.odps import ODPSAdapter
from dbt.contracts.graph.manifest import ManifestStateCheck
from dbt.contracts.files import FileHash
from dbt.adapters.odps import Plugin as OdpsPlugin
from odps import dbapi

from .utils import config_from_parts_or_dicts, load_internal_manifest_macros, inject_adapter, clear_plugin


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
                    "endpoint": "mock_endpoint",
                    "access_id": "mock_access_id",
                    "secret_access_key": "mock_secret_access_key",
                    "database": "mock_database",
                    "schema": "default",
                }
            },
            "target": "test",
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)

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


class TestConnectionOdpsAdapter(unittest.TestCase):
    def setUp(self) -> None:
        target_dict = {
            "type": "odps",
            "endpoint": os.environ.get("ODPS_ENDPOINT"),
            "access_id": os.environ.get("ODPS_ACCESS_ID"),
            "secret_access_key": os.environ.get("ODPS_SECRET_ACCESS_KEY"),
            "database": os.environ.get("ODPS_PROJECT"),
            "schema": "public",
        }

        profile_cfg = {
            "outputs": {
                "test": target_dict,
            },
            "target": "test",
        }
        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "config-version": 2,
        }
        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)

        self.handle = mock.MagicMock(spec=dbapi.Connection)
        self.cursor = self.handle.cursor.return_value
        self.mock_execute = self.cursor.execute
        self.patcher = mock.patch(
            "dbt.adapters.odps.connections.ODPSConnection",
            return_value=self.handle,
        )
        self.odps = self.patcher.start()

        # Create the Manifest.state_check patcher
        @mock.patch("dbt.parser.manifest.ManifestLoader.build_manifest_state_check")
        def _mock_state_check(self):
            all_projects = self.all_projects
            return ManifestStateCheck(
                vars_hash=FileHash.from_contents("vars"),
                project_hashes={name: FileHash.from_contents(name) for name in all_projects},
                profile_hash=FileHash.from_contents("profile"),
            )

        self.load_state_check = mock.patch("dbt.parser.manifest.ManifestLoader.build_manifest_state_check")
        self.mock_state_check = self.load_state_check.start()
        self.mock_state_check.side_effect = _mock_state_check

        self.adapter = ODPSAdapter(self.config)
        self.adapter._macro_manifest_lazy = load_internal_manifest_macros(self.config)
        self.adapter.connections.query_header = MacroQueryStringSetter(
            self.config, self.adapter._macro_manifest_lazy
        )

        self.qh_patch = mock.patch.object(self.adapter.connections.query_header, "add")
        self.adapter.acquire_connection()
        inject_adapter(self.adapter, OdpsPlugin)

    def tearDown(self):
        # we want a unique self.handle every time.
        self.adapter.cleanup_connections()
        self.qh_patch.stop()
        self.patcher.stop()
        self.load_state_check.stop()
        clear_plugin(OdpsPlugin)
