import dbt.exceptions
from contextlib import contextmanager
from dataclasses import dataclass
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.contracts.connection import AdapterResponse, ConnectionState, AdapterRequiredConfig
from dbt.dataclass_schema import StrEnum
from odps.errors import ODPSError
from .dbapi import ODPSConnection
from .errors import NotTableError

DEFAULT_HINTS = {
    "odps.sql.allow.cartesian": "true",
}


@dataclass
class SchemaTypes(StrEnum):
    PREFIX_SCHEMA = "prefix_schema"


@dataclass(order=False)
class ODPSCredentials(Credentials):
    """
    Defines database specific credentials that get added to
    profiles.yml to connect to new adapter
    """

    # Add credentials members here, like:
    endpoint: str
    access_id: str
    secret_access_key: str
    schema_type: SchemaTypes = SchemaTypes.PREFIX_SCHEMA

    _ALIASES = {"ak": "access_id", "sk": "secret_access_key"}

    @property
    def type(self):
        """Return name of adapter."""
        return "odps"

    @property
    def unique_field(self):
        """
        Hashed and included in anonymous telemetry to track adapter adoption.
        Pick a field that can uniquely identify one team/organization building with this adapter
        """
        return self.endpoint + "#" + self.database

    def _connection_keys(self):
        """
        List of keys to display in the `dbt debug` output.
        """
        return ("endpoint", "access_id", "database", "schema", "schema_type")


class ODPSConnectionManager(SQLConnectionManager):
    TYPE = "odps"

    def __init__(self, profile: AdapterRequiredConfig):
        # disable query comment for odps, since it's not supported
        profile.query_comment.comment = None

        super().__init__(profile)

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except Exception as exc:
            logger.debug("Error while running:\n{}".format(sql))
            logger.debug(exc)

            if isinstance(exc, ODPSError) and exc.code == "ODPS-0130071":
                raise NotTableError(exc.code, exc.args[0])

            if len(exc.args) == 0:
                raise

            thrift_resp = exc.args[0]
            if hasattr(thrift_resp, "status"):
                msg = thrift_resp.status.errorMessage
                raise dbt.exceptions.DbtRuntimeError(msg)
            else:
                raise dbt.exceptions.DbtRuntimeError(str(exc))

    @classmethod
    def open(cls, connection):
        """
        Receives a connection object and a Credentials object
        and moves it to the "open" state.
        """
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials: ODPSCredentials = connection.credentials
        try:
            handle = ODPSConnection(
                endpoint=credentials.endpoint,
                access_id=credentials.access_id,
                secret_access_key=credentials.secret_access_key,
                project=credentials.database,
                hints=DEFAULT_HINTS,
            )
            if not handle.odps.exist_project(credentials.database):
                logger.debug("Project {} does not exist".format(credentials.database))
                raise dbt.exceptions.FailedToConnectError(f"Project {credentials.database} does not exist.")

            connection.state = "open"
            connection.handle = handle
        except Exception as exc:
            logger.debug("Error opening connection: {}".format(exc))
            connection.handle = None
            connection.state = "fail"

        return connection

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        # ODPS does not support cursor and rowcount
        # https://github.com/dbt-labs/dbt-spark/issues/142
        message = "OK"
        return AdapterResponse(_message=message)

    # No transactions on ODPS....
    def add_begin_query(self, *args, **kwargs):
        logger.debug("Not Supported: add_begin_query")

    def add_commit_query(self, *args, **kwargs):
        logger.debug("Not Supported: add_commit_query")

    def commit(self, *args, **kwargs):
        logger.debug("Not Supported: commit")

    def rollback(self, *args, **kwargs):
        logger.debug("Not Supported: rollback")

    def cancel(self, connection):
        connection.handle.cancel()
