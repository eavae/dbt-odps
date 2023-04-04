import dbt.exceptions  # noqa
from contextlib import contextmanager
from dataclasses import dataclass
from dbt.adapters.base import Credentials, BaseConnectionManager
from dbt.logger import GLOBAL_LOGGER as logger
from odps import ODPS


@dataclass
class ODPSCredentials(Credentials):
    """
    Defines database specific credentials that get added to
    profiles.yml to connect to new adapter
    """

    # Add credentials members here, like:
    endpoint: str
    access_id: str
    secret_access_key: str
    project: str

    _ALIASES = {"access_id": "ak", "secret_access_key": "sk"}

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
        return self.project

    def _connection_keys(self):
        """
        List of keys to display in the `dbt debug` output.
        """
        return ("endpoint", "access_id", "project")


class ODPSConnectionManager(BaseConnectionManager):
    TYPE = "odps"

    @contextmanager
    def exception_handler(self, sql: str):
        """
        Returns a context manager, that will handle exceptions raised
        from queries, catch, log, and raise dbt exceptions it knows how to handle.
        """
        # ## Example ##
        # try:
        #     yield
        # except myadapter_library.DatabaseError as exc:
        #     self.release(connection_name)

        #     logger.debug("myadapter error: {}".format(str(e)))
        #     raise dbt.exceptions.DatabaseException(str(exc))
        # except Exception as exc:
        #     logger.debug("Error running SQL: {}".format(sql))
        #     logger.debug("Rolling back transaction.")
        #     self.release(connection_name)
        #     raise dbt.exceptions.RuntimeException(str(exc))
        pass

    @classmethod
    def open(cls, connection):
        """
        Receives a connection object and a Credentials object
        and moves it to the "open" state.
        """
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials: ODPSCredentials = connection.credentials

        try:
            handle = ODPS(
                endpoint=credentials.endpoint,
                access_id=credentials.access_id,
                secret_access_key=credentials.secret_access_key,
                project=credentials.project,
            )
            connection.state = "open"
            connection.handle = handle
        except Exception as exc:
            logger.debug("Error opening connection: {}".format(exc))
            connection.handle = None
            connection.state = "fail"

        return connection

    @classmethod
    def get_response(cls, cursor):
        """
        Gets a cursor object and returns adapter-specific information
        about the last executed command generally a AdapterResponse object
        that has items such as code, rows_affected,etc. can also just be a string ex. "OK"
        if your cursor does not offer rich metadata.
        """
        # ## Example ##
        # return cursor.status_message
        pass

    def cancel(self, connection):
        """
        Gets a connection object and attempts to cancel any ongoing queries.
        """
        # ## Example ##
        # tid = connection.handle.transaction_id()
        # sql = "select cancel_transaction({})".format(tid)
        # logger.debug("Cancelling query "{}" ({})".format(connection_name, pid))
        # _, cursor = self.add_query(sql, "master")
        # res = cursor.fetchone()
        # logger.debug("Canceled query "{}": {}".format(connection_name, res))
        pass
