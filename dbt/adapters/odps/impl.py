from dbt.adapters.base import BaseAdapter

from dbt.adapters.odps import ODPSConnectionManager


class ODPSAdapter(BaseAdapter):
    """
    Controls actual implmentation of adapter, and ability to override certain methods.
    """

    ConnectionManager = ODPSConnectionManager

    @classmethod
    def date_function(cls):
        """
        Returns canonical date func
        """
        return "datenow()"


# may require more build out to make more user friendly to confer with team and community.
