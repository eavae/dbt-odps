from decimal import Decimal
from odps.dbapi import Connection, Cursor
from odps.utils import to_str


def is_numeric(value):
    return isinstance(value, (int, float, Decimal))


def quote(value):
    return f"'{to_str(value)}'" if not is_numeric(value) else to_str(value)


class ODPSCursor(Cursor):
    def execute(self, operation, parameters=None, **kwargs):
        for k in ["async", "async_"]:
            if k in kwargs:
                async_ = kwargs[k]
                break
        else:
            async_ = False

        # format parameters
        if parameters is None:
            sql = operation
        else:
            raise NotImplementedError("Parameters are not supported yet")

        self._reset_state()
        odps = self._connection.odps
        run_sql = odps.execute_sql
        if self._use_sqa:
            run_sql = self._run_sqa_with_fallback
        if async_:
            run_sql = odps.run_sql

        print("Final SQL:\n {}".format(sql))
        self._instance = run_sql(sql, hints=self._hints)


class ODPSConnection(Connection):
    def cursor(self, *args, **kwargs):
        return ODPSCursor(
            self,
            *args,
            use_sqa=self._use_sqa,
            fallback_policy=self._fallback_policy,
            hints=self._hints,
            **kwargs,
        )
