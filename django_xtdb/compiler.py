from __future__ import annotations

import typing
from typing import Any

from django.db.models.sql.compiler import SQLAggregateCompiler  # noqa: F401
from django.db.models.sql.compiler import SQLCompiler as BaseSQLCompiler
from django.db.models.sql.compiler import SQLDeleteCompiler as BaseSQLDeleteCompiler
from django.db.models.sql.compiler import SQLInsertCompiler as BaseSQLInsertCompiler
from django.db.models.sql.compiler import SQLUpdateCompiler as BaseSQLUpdateCompiler

if typing.TYPE_CHECKING:
    from django.db.models.sql.compiler import _AsSqlType


class SQLCompiler(BaseSQLCompiler):
    # This is workaround for the XTDB duplicate column projection error that
    # happens if you select two columns of the same name from two different
    # tables
    def as_sql(self, with_limits: bool = True, with_col_aliases: bool = False) -> _AsSqlType:
        return super().as_sql(with_limits, with_col_aliases=True)

    def execute_sql(self, *args: Any, **kwargs: Any) -> Any:
        if self.connection.connection:
            self.connection.connection.read_only = True
            with self.connection.connection.transaction():
                ret = super().execute_sql(*args, **kwargs)
        else:
            ret = super().execute_sql(*args, **kwargs)

        return ret


class SQLInsertCompiler(BaseSQLInsertCompiler):
    def execute_sql(self, *args: Any, **kwargs: Any) -> list[tuple[Any]]:  # type: ignore[override]
        if self.connection.connection:
            self.connection.connection.read_only = False
            with self.connection.connection.transaction():
                ret = super().execute_sql(*args, **kwargs)
        else:
            ret = super().execute_sql(*args, **kwargs)

        return ret


class SQLUpdateCompiler(BaseSQLUpdateCompiler):
    def execute_sql(self, *args: Any, **kwargs: Any) -> int:  # type: ignore[override]
        if self.connection.connection:
            self.connection.connection.read_only = False
            with self.connection.connection.transaction():
                super().execute_sql(*args, **kwargs)
        else:
            super().execute_sql(*args, **kwargs)

        # XTDB currently does not return changed row counts so we fake that
        # something changed to make Django happy
        return 1


class SQLDeleteCompiler(BaseSQLDeleteCompiler):
    def execute_sql(self, *args: Any, **kwargs: Any) -> Any:
        if self.connection.connection:
            self.connection.connection.read_only = False
            with self.connection.connection.transaction():
                ret = super().execute_sql(*args, **kwargs)
        else:
            ret = super().execute_sql(*args, **kwargs)

        return ret
