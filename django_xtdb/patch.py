from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from django.conf import settings
from django.db import router
from django.db.models.fields import NOT_PROVIDED, AutoField, Field

if TYPE_CHECKING:
    from django.db.models import Model


def is_xtdb_model(cls: type[Model]) -> bool:
    db = router.db_for_write(cls)
    if settings.DATABASES[db]["ENGINE"] == "django_xtdb":
        return True
    else:
        return False


def contribute_to_class(self: Any, cls: type[Model], name: str, private_only: bool = False) -> None:
    if self.primary_key and is_xtdb_model(cls):
        # XTDB always requires an _id column with the primary key of the row.
        self.db_column = "_id"
        if self.default is NOT_PROVIDED and isinstance(self, AutoField):
            # XTDB does not support server side IDs and Django AutoField that is
            # used for the primary key field relies on this. Normally you insert
            # a row without the primary key and get the primary key back from
            # PostgreSQL. For XTDB we need to generate the primary key IDs
            # ourself.
            #
            # The number of nanoseconds since the unix epoch gives us a 64-bit
            # number that is increasing and also very unlikely to give
            # collisions. This is also what is used in UUID1 and because we
            # don't care about being globally unique we don't need the host part
            # of UUID1.
            #
            # Some tests in de Django test suite expect that ids are increasing
            # and fail when you use random numbers, so having a number that is
            # increasing makes running the test suite easier.
            self.default = time.time_ns

            # Given that XTDB does not have server side IDs those also won't be returned
            # and we have to set this to False.
            self.db_returning = False

    if self.remote_field and self.remote_field.parent_link and is_xtdb_model(cls):
        self.db_column = "_id"

    self.orig_contribute_to_class(cls, name, private_only)


def monkey_patch() -> None:
    Field.orig_contribute_to_class = Field.contribute_to_class  # type: ignore[attr-defined]
    Field.contribute_to_class = contribute_to_class  # type: ignore[method-assign]
