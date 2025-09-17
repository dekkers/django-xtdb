import time
from typing import Any

from django.db.models.fields import NOT_PROVIDED, AutoField, BigAutoField, Field, SmallAutoField


def field_init(self: Any, *args: Any, **kwargs: Any) -> None:
    self.__orig_init__(*args, **kwargs)

    if self.primary_key:
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

    # This is for model inheritance. The parent_link is the field that links
    # back to the parent model. Given that functions as the primary key the
    # field also needs to be called _id.
    if self.remote_field and self.remote_field.parent_link:
        self.db_column = "_id"


def monkey_patch() -> None:
    # Given that XTDB does not have server side IDs those also won't be returned
    # and we have to set this to False.
    AutoField.db_returning = False
    SmallAutoField.db_returning = False
    BigAutoField.db_returning = False

    Field.__orig_init__ = Field.__init__  # type: ignore[attr-defined]
    Field.__init__ = field_init  # type: ignore[method-assign]
