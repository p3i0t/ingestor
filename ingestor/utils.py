import datetime
from typing import TypeVar, Annotated

import dateutil.parser

Datetime = TypeVar('Datetime', 
                   Annotated[str, "string like '20220201', '2022-02-01' or 'today'."], 
                   datetime.date,
                   Annotated[int, "int like 20220201."])

def any2date(dt: Datetime) -> datetime.date:
    if isinstance(dt, datetime.date):
        return dt
    if isinstance(dt, int):
        dt = str(dt) # noqa
    if isinstance(dt, str):
        if dt == 'today':
            o = datetime.datetime.today().date()
        else:
            o = dateutil.parser.parse(dt).date()
        return o
    else:
        raise TypeError(f"dt type {type(dt)} not supported.")