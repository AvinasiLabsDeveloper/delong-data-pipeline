from pydantic import BaseModel
from typing import List, Union
from datetime import datetime, timedelta


# Data Pipe
class FakeDataRequest(BaseModel):
    data: List[Union[str, int, float, bool, datetime, timedelta]]
    n_samples: int


class StatColumnDataRequest(BaseModel):
    data: List[Union[str, int, float, bool, datetime, timedelta]]