from typing import Union

from pydantic import BaseModel

from .coordinates import Coordinates
from .street import Street


class Location(BaseModel):
    street: Street
    city: str
    state: str
    country: str
    postcode: Union[str, int]
    coordinates: Coordinates
