from pydantic import BaseModel

from .dob import Dob
from .location import Location
from .media_data import MediaData
from .name import Name
from .registered import Registered
from .registration_data import RegistrationData


class Users(BaseModel):
    valid: bool
    gender: str
    name: Name
    location: Location
    dob: Dob
    nat: str
    email: str
    login: RegistrationData
    registered: Registered
    phone: str
    cell: str
    picture: MediaData
