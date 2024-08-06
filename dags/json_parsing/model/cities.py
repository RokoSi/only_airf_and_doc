from pydantic import BaseModel


class Cities(BaseModel):
    city: str
    state: str
    country: str
