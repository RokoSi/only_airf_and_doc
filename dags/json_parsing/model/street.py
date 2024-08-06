from pydantic import BaseModel


class Street(BaseModel):
    name: str
    number: int
