from pydantic import BaseModel


class Name(BaseModel):
    title: str
    first: str
    last: str
