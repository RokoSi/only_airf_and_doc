from pydantic import BaseModel


class Registered(BaseModel):
    date: str
    age: int
