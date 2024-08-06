from pydantic import BaseModel


class Dob(BaseModel):
    age: int
