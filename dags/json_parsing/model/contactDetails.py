from pydantic import BaseModel


class ContactDetails(BaseModel):
    phone: str
    cell: str
