from pydantic import BaseModel


class MediaData(BaseModel):
    large: str
    medium: str
    thumbnail: str
