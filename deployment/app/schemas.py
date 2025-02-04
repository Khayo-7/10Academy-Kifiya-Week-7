from pydantic import BaseModel
from typing import List, Optional

class PredictionOutput(BaseModel):
    name: str
    content: str
    path: str

class PredictionInput(BaseModel):
    id: int
    name: str
    content: str
    path: str

class CleanedDataSchema(BaseModel):
    id: int
    business_name: str
    phone_number: Optional[str] = None
    address: Optional[str] = None
    scraped_date: str
    media_paths: List[str] = []
    links: List[str] = []

    class Config:
        from_attributes = True
