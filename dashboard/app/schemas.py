from pydantic import BaseModel

class PredictionOutput(BaseModel):
    name: str
    content: str
    path: str

class PredictionInput(BaseModel):
    id: int
    name: str
    content: str
    path: str