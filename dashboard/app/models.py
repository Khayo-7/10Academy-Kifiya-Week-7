from sqlalchemy import Column, Integer, String
from database import Base

class Images(Base):
    __tablename__ = "images"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    path = Column(String, index=True)
    content = Column(String, index=True)