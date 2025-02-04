from sqlalchemy import Column, Integer, String, JSON
from database import Base

class Images(Base):
    __tablename__ = "images"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    path = Column(String, index=True)
    content = Column(String, index=True)

class CleanedData(Base):
    __tablename__ = "cleaned_data"

    id = Column(Integer, primary_key=True, index=True)
    business_name = Column(String, index=True)
    phone_number = Column(String)
    address = Column(String)
    scraped_date = Column(String)
    media_paths = Column(JSON)
    links = Column(JSON)
