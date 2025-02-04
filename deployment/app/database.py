from pymongo import MongoClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
# from sqlalchemy.ext.declarative import declarative_base

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017"
mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client["medical"]

# PostgreSQL Configuration
POSTGRES_URI = "postgresql://postgres:1234@localhost:5432/medical_db"
pg_engine = create_engine(POSTGRES_URI)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=pg_engine)

# SQLAlchemy Base
Base = declarative_base()

# Dependency function for PostgreSQL session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
