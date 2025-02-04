from models import CleanedData
from sqlalchemy.orm import Session
from database import mongo_db, SessionLocal

# Fetch Scraped Data from MongoDB
def get_scraped_data(limit: int = 10):
    collection = mongo_db["raw_data"]
    data = list(collection.find().limit(limit))
    for doc in data:
        doc["_id"] = str(doc["_id"])  # Convert ObjectId to string
    return data

# Fetch Cleaned Data from MongoDB
def get_cleaned_data(business_name: str = None, limit: int = 10):
    collection = mongo_db["cleaned_data"]
    query = {}
    if business_name:
        query["business_name"] = {"$regex": business_name, "$options": "i"}
    
    data = list(collection.find(query).limit(limit))
    for doc in data:
        doc["_id"] = str(doc["_id"])
    
    return data

# Fetch Object Detection Results from MongoDB
def get_detected_objects(object_class: str = None, limit: int = 10):
    collection = mongo_db["detected_objects"]
    query = {}
    if object_class:
        query["detections.class"] = {"$regex": object_class, "$options": "i"}
    
    data = list(collection.find(query).limit(limit))
    for doc in data:
        doc["_id"] = str(doc["_id"])
    
    return data

# Fetch Cleaned Data from PostgreSQL
def get_cleaned_data_pg(db: Session, limit: int = 10):
    return db.query(CleanedData).limit(limit).all()
