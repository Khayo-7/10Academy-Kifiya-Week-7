import os, sys, logging
from typing import List, Union

import uvicorn
from sqlalchemy.orm import Session
from fastapi import FastAPI, Query, Depends, HTTPException

import controllers, schemas, prediction, database

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
try:
    from scripts.utils.logger import setup_logger
except ImportError as e:
    logging(f"Import error: {e}. Please check the module path.")

# Setup logger for deployement
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'logs')
logger = setup_logger("fastapi_deployement", log_dir)

# Initialize FastAPI
app = FastAPI(title="Medical Business API", version="1.0")

# Get Scraped Data from MongoDB**
@app.get("/scraped-data", tags=["Scraped Data"])
def get_scraped_data(limit: int = Query(10, description="Number of records to fetch")):
    return {"data": controllers.get_scraped_data(limit)}

# Get Cleaned Data from MongoDB**
@app.get("/cleaned-data", tags=["Cleaned Data"])
def get_cleaned_data(
    business_name: str = Query(None, description="Filter by business name"),
    limit: int = Query(10, description="Number of records to fetch"),
):
    return {"data": controllers.get_cleaned_data(business_name, limit)}

# Get Object Detection Results from MongoDB**
@app.get("/detected-objects", tags=["Object Detection"])
def get_detected_objects(
    object_class: str = Query(None, description="Filter by object class"),
    limit: int = Query(10, description="Number of records to fetch"),
):
    return {"data": controllers.get_detected_objects(object_class, limit)}

# Get Cleaned Data from PostgreSQL**
@app.get("/cleaned-data-pg", tags=["Cleaned Data"], response_model=list[schemas.CleanedDataSchema])
def get_cleaned_data_pg(
    limit: int = Query(10, description="Number of records to fetch"),
    db: Session = Depends(database.get_db),
):
    return controllers.get_cleaned_data_pg(db, limit)

@app.get("/", tags=["Root"])
def root():
    return {"message": "Welcome to the Medical Business API!"}

@app.post("/predict/", response_model=Union[schemas.PredictionOutput, list[schemas.PredictionOutput]])
async def predict(
    input_image: Union[schemas.PredictionInput, List[schemas.PredictionInput], dict], 
    # db: Session = Depends(database.get_db)
):

    logger.info("Starting unified prediction endpoint...")

    try:
        predictions = prediction.make_prediction(input_image.model_dump())
        logger.info("Successfully completed batch prediction.")
        return {"Predictions": predictions}

    except Exception as e:
        logger.error("Error in prediction endpoint: %s", str(e))
        raise HTTPException(status_code=500, detail="Internal Server Error.")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=7777, reload=True)
    # uvicorn app.main:app --reload --host 0.0.0.0 --port 7777