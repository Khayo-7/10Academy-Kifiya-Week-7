import os
import sys
import logging
from typing import List, Union
import uvicorn
from fastapi import FastAPI, Depends, HTTPException
from app.prediction import make_prediction
from app.schemas import PredictionInput, PredictionOutput
from sqlalchemy.orm import Session
import crud, models, schemas
from database import SessionLocal, engine, get_db

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
try:
    from scripts.utils.logger import setup_logger
except ImportError as e:
    logging(f"Import error: {e}. Please check the module path.")

# Setup logger for deployement
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'logs')
logger = setup_logger("fastapi_deployement", log_dir)

app = FastAPI()
models.Base.metadata.create_all(bind=engine)

@app.get("/")
async def root():
    logger.info("Starting root function...")
    response = {"message": "Welcome to the Credit scoring API! The server is running!"}
    logger.info("Ending root function...")
    return response

@app.post("/predict/", response_model=Union[PredictionOutput, list[PredictionOutput]])
async def predict(input_image: Union[PredictionInput, List[PredictionInput], dict], db: Session = Depends(get_db)):

    logger.info("Starting unified prediction endpoint...")

    try:
        predictions = make_prediction(input_image.model_dump())
        logger.info("Successfully completed batch prediction.")
        return {"Predictions": predictions}

    except Exception as e:
        logger.error("Error in prediction endpoint: %s", str(e))
        raise HTTPException(status_code=500, detail="Internal Server Error.")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=7777, reload=True)
    # uvicorn app.main:app --reload --host 0.0.0.0 --port 7777