from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import List, Optional
import os

load_dotenv()

app = FastAPI(
    title="E-commerce Recommendation API",
    description="API for serving product recommendations using ALS model",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["ecommerce_recommendation"]
collection = db["recommendations"]

class Recommendation(BaseModel):
    product_id: int
    score: float

class UserRecommendations(BaseModel):
    user_id: int
    recommendations: List[Recommendation]

@app.get("/", tags=["Health"])
async def root():
    """Root endpoint - API status"""
    return {
        "message": "E-commerce Recommendation API",
        "status": "running",
        "version": "1.0.0"
    }

@app.get("/health", tags=["Health"])
async def health_check():
    """Check API and MongoDB connection status"""
    try:
        client.admin.command('ping')
        db_status = "connected"
    except Exception as e:
        db_status = f"error: {str(e)}"
    
    return {
        "status": "healthy",
        "mongodb": db_status
    }
@app.get("/users/{user_id}/recommendations", response_model=UserRecommendations, tags=["Recommendations"])
async def get_user_recommendations(user_id: int):
    """Get product recommendations for a specific user"""
    try:
        result = collection.find_one({"user_id": user_id})
        
        if result is None:
            raise HTTPException(
                status_code=404, 
                detail=f"No recommendations found for user {user_id}"
            )
        result.pop("_id", None)
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/recommendations", response_model=List[UserRecommendations], tags=["Recommendations"])
async def get_all_recommendations(limit: int = 100):
    """Get all recommendations (with optional limit)"""
    try:
        results = list(collection.find().limit(limit))
        for result in results:
            result.pop("_id", None)
        
        return results
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/recommendations/stats", tags=["Statistics"])
async def get_stats():
    """Get statistics about recommendations"""
    try:
        total_users = collection.count_documents({})
        
        sample = list(collection.find().limit(10))
        avg_score = 0
        if sample:
            total_score = sum(
                rec.get("score", 0) 
                for doc in sample 
                for rec in doc.get("recommendations", [])
            )
            total_recs = sum(
                len(doc.get("recommendations", [])) 
                for doc in sample
            )
            avg_score = total_score / total_recs if total_recs > 0 else 0
        
        return {
            "total_users": total_users,
            "avg_score_sample": round(avg_score, 2),
            "mongodb_collection": collection.name
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
