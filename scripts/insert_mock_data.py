from pymongo import MongoClient
from dotenv import load_dotenv
import os
import json

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

client = MongoClient(MONGO_URI)

db = client["ecommerce_recommendation"]
collection = db["recommendations"]

with open("scripts/recommendations_mock.json", "r") as file:
    data = json.load(file)

collection.delete_many({})
collection.insert_many(data)

print("Mock data inserted successfully!")