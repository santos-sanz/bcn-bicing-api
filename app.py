from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

app = FastAPI()

# Allowed origins
origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://example.com",
]

# I only want to allow GET method
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Orígenes permitidos
    allow_credentials=True,
    allow_methods=["GET"],  # Allowed methods
    allow_headers=["*"],  # Allowed headers
)

@app.get("/")
def read_root():
    return {"message": "Hello, World!"}

@app.get("/")
def read_root():
    return {"message": "Hello, World!"}


