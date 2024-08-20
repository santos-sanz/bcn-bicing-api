from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://example.com",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Orígenes permitidos
    allow_credentials=True,
    allow_methods=["*"],  # Métodos permitidos
    allow_headers=["*"],  # Cabeceras permitidas
)

@app.get("/")
def read_root():
    return {"message": "Hello, World!"}


