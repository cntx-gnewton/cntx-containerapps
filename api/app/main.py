from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging
from .blob_client import BlobClient
import os 

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods="GET",
    allow_headers=["*"]
)


@app.get("/")
def read_root():
    logging.info(f'{os.environ["BlobServiceClientConnStr"]}')
    return {"status":202}


@app.get("/albums")
def get_albums():
    logging.info(f'{os.environ["BlobServiceClientConnStr"]}')
    cli = BlobClient('test')
    content = cli.read('test.txt')
    return {"status": 202, "content": content}
