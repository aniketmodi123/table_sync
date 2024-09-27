import models
import time
import pytz
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from config import engine
from datetime import datetime
from utils import logs
from scripts.sync_table import sync_table
from scripts.sync_table_column import selective_column_sync_table
from scripts.combine_table_sync import combine_table_and_sync

models.Base.metadata.create_all(bind=engine)

app = FastAPI(tzinfo=pytz.timezone('Asia/Kolkata'))

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*']
)


@app.on_event("startup")
async def startup_event():
    # Set the timezone for the application
    datetime.now(pytz.timezone('Asia/Kolkata'))


@app.middleware("http")
async def add_process_time_header(request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(f'{process_time:0.4f} sec')
    return response


@app.get('/')
def index():
    data = {'Hello': 'World'}
    logs('Hello world')

    return data

@app.get('/sync-tables')
def syncing_table():
    logs("start syncing tables")
    result = sync_table()
    print(result)
    return "done" if result  else "not done"

@app.get('/selective-column-sync-tables')
def syncing_table():
    logs("start syncing tables")
    result = selective_column_sync_table()
    print(result)
    return "done" if result  else "not done"

@app.get('/combine-table-and-sync')
def syncing_table():
    logs("start syncing tables")
    result = combine_table_and_sync()
    print(result)
    return "done" if result  else "not done"


