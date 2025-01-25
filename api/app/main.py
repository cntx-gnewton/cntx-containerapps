from fastapi import FastAPI, Form, Request, status, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
import logging
# from .blob_client import BlobClient
import os 
os.environ['BlobServiceClientConnStr'] = 'DefaultEndpointsProtocol=https;AccountName=cntxstorage7a1f4155;AccountKey=5KKTlwjwkozJgRqYj33pr3DxpCZuQ8gkvq1jm6i/CVnJrGkCpSgxKosTKTnH6W2GuBriw5EEC08A+AStYQvmkw==;EndpointSuffix=core.windows.net'
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods="GET",
    allow_headers=["*"]
)


app = FastAPI(debug=True)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    print('Request for index page received')
    return templates.TemplateResponse('index.html', {"request": request})


@app.get('/favicon.ico')
async def favicon():
    file_name = 'favicon.ico'
    file_path = './static/' + file_name
    return FileResponse(path=file_path, headers={'mimetype': 'image/vnd.microsoft.icon'})


@app.post('/hello', response_class=HTMLResponse)
async def hello(request: Request, name: str = Form(...)):
    if name:
        print('Request for hello page received with name=%s' % name)
        return templates.TemplateResponse('hello.html', {"request": request, 'name': name})
    else:
        print('Request for hello page received with no name or blank name -- redirecting')
        return RedirectResponse(request.url_for("index"), status_code=status.HTTP_302_FOUND)


@app.get("/read_blob")
def read_blob():
    try:
        logging.info(f'{os.environ["BlobServiceClientConnStr"]}')
        # cli = BlobClient('test')
        # content = cli.read('test.txt')
        # if content:
        return {'status': 202}
        # else:
        #     raise HTTPException(status_code=404, detail="Blob not found")
        
    except Exception as e:
        logging.error(f"Error reading blob: {e}")
        raise HTTPException(status_code=500, detail="Failed to read blob")
