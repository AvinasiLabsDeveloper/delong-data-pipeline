from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from os import getenv

# Local imports
from routers.data_pipe import router as data_pipe_router

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  #  List of origins that are allowed to make cross-origin requests. Use ["*"] to allow all.
    allow_credentials=True, #  Support cookies and authorization headers.
    allow_methods=["*"],    #  Allow all HTTP methods (GET, POST, PUT, DELETE, etc.). Or specify like ["GET", "POST"].
    allow_headers=["*"],    #  Allow all headers. Or specify like ["Content-Type", "Authorization"].
)

# Add routers
app.include_router(data_pipe_router, prefix="/api/delong/v1/data_pipe")


@app.get("/api/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=getenv("PORT", 8019))
