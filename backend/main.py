from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .endpoints import router

app = FastAPI(title="Load Flow Analysis App")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all for development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


@app.get("/")
async def root():
    return {"message": "Load Flow Analysis API is running"}


@app.get("/api/health")
async def health():
    return {"status": "ok"}
