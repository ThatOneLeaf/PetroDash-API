from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routers import economic  # Import just the economic router for now

app = FastAPI(
    title="PetroDash API",
    description="REST API for PetroEnergy's data warehouse analytics",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(economic.router, prefix="/economic", tags=["Economic Data"])

@app.get("/", tags=["Root"])
async def root():
    return {
        "message": "Welcome to PetroDash API",
        "docs_url": "/docs",
        "openapi_url": "/openapi.json"
    }



