# Empty init file to make the directory a package

from .economic import router as economic_router


from fastapi import APIRouter
from .energy import energy_router

api_router = APIRouter()
api_router.include_router(energy_router, prefix="/energy", tags=["energy"])
