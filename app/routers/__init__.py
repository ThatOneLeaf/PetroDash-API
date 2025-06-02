# Empty init file to make the directory a package
from fastapi import APIRouter
from .economic import router as economic_router
from .reference import router as reference_router
from .energy import router as energy_router  # ✅ Must be named 'router'
from .environment import router as environment_router  # ✅ Must be named 'router'
from .hr import router as hr_router
from .csr import router as csr_router


api_router = APIRouter()
api_router.include_router(energy_router, prefix="/energy", tags=["energy"])
api_router.include_router(economic_router, prefix="/economic", tags=["economic"])
api_router.include_router(reference_router, prefix="/reference", tags=["reference"])
api_router.include_router(environment_router, prefix="/environment", tags=["environment"])
api_router.include_router(hr_router, prefix="/hr", tags=["hr"])
api_router.include_router(csr_router, prefix="/csr", tags=["csr"])
