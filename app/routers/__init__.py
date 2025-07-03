# Empty init file to make the directory a package
from fastapi import APIRouter
from .auth import router as auth_router
from .economic import router as economic_router
from .reference import router as reference_router
from .energy import router as energy_router  # ✅ Must be named 'router'
from .environment import router as environment_router  # ✅ Must be named 'router'
from .hr import router as hr_router
from .csr import router as csr_router
from .usable_apis import router as usable_apis_router
from .environment_dash import router as environment_dash_router
from .account import router as account_router  # ✅ Must be named 'router'


api_router = APIRouter()
api_router.include_router(auth_router, prefix="/auth", tags=["authentication"])
api_router.include_router(energy_router, prefix="/energy", tags=["energy"])
api_router.include_router(economic_router, prefix="/economic", tags=["economic"])
api_router.include_router(reference_router, prefix="/reference", tags=["reference"])
api_router.include_router(environment_router, prefix="/environment", tags=["environment"])
api_router.include_router(hr_router, prefix="/hr", tags=["hr"])
api_router.include_router(csr_router, prefix="/help", tags=["help"])
api_router.include_router(usable_apis_router, prefix="/usable_apis", tags=["usable_apis"])
api_router.include_router(environment_dash_router, prefix="/environment_dash", tags=["environment_dash"])
api_router.include_router(account_router, prefix="/accounts", tags=["accounts"])