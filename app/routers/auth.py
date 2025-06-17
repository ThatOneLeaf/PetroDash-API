from datetime import timedelta
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from app.services.auth import (
    AuthService, 
    Token, 
    User, 
    ACCESS_TOKEN_EXPIRE_MINUTES,
    oauth2_scheme
)
from app.dependencies import get_db

router = APIRouter()

@router.post("/token", response_model=Token)
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: Session = Depends(get_db)
):
    """
    OAuth2 compatible token login, get an access token for future requests.
    """
    user = AuthService.authenticate_user(form_data.username, form_data.password, db)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = AuthService.create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "expires_in": int(ACCESS_TOKEN_EXPIRE_MINUTES * 60)  # in seconds
    }

@router.get("/me", response_model=User)
async def read_users_me(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db)
):
    """
    Get current user information.
    """
    current_user = AuthService.get_current_user(token, db)
    return AuthService.get_current_active_user(current_user)

@router.post("/validate-token")
async def validate_token(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db)
):
    """
    Validate if the current token is still valid.
    """
    try:
        current_user = AuthService.get_current_user(token, db)
        AuthService.get_current_active_user(current_user)
        return {"valid": True, "message": "Token is valid"}
    except HTTPException:
        return {"valid": False, "message": "Token is invalid or expired"}

# Future SSO endpoints
@router.get("/sso/login")
async def sso_login(redirect_uri: str):
    """
    Initiate SSO login flow.
    This endpoint will be implemented when transitioning to SSO.
    """
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="SSO authentication is not yet implemented"
    )

@router.post("/sso/callback")
async def sso_callback(code: str, state: str):
    """
    Handle SSO callback.
    This endpoint will be implemented when transitioning to SSO.
    """
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="SSO authentication is not yet implemented"
    ) 