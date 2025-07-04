from datetime import timedelta
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from app.services.auth import (
    AuthService, 
    Token, 
    User, 
    ACCESS_TOKEN_EXPIRE_HOURS,
    oauth2_scheme,
    active_sessions
)
from app.dependencies import get_db
from app.services.audit_trail import append_audit_trail
import time

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
    
    # Create token with absolute expiration (24 hours)
    access_token = AuthService.create_access_token(
        data={"sub": user.username}
    )
    
    # Calculate expiration timestamps
    current_time = int(time.time())
    absolute_expiry = current_time + (ACCESS_TOKEN_EXPIRE_HOURS * 3600)
    
    # Log successful login
    append_audit_trail(
        db=db,
        account_id=str(user.account_id),
        target_table="account",
        record_id=user.username,
        action_type="login",
        old_value="",
        new_value="success",
        description=f"User {user.username} logged in"
    )
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "expires_in": ACCESS_TOKEN_EXPIRE_HOURS * 3600,  # in seconds
        "absolute_expiry": absolute_expiry
    }

@router.post("/update-activity")
async def update_session_activity(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db)
):
    """
    Update the last activity timestamp for the current session.
    This keeps the session alive if the user is active.
    """
    try:
        # This will automatically update the last_activity timestamp
        token_data = AuthService.verify_token(token)
        session = active_sessions.get(token)
        
        if not session:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired session",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        return {
            "valid": True,
            "last_activity": session["last_activity"],
            "message": "Session activity updated successfully"
        }
        
    except HTTPException as e:
        return {
            "valid": False,
            "message": str(e.detail)
        }

@router.post("/logout")
async def logout(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db)
):
    """
    Invalidate the current session token.
    """
    try:
        # Get user info before removing session
        token_data = AuthService.verify_token(token)
        user = AuthService.get_user(token_data.username, db)
        
        if user:
            # Log manual logout first
            append_audit_trail(
                db=db,
                account_id=str(user.account_id),
                target_table="account",
                record_id=user.username,
                action_type="logout",
                old_value="",
                new_value="manual",
                description=f"User {user.username} manually logged out"
            )
            
            # Then remove session
            active_sessions.pop(token, None)
            
            return {"message": "Successfully logged out"}
        else:
            return {"message": "User not found"}
            
    except Exception as e:
        return {"message": "Error during logout", "error": str(e)}

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