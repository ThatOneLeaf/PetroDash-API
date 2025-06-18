from functools import wraps
from fastapi import Depends, HTTPException, status
from sqlalchemy.orm import Session
from .dependencies import get_db
from .services.auth import AuthService, User, oauth2_scheme

def get_current_user_with_roles(*required_roles):
    """
    Dependency that gets current user and checks for required roles.
    """
    def dependency(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
        print(f"[ROLE DEBUG] Checking roles for endpoint, required: {required_roles}")
        
        # Get current user
        current_user = AuthService.get_current_user(token, db)
        current_user = AuthService.get_current_active_user(current_user)
        
        print(f"[ROLE DEBUG] User: {current_user.email}, User roles: {current_user.roles}")
        
        # Check if user has required role
        has_access = any(role in current_user.roles for role in required_roles)
        print(f"[ROLE DEBUG] Access check - Has access: {has_access}")
        
        if not has_access:
            role_names = {
                "R01": "System Administrator",
                "R02": "Executive",
                "R03": "Head Office-Level Checker", 
                "R04": "Site-Level Checker",
                "R05": "Encoder"
            }
            allowed_names = [role_names.get(role, role) for role in required_roles]
            print(f"[ROLE DEBUG] Access denied for user {current_user.email}")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access denied. Required roles: {', '.join(allowed_names)}"
            )
        
        print(f"[ROLE DEBUG] Access granted for user {current_user.email}")
        return current_user
    
    return dependency

# Create specific dependency functions for common role combinations
def require_role(*allowed_roles):
    """
    Decorator that creates a dependency for role checking.
    """
    return Depends(get_current_user_with_roles(*allowed_roles))

# Convenience decorators for specific roles
def system_admin_only(func):
    """Decorator for System Administrator only access (R01)"""
    @wraps(func)
    def wrapper(*args, current_user: User = Depends(get_current_user_with_roles("R01")), **kwargs):
        return func(*args, **kwargs)
    return wrapper

def executive_only(func):
    """Decorator for Executive only access (R02)"""
    @wraps(func)
    def wrapper(*args, current_user: User = Depends(get_current_user_with_roles("R02")), **kwargs):
        return func(*args, **kwargs)
    return wrapper

def office_checker_only(func):
    """Decorator for Head Office Checker only access (R03)"""
    @wraps(func)
    def wrapper(*args, current_user: User = Depends(get_current_user_with_roles("R03")), **kwargs):
        return func(*args, **kwargs)
    return wrapper

def site_checker_only(func):
    """Decorator for Site Checker only access (R04)"""
    @wraps(func)
    def wrapper(*args, current_user: User = Depends(get_current_user_with_roles("R04")), **kwargs):
        return func(*args, **kwargs)
    return wrapper

def encoder_only(func):
    """Decorator for Encoder only access (R05)"""
    @wraps(func)
    def wrapper(*args, current_user: User = Depends(get_current_user_with_roles("R05")), **kwargs):
        return func(*args, **kwargs)
    return wrapper