from functools import wraps
import asyncio
from fastapi import Depends, HTTPException, status
from sqlalchemy.orm import Session
from .dependencies import get_db
from .services.auth import AuthService, User, oauth2_scheme

def get_user_info(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db)
):
    """
    Dependency that gets current user information.
    """
    current_user = AuthService.get_current_user(token, db)
    current_user = AuthService.get_current_active_user(current_user)
    return current_user

def get_current_user_with_roles(*required_roles):
    """
    Dependency that gets current user and checks for required roles.
    """
    def dependency(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
        # Get current user
        current_user = AuthService.get_current_user(token, db)
        current_user = AuthService.get_current_active_user(current_user)
        
        # Check if user has required role
        has_access = any(role in current_user.roles for role in required_roles)
        
        if not has_access:
            role_names = {
                "R01": "System Administrator",
                "R02": "Executive",
                "R03": "Head Office-Level Checker", 
                "R04": "Site-Level Checker",
                "R05": "Encoder"
            }
            allowed_names = [role_names.get(role, role) for role in required_roles]
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access denied. Required roles: {', '.join(allowed_names)}"
            )
        
        return current_user
    
    return dependency

# Create specific dependency functions for common role combinations
def require_role(*allowed_roles):
    """
    Decorator that creates a dependency for role checking.
    """
    return Depends(get_current_user_with_roles(*allowed_roles))

# Helper function to create role-based decorators that handle both sync and async functions
def create_role_decorator(role_id: str):
    """Create a decorator for a specific role that handles both sync and async functions"""
    def decorator(func):
        if asyncio.iscoroutinefunction(func):
            # Handle async functions
            @wraps(func)
            async def async_wrapper(*args, current_user: User = Depends(get_current_user_with_roles(role_id)), **kwargs):
                return await func(*args, **kwargs)
            return async_wrapper
        else:
            # Handle sync functions
            @wraps(func)
            def sync_wrapper(*args, current_user: User = Depends(get_current_user_with_roles(role_id)), **kwargs):
                return func(*args, **kwargs)
            return sync_wrapper
    return decorator

def allow_roles(*role_ids):
    """Decorator for allowing multiple roles access"""
    def decorator(func):
        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, current_user: User = Depends(get_current_user_with_roles(*role_ids)), **kwargs):
                return await func(*args, **kwargs)
            return async_wrapper
        else:
            @wraps(func)
            def sync_wrapper(*args, current_user: User = Depends(get_current_user_with_roles(*role_ids)), **kwargs):
                return func(*args, **kwargs)
            return sync_wrapper
    return decorator

# Convenience decorators for specific roles
def system_admin_only(func):
    """Decorator for System Administrator only access (R01)"""
    return create_role_decorator("R01")(func)

def executive_only(func):
    """Decorator for Executive only access (R02)"""
    return create_role_decorator("R02")(func)

def office_checker_only(func):
    """Decorator for Head Office Checker only access (R03)"""
    return create_role_decorator("R03")(func)

def site_checker_only(func):
    """Decorator for Site Checker only access (R04)"""
    return create_role_decorator("R04")(func)

def encoder_only(func):
    """Decorator for Encoder only access (R05)"""
    return create_role_decorator("R05")(func)