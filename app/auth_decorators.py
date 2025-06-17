from functools import wraps
from fastapi import Depends, HTTPException, status
from sqlalchemy.orm import Session
from .dependencies import get_db
from .services.auth import AuthService, User, oauth2_scheme

def require_role(*allowed_roles):
    """
    Decorator that requires specific roles to access an endpoint.
    
    Usage:
        @require_role("R01")  # System Admin only
        @require_role("R01", "R02")  # System Admin or Executive
        @require_role("R01", "R02", "R03")  # System Admin, Executive, or Head Office Checker
    
    Args:
        *allowed_roles: Variable number of role IDs that are allowed to access the endpoint
    """
    def decorator(func):
        # Get the original function signature
        import inspect
        sig = inspect.signature(func)
        
        # Check if function already has current_user parameter
        has_current_user = any(
            param.name == 'current_user' for param in sig.parameters.values()
        )
        
        if has_current_user:
            # Function already has current_user parameter, just add role checking
            @wraps(func)
            async def wrapper(*args, **kwargs):
                # Get current_user from kwargs
                current_user = kwargs.get('current_user')
                if not current_user:
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="Authentication required"
                    )
                
                # Check if user has required role
                if not any(role in current_user.roles for role in allowed_roles):
                    role_names = {
                        "R01": "System Administrator",
                        "R02": "Executive", 
                        "R03": "Head Office-Level Checker",
                        "R04": "Site-Level Checker",
                        "R05": "Encoder"
                    }
                    allowed_names = [role_names.get(role, role) for role in allowed_roles]
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail=f"Access denied. Required roles: {', '.join(allowed_names)}"
                    )
                
                return await func(*args, **kwargs) if inspect.iscoroutinefunction(func) else func(*args, **kwargs)
            
            return wrapper
        else:
            # Function doesn't have current_user parameter, add it
            @wraps(func)
            async def wrapper(*args, token: str = Depends(oauth2_scheme), db: Session = Depends(get_db), **kwargs):
                # Get current user directly from AuthService
                current_user = AuthService.get_current_user(token, db)
                current_user = AuthService.get_current_active_user(current_user)
                
                # Check if user has required role
                if not any(role in current_user.roles for role in allowed_roles):
                    role_names = {
                        "R01": "System Administrator",
                        "R02": "Executive",
                        "R03": "Head Office-Level Checker", 
                        "R04": "Site-Level Checker",
                        "R05": "Encoder"
                    }
                    allowed_names = [role_names.get(role, role) for role in allowed_roles]
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail=f"Access denied. Required roles: {', '.join(allowed_names)}"
                    )
                
                # Add current_user to kwargs for the function
                kwargs['current_user'] = current_user
                return await func(*args, **kwargs) if inspect.iscoroutinefunction(func) else func(*args, **kwargs)
            
            return wrapper
    
    return decorator

# Convenience decorators for specific roles (1:1 mapping)
def system_admin_only(func):
    """Decorator for System Administrator only access (R01)"""
    return require_role("R01")(func)

def executive_only(func):
    """Decorator for Executive only access (R02)"""
    return require_role("R02")(func)

def office_checker_only(func):
    """Decorator for Head Office Checker only access (R03)"""
    return require_role("R03")(func)

def site_checker_only(func):
    """Decorator for Site Checker only access (R04)"""
    return require_role("R04")(func)

def encoder_only(func):
    """Decorator for Encoder only access (R05)"""
    return require_role("R05")(func)