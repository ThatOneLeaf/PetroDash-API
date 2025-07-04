from datetime import datetime, timedelta
from typing import Optional, Union
import jwt
from fastapi import HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from passlib.context import CryptContext
import time

# Password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Configuration
import os
from dotenv import load_dotenv

load_dotenv()

SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-here-change-in-production")
ALGORITHM = os.getenv("ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_HOURS = 24  # Absolute expiration time in hours
INACTIVITY_TIMEOUT_MINUTES = 60  # Sliding window timeout in minutes (1 hour)

# OAuth2 scheme
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/token")

# In-memory session store (replace with Redis in production)
active_sessions = {}

# Models
class Token(BaseModel):
    access_token: str
    token_type: str
    expires_in: int
    absolute_expiry: int

class TokenData(BaseModel):
    username: Optional[str] = None
    scopes: list[str] = []
    last_activity: float = None

class User(BaseModel):
    username: str
    email: str
    full_name: Optional[str] = None
    disabled: Optional[bool] = False
    roles: list[str] = []
    account_id: Optional[str] = None
    power_plant_id: Optional[str] = None
    company_id: Optional[str] = None
    account_status: Optional[str] = None
    date_created: Optional[datetime] = None
    date_updated: Optional[datetime] = None

class UserInDB(User):
    hashed_password: str

# Database imports
from sqlalchemy.orm import Session
from sqlalchemy import text

class AuthService:
    """
    Authentication service that abstracts authentication logic.
    This design makes it easy to switch between different auth providers (local, SSO, etc.)
    """
    
    @staticmethod
    def verify_password(password: str, hashed_password: str) -> bool:
        """Verify a password against its bcrypt hash."""
        return pwd_context.verify(password, hashed_password)
    
    @staticmethod
    def get_password_hash(password: str) -> str:
        """Generate hash for a password."""
        return pwd_context.hash(password)
    
    @staticmethod
    def get_user(email: str, db: Session) -> Optional[UserInDB]:
        """Get user from database by email with all user details."""
        try:
            query = text("""
                SELECT 
                    email as username,
                    email,
                    account_id,
                    account_role as roles,
                    account_status,
                    power_plant_id,
                    company_id,
                    date_created,
                    date_updated,
                    password as hashed_password
                FROM account 
                WHERE email = :email AND account_status = 'active'
                LIMIT 1
            """)
            
            result = db.execute(query, {"email": email}).fetchone()
            
            if result:
                user_data = {
                    "username": result.email,
                    "email": result.email,
                    "full_name": result.email,
                    "hashed_password": result.hashed_password,
                    "disabled": result.account_status != 'active',
                    "roles": [result.roles] if result.roles else ["user"],
                    "account_id": result.account_id,
                    "power_plant_id": result.power_plant_id,
                    "company_id": result.company_id,
                    "account_status": result.account_status,
                    "date_created": result.date_created,
                    "date_updated": result.date_updated
                }
                return UserInDB(**user_data)
            
            return None
            
        except Exception as e:
            print(f"Database error in get_user: {str(e)}")
            return None
    
    @staticmethod
    def authenticate_user(email: str, password: str, db: Session) -> Union[UserInDB, bool]:
        """
        Authenticate a user with email and password using bcrypt password hashing.
        """
        user = AuthService.get_user(email, db)
        if not user:
            return False
        
        if not AuthService.verify_password(password, user.hashed_password):
            return False
        
        return user
    
    @staticmethod
    def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
        """Create a JWT access token with both absolute and sliding expiration."""
        to_encode = data.copy()
        
        # Set absolute expiration (24 hours)
        absolute_expire = datetime.utcnow() + timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS)
        
        # Set last activity timestamp
        last_activity = time.time()
        
        to_encode.update({
            "exp": absolute_expire,
            "last_activity": last_activity
        })
        
        encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
        
        # Store session info
        active_sessions[encoded_jwt] = {
            "last_activity": last_activity,
            "username": data.get("sub")
        }
        
        return encoded_jwt
    
    @staticmethod
    def verify_token(token: str, db: Session = None) -> TokenData:
        """Verify and decode a JWT token with activity check."""
        credentials_exception = HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
        
        inactivity_exception = HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session expired due to inactivity",
            headers={"WWW-Authenticate": "Bearer"},
        )
        
        try:
            # Decode token first to get username
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            username: str = payload.get("sub")
            if username is None:
                raise credentials_exception
            
            # Check if session exists and is active
            session = active_sessions.get(token)
            if not session:
                if db:
                    # Get user info before removing session
                    user = AuthService.get_user(username, db)
                    if user:
                        from app.services.audit_trail import append_audit_trail
                        append_audit_trail(
                            db=db,
                            account_id=str(user.account_id),
                            target_table="account",
                            record_id=username,
                            action_type="logout",
                            old_value="",
                            new_value="expired",
                            description=f"Session expired for user {username}"
                        )
                raise credentials_exception
            
            # Check for inactivity timeout
            current_time = time.time()
            if current_time - session["last_activity"] > INACTIVITY_TIMEOUT_MINUTES * 60:
                # Remove expired session
                active_sessions.pop(token, None)
                if db:
                    # Get user info before removing session
                    user = AuthService.get_user(username, db)
                    if user:
                        from app.services.audit_trail import append_audit_trail
                        append_audit_trail(
                            db=db,
                            account_id=str(user.account_id),
                            target_table="account",
                            record_id=username,
                            action_type="logout",
                            old_value="",
                            new_value="inactive",
                            description=f"Session expired due to inactivity for user {username}"
                        )
                raise inactivity_exception
            
            # Update last activity time
            session["last_activity"] = current_time
            
            token_data = TokenData(
                username=username,
                last_activity=session["last_activity"]
            )
            return token_data
            
        except jwt.ExpiredSignatureError:
            # Remove expired session
            active_sessions.pop(token, None)
            raise credentials_exception
        except jwt.PyJWTError as e:
            raise credentials_exception
    
    @staticmethod
    def get_current_user(token: str, db: Session) -> User:
        """Get the current user from a JWT token."""
        token_data = AuthService.verify_token(token)
        user = AuthService.get_user(email=token_data.username, db=db)
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return User(**user.dict())
    
    @staticmethod
    def get_current_active_user(user: User) -> User:
        """Ensure the current user is active."""
        if user.disabled:
            raise HTTPException(status_code=400, detail="Inactive user")
        return user

# Future SSO Integration Interface
class SSOAuthService:
    """
    Interface for SSO authentication services (SAML, OIDC, etc.)
    Implement this interface when transitioning to SSO
    """
    
    @staticmethod
    def validate_sso_token(token: str) -> User:
        """Validate SSO token and return user info."""
        # TODO: Implement SSO token validation
        raise NotImplementedError("SSO authentication not yet implemented")
    
    @staticmethod
    def get_sso_login_url(redirect_uri: str) -> str:
        """Get SSO login URL."""
        # TODO: Implement SSO login URL generation
        raise NotImplementedError("SSO authentication not yet implemented")
    
    @staticmethod
    def handle_sso_callback(code: str, state: str) -> User:
        """Handle SSO callback and return user info."""
        # TODO: Implement SSO callback handling
        raise NotImplementedError("SSO authentication not yet implemented")
