from datetime import datetime, timedelta
from typing import Optional, Union
import jwt
from fastapi import HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel

# Configuration
import os
from dotenv import load_dotenv

load_dotenv()

SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-here-change-in-production")
ALGORITHM = os.getenv("ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = float(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))
#ACCESS_TOKEN_EXPIRE_MINUTES = float(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "0.5"))  # 30 seconds for testing

# OAuth2 scheme
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/token")

# Models
class Token(BaseModel):
    access_token: str
    token_type: str
    expires_in: int

class TokenData(BaseModel):
    username: Optional[str] = None
    scopes: list[str] = []

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
    def get_user(email: str, db: Session) -> Optional[UserInDB]:
        """Get user from database by email with all user details."""
        try:
            print(f"[AUTH DEBUG] Looking up user: {email}")
            # Query accounts table to get all user details
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
                print(f"[AUTH DEBUG] User found - Email: {result.email}, Role: {result.roles}, Status: {result.account_status}")
                print(f"[AUTH DEBUG] Additional details - Account ID: {result.account_id}, Power Plant ID: {result.power_plant_id}, Company ID: {result.company_id}")
                
                # Convert result to dict and create UserInDB object with all details
                user_data = {
                    "username": result.email,
                    "email": result.email,
                    "full_name": result.email,  # You can join with user profile table if needed
                    "hashed_password": result.hashed_password,  # Keep for now, will be removed in SSO
                    "disabled": result.account_status != 'active',
                    "roles": [result.roles] if result.roles else ["user"],  # Convert single role to list
                    "account_id": result.account_id,
                    "power_plant_id": result.power_plant_id,
                    "company_id": result.company_id,
                    "account_status": result.account_status,
                    "date_created": result.date_created,
                    "date_updated": result.date_updated
                }
                print(f"[AUTH DEBUG] User data created - Roles: {user_data['roles']}, Disabled: {user_data['disabled']}")
                print(f"[AUTH DEBUG] Full user data: Account ID: {user_data['account_id']}, Power Plant: {user_data['power_plant_id']}, Company: {user_data['company_id']}")
                return UserInDB(**user_data)
            else:
                print(f"[AUTH DEBUG] User not found: {email}")
            
            return None
            
        except Exception as e:
            print(f"[AUTH DEBUG] Database error in get_user: {str(e)}")
            return None
    
    @staticmethod
    def authenticate_user(email: str, password: str, db: Session) -> Union[UserInDB, bool]:
        """
        Authenticate a user with email and password.
        For now, we'll do simple password comparison until SSO migration.
        In production, passwords should be hashed in the database.
        """
        user = AuthService.get_user(email, db)
        if not user:
            return False
        # Simple password check - replace with proper hashing in production if needed
        if password != user.hashed_password:
            return False
        return user
    
    @staticmethod
    def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
        """Create a JWT access token."""
        to_encode = data.copy()
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(minutes=15)
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
        return encoded_jwt
    
    @staticmethod
    def verify_token(token: str) -> TokenData:
        """Verify and decode a JWT token."""
        print(f"[AUTH DEBUG] Verifying token type: {type(token)}")
        credentials_exception = HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            username: str = payload.get("sub")
            if username is None:
                raise credentials_exception
            token_data = TokenData(username=username)
            print(f"[AUTH DEBUG] Token decoded successfully, username: {username}")
        except jwt.PyJWTError as e:
            print(f"[AUTH DEBUG] JWT decode error: {str(e)}")
            raise credentials_exception
        return token_data
    
    @staticmethod
    def get_current_user(token: str, db: Session) -> User:
        """Get the current user from a JWT token."""
        print(f"[AUTH DEBUG] Getting current user from token")
        token_data = AuthService.verify_token(token)
        print(f"[AUTH DEBUG] Token verified, username: {token_data.username}")
        user = AuthService.get_user(email=token_data.username, db=db)
        if user is None:
            print(f"[AUTH DEBUG] User not found for token username: {token_data.username}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )
        print(f"[AUTH DEBUG] Current user retrieved: {user.email}, roles: {user.roles}")
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
