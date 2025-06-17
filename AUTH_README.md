# PetroDash API Authentication System

This document explains how to use the OAuth2 with password bearer authentication system in the PetroDash API.

## Architecture Overview

The authentication system is designed with separation of concerns and role-based access control:

### 📁 `app/services/auth.py` - **Business Logic Layer**

- Contains the `AuthService` class with core authentication logic
- Handles JWT token creation/verification, user authentication
- Pure business logic, no FastAPI dependencies
- Works directly with your database
- Simplified password handling ready for SSO migration

### 📁 `app/routers/auth.py` - **API Endpoints Layer**

- Contains FastAPI route handlers (`/auth/token`, `/auth/me`, `/auth/validate-token`)
- Handles HTTP requests/responses and validation
- Uses `AuthService` for the actual authentication work
- Translates between HTTP and business logic

### 📁 `app/dependencies.py` - **Database Dependencies**

- Contains the `get_db()` dependency for database session management
- Used with `Depends()` in route handlers that need database access
- Authentication and role-based access control is handled by decorators in `auth_decorators.py`

## Required Libraries

- **`pyjwt`** - JWT (JSON Web Token) creation and verification (as recommended by FastAPI docs)
- **`python-multipart`** - For handling OAuth2 password form data

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Environment Configuration

Create a `.env` file in the root directory with the following variables:

```env
# Authentication Configuration
SECRET_KEY=9f8a7b6c5d4e3f2g1h0i9j8k7l6m5n4o3p2q1r0s9t8u7v6w5x4y3z2a1b0c9d8e7f6
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30

# Database Configuration
DATABASE_URL=postgresql://username:password@localhost/database_name

# Environment
ENVIRONMENT=development

# CORS Settings
ALLOWED_ORIGINS=http://localhost:3000,http://localhost:5173
```

**⚠️ IMPORTANT:** Generate a secure SECRET_KEY for production:

```bash
# Generate a secure secret key
openssl rand -hex 32
```

### 3. Database Integration

The system integrates with your existing `accounts` table. Make sure your table has these columns:

- `email` - User's email address (used as username)
- `password` - Password (will be replaced by SSO tokens)
- `account_id` - Unique account identifier
- `account_role` - User role (R01=admin, R02=user, etc.)
- `account_status` - Account status ('active', 'inactive', etc.)

**Note:** If your table structure differs, update the SQL query in `AuthService.get_user()` method in `app/services/auth.py`.

### 4. Test User Setup

Since we're preparing for SSO migration, passwords are stored as plain text for now:

**⚠️ Note:** This is temporary for development. In production with SSO, the password field will be replaced by SSO tokens.

## API Endpoints

### Authentication Endpoints

#### Login

```http
POST /auth/token
Content-Type: application/x-www-form-urlencoded

username=user@example.com&password=yourpassword
```

Response:

```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer",
  "expires_in": 1800
}
```

#### Get Current User

```http
GET /auth/me
Authorization: Bearer your-jwt-token-here
```

Response:

```json
{
  "email": "user@example.com",
  "full_name": "user@example.com",
  "disabled": false,
  "roles": ["R02"]
}
```

#### Validate Token

```http
POST /auth/validate-token
Authorization: Bearer your-jwt-token-here
```

Response:

```json
{
  "valid": true,
  "message": "Token is valid"
}
```

## Using Authentication in Your Code

### Basic Authentication

For endpoints that only need to verify the user is authenticated (no role requirements), you can create custom dependencies or use the auth decorators with minimal roles. Since we've simplified the system, most endpoints will use role-based decorators:

```python
from fastapi import APIRouter, Depends
from app.dependencies import get_db
from app.services.auth import AuthService, oauth2_scheme
from sqlalchemy.orm import Session

router = APIRouter()

# Custom authentication dependency if needed
async def get_authenticated_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    current_user = AuthService.get_current_user(token, db)
    return AuthService.get_current_active_user(current_user)

# Any authenticated user
@router.get("/profile")
async def get_profile(current_user = Depends(get_authenticated_user)):
    return {"user": current_user.username, "roles": current_user.roles}

# Or more commonly, just use role decorators for all endpoints
from app.auth_decorators import require_role

@router.get("/dashboard")
@require_role("R01", "R02", "R03", "R04", "R05")  # All authenticated users
def get_dashboard():
    return {"message": "Welcome to dashboard"}
```

### Role-Based Access Control

For role-based access control, use the decorators from `auth_decorators.py`:

```python
from app.auth_decorators import (
    require_role,
    system_admin_only,
    executive_only,
    office_checker_only,
    site_checker_only,
    encoder_only
)

# 1:1 Role mapping decorators - exact role match only
@router.get("/admin/config")
@system_admin_only  # Only R01 users
def get_system_config():
    return {"config": "system data"}

@router.get("/reports/executive")
@executive_only  # Only R02 users
def get_executive_reports():
    return {"reports": "executive data"}

@router.get("/office/data")
@office_checker_only  # Only R03 users
def get_office_data():
    return {"data": "office data"}

# Custom role combinations - specify exactly which roles are allowed
@router.get("/economic/dashboard")
@require_role("R01", "R02")  # System Admin OR Executive
def get_dashboard():
    return {"data": "dashboard data"}

@router.get("/data/analysis")
@require_role("R02", "R03", "R04")  # Executive OR Office Checker OR Site Checker
def get_analysis():
    return {"data": "analysis data"}
```

### Role System (No Hierarchy)

The role system uses **exact role matching** with no hierarchy:

- **R01 - System Administrator**: Full system access
- **R02 - Executive**: Executive-level access
- **R03 - Office Checker**: Office-level checking privileges
- **R04 - Site Checker**: Site-level checking privileges
- **R05 - Encoder**: Data entry privileges

**Important:**

- `@system_admin_only` allows **ONLY** R01 users
- `@executive_only` allows **ONLY** R02 users
- `@require_role("R01", "R03")` allows **ONLY** R01 OR R03 users (not R02)
- If you need multiple roles, use `@require_role("R01", "R02", "R03")`

## SSO Migration Path

The authentication system is designed for easy migration to SSO. When ready to implement SSO:

1. **Update Environment Variables:**

   ```env
   SSO_PROVIDER=azure_ad  # or okta, google, saml
   SSO_CLIENT_ID=your-sso-client-id
   SSO_CLIENT_SECRET=your-sso-client-secret
   SSO_TENANT_ID=your-tenant-id
   SSO_REDIRECT_URI=http://localhost:8000/auth/sso/callback
   ```

2. **Implement SSO Service:**

   - The `SSOAuthService` class in `app/services/auth.py` provides the interface
   - Implement the methods for your specific SSO provider

3. **Update Authentication Flow:**
   - Frontend redirects to `/auth/sso/login`
   - SSO provider handles authentication
   - Callback handled at `/auth/sso/callback`
   - JWT tokens continue to work the same way

## Security Considerations

1. **Change the SECRET_KEY:** Never use the default secret key in production
2. **Use HTTPS:** Always use HTTPS in production for token security
3. **Token Expiration:** Tokens expire after 30 minutes by default
4. **SSO Ready:** Current password handling is temporary for easy SSO migration
5. **CORS:** Configure ALLOWED_ORIGINS properly for your frontend domains
