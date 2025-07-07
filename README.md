# PetroDash API

**FastAPI-based REST API for PetroEnergy's data warehouse analytics and dashboard management system.**

![Python](https://img.shields.io/badge/python-3.12%2B-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.68%2B-green.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-blue.svg)
![License](https://img.shields.io/badge/license-MIT-blue.svg)

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Configuration](#configuration)
- [API Documentation](#api-documentation)
- [Authentication](#authentication)
- [Database](#database)
- [Deployment](#deployment)
- [Contributing](#contributing)
- [License](#license)

## 🎯 Overview

PetroDash API is a comprehensive data analytics and management system designed for petroleum energy companies. It provides real-time analytics, data visualization, and management capabilities for:

- **Energy Generation & Production Analytics**
- **Environmental Impact Monitoring**
- **Human Resources Management**
- **Economic Performance Tracking**
- **Corporate Social Responsibility (CSR) Management**
- **Reference Data Management**

## ✨ Features

### 🔐 Authentication & Authorization
- JWT-based authentication with role-based access control
- OAuth2 with password bearer tokens
- Multi-level user roles (R02, R03, R04, R05)
- Session management and token validation

### 📊 Analytics & Dashboards
- Real-time energy generation analytics
- Environmental impact monitoring
- HR performance metrics
- Economic KPIs and financial analytics
- Interactive data visualizations

### 📈 Data Management
- Bulk data upload and processing
- Excel template generation and validation
- Data quality checks and validation
- Audit trail for all data operations

### 🌱 Environmental Monitoring
- Water abstraction, discharge, and consumption tracking
- Waste management (hazardous and non-hazardous)
- Energy consumption monitoring (diesel, electricity)
- CO2 emission tracking and equivalence calculations

### 👥 Human Resources
- Employee demographics and tenure tracking
- Safety metrics and occupational health
- Training records and certification management
- Parental leave and benefits tracking

## 🏗️ Architecture

### Project Structure
```
PetroDash-API/
├── app/
│   ├── __init__.py
│   ├── main.py                 # FastAPI application entry point
│   ├── dependencies.py         # Database and auth dependencies
│   ├── auth_decorators.py      # Role-based access decorators
│   ├── database.py             # Database configuration
│   │
│   ├── routers/                # API route handlers
│   │   ├── auth.py            # Authentication endpoints
│   │   ├── energy.py          # Energy analytics endpoints
│   │   ├── environment.py     # Environmental data endpoints
│   │   ├── environment_dash.py # Environmental dashboard
│   │   ├── hr.py              # Human resources endpoints
│   │   ├── economic.py        # Economic analytics endpoints
│   │   ├── csr.py            # CSR management endpoints
│   │   ├── reference.py       # Reference data endpoints
│   │   ├── account.py         # Account management
│   │   └── usable_apis.py     # Utility APIs
│   │
│   ├── services/              # Business logic services
│   │   ├── auth.py           # Authentication service
│   │   ├── audit_trail.py    # Audit logging service
│   │   └── file_handler.py   # File processing service
│   │
│   ├── models/                # Database models
│   ├── bronze/                # Data layer models and CRUD
│   ├── public/                # Public schema models
│   ├── reference/             # Reference data models
│   ├── crud/                  # Base CRUD operations
│   ├── template/              # Template configurations
│   └── utils/                 # Utility functions
│
├── requirements.txt            # Python dependencies
├── AUTH_README.md             # Authentication documentation
└── README.md                  # This file
```

### Technology Stack
- **Backend Framework**: FastAPI 0.68+
- **Database**: PostgreSQL with SQLAlchemy ORM
- **Authentication**: JWT tokens with PassLib/BCrypt
- **File Processing**: Pandas, OpenPyXL for Excel operations
- **API Documentation**: Automatic OpenAPI/Swagger generation
- **CORS**: Configurable cross-origin resource sharing

## 🚀 Installation

### Prerequisites
- Python 3.12 or higher
- PostgreSQL database
- Git

### Setup Instructions

1. **Clone the repository**
```bash
git clone https://github.com/your-org/PetroDash-API.git
cd PetroDash-API
```

2. **Create virtual environment**
```bash
# On Windows
python -m venv venv
venv\Scripts\activate

# On Unix/Linux/MacOS
python3 -m venv venv
source venv/bin/activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Environment Configuration**
Create a `.env` file in the root directory:
```env
DATABASE_URL=postgresql://username:password@localhost:5432/petrodash_db
SECRET_KEY=your-secret-key-here
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30
```

5. **Database Setup**
```bash
# Create database tables (ensure PostgreSQL is running)
# Run your database migration scripts here
```

6. **Run the application**
```bash
uvicorn app.main:app --reload
```

The API will be available at `http://localhost:8000`

## ⚙️ Configuration

### Environment Variables
| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | Required |
| `SECRET_KEY` | JWT secret key | Required |
| `ALGORITHM` | JWT algorithm | `HS256` |
| `ACCESS_TOKEN_EXPIRE_MINUTES` | Token expiration time | `30` |

### Database Configuration
The application uses PostgreSQL with multiple schemas:
- **public**: Core system tables (users, roles, status)
- **ref**: Reference data (companies, types, categories)
- **bronze**: Raw data layer
- **silver**: Processed data layer
- **gold**: Analytics and aggregated data

## 📚 API Documentation

### Interactive Documentation
Once the server is running, access the interactive API documentation:

- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`
- **OpenAPI JSON**: `http://localhost:8000/openapi.json`

### Main API Endpoints

#### Authentication
- `POST /auth/token` - Get access token
- `GET /auth/me` - Get current user info
- `POST /auth/validate-token` - Validate token

#### Energy Analytics
- `GET /energy/energy_dashboard` - Energy generation dashboard
- `GET /energy/fact_energy` - Energy fact table data
- `GET /energy/fund_allocation_dashboard` - Fund allocation analytics
- `POST /energy/upload_energy_file` - Upload energy data
- `GET /energy/download_template` - Download Excel template

#### Environment Monitoring
- `GET /environment/water_abstraction` - Water abstraction data
- `GET /environment/diesel_consumption` - Diesel consumption metrics
- `GET /environment/electric_consumption` - Electricity consumption
- `GET /environment_dash/abstraction` - Water dashboard metrics
- `GET /environment_dash/elec-pie-chart` - Electricity pie charts

#### Human Resources
- `GET /hr/total_safety_manhours` - Safety manhours analytics
- `GET /hr/employee_count_per_company` - Employee statistics
- `GET /hr/gender_distribution_per_position` - Gender analytics
- `POST /hr/bulk_upload_employability` - Bulk employee data upload

#### Economic Analytics
- `GET /economic/expenditure_analysis` - Expenditure analytics
- `GET /economic/financial_performance` - Financial KPIs
- `POST /economic/upload_financial_data` - Upload financial data

#### CSR Management
- `GET /help/investments-per-company` - CSR investments by company
- `GET /help/investments-per-project` - CSR project investments
- `POST /help/activities-update` - Update CSR activities

#### Reference Data
- `GET /reference/companies` - Company master data
- `GET /reference/system-health` - System health metrics
- `GET /reference/kpi-data` - KPI reference data

### Response Format
All API responses follow a consistent format:
```json
{
  "data": [...],
  "message": "Success",
  "status_code": 200,
  "timestamp": "2025-01-07T10:30:00Z"
}
```

### Error Handling
Error responses include detailed information:
```json
{
  "detail": "Error description",
  "status_code": 400,
  "timestamp": "2025-01-07T10:30:00Z"
}
```

## 🔐 Authentication

The API uses JWT-based authentication with role-based access control. See [AUTH_README.md](AUTH_README.md) for detailed authentication documentation.

### User Roles
- **R02**: Data Viewer - Read-only access to dashboards
- **R03**: Data Analyst - Read access + basic analytics
- **R04**: Data Manager - Read/Write access to data
- **R05**: System Administrator - Full access to all features

### Authentication Flow
1. **Login**: `POST /auth/token` with credentials
2. **Get Token**: Receive JWT access token
3. **Use Token**: Include in Authorization header: `Bearer <token>`
4. **Refresh**: Token expires after configured time

## 🗄️ Database

### Schema Design
The database follows a medallion architecture:

#### Bronze Layer (Raw Data)
- Direct data ingestion from various sources
- Minimal transformation
- Preserves original data structure

#### Silver Layer (Processed Data)
- Cleaned and validated data
- Business rules applied
- Standardized formats

#### Gold Layer (Analytics)
- Aggregated data for reporting
- Pre-calculated metrics
- Optimized for dashboard queries

### Key Tables
- `ref.company_main` - Company master data
- `bronze.energy_records` - Raw energy generation data
- `silver.hr_demographics` - Processed HR data
- `gold.fact_energy_generated` - Aggregated energy metrics

## 🚀 Deployment

### Production Deployment

1. **Environment Setup**
```bash
# Set production environment variables
export DATABASE_URL="postgresql://user:pass@prod-db:5432/petrodash"
export SECRET_KEY="your-production-secret-key"
```

2. **Use Production WSGI Server**
```bash
# Install Gunicorn
pip install gunicorn

# Run with Gunicorn
gunicorn app.main:app -w 4 -k uvicorn.workers.UvicornWorker
```

3. **Docker Deployment**
```dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
CMD ["gunicorn", "app.main:app", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000"]
```

4. **Reverse Proxy (Nginx)**
```nginx
server {
    listen 80;
    server_name your-domain.com;
    
    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

### Performance Optimization
- Use connection pooling for database connections
- Implement Redis for caching frequently accessed data
- Enable gzip compression for API responses
- Use CDN for static assets

## 🤝 Contributing

### Development Setup
1. Fork the repository
2. Create a feature branch: `git checkout -b feature/new-feature`
3. Make your changes
4. Write tests for new functionality
5. Submit a pull request

### Code Style
- Follow PEP 8 Python style guidelines
- Use type hints for function parameters and return values
- Add docstrings for all public methods
- Maintain consistent naming conventions

### Testing
```bash
# Run tests
pytest tests/

# Run with coverage
pytest --cov=app tests/
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📞 Support

For questions or support, please contact:
- **Email**: support@petroenergy.com
- **Documentation**: [API Docs](http://localhost:8000/docs)
- **Issues**: [GitHub Issues](https://github.com/your-org/PetroDash-API/issues)

---

**Last Updated**: January 7, 2025
**Version**: 1.0.0
