# PetroDash-API
FastAPI-based REST API for PetroEnergy's data warehouse analytics.

# Create virtual environment
# on Windows
python -m venv venv
# on Lunix
python3 -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On Unix or MacOS:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run the server

uvicorn app.main:app --reload
