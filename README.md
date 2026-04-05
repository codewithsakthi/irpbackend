# Backend

This is the backend API for the Spark project.

## Getting Started

### Prerequisites
- Python 3.8+
- pip

### Installation
```bash
pip install -r requirements.txt
```

### Development
```bash
# Run database migrations
alembic upgrade head

# Start the development server
python -m app.main
```

### Testing
```bash
python -m pytest
```