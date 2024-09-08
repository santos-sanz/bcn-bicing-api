# bcn-bicing-api
API that provides the analytical layer for the Barcelona Bicing system

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/your-username/bcn-bicing-api.git
   cd bcn-bicing-api
   ```

2. Create a virtual environment (optional but recommended):
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows use: venv\Scripts\activate
   ```

3. Install the dependencies:
   ```
   pip install -r requirements.txt
   ```

## Running the API

To run the API, use the following command:
   ```
   uvicorn app:app --reload
   ```

The API will be available at http://localhost:8000.

## Documentation

You can access the interactive API documentation at `http://localhost:8000/docs` after starting the server.