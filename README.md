# bcn-bicing-api
API that provides the analytical layer for the Barcelona Bicing system. It processes both real-time data and historical Parquet files to provide comprehensive insights about the bicycle sharing service.

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

## Data Sources

The API works with two types of data:
- Real-time data from the Bicing API
- Historical data stored in Parquet files for efficient data analysis and querying

## Running the API

To run the API, use the following command:
   ```
   uvicorn app:app --reload
   ```

The API will be available at http://localhost:8000.

## Documentation

You can access the interactive API documentation at `http://localhost:8000/docs` after starting the server.

## Data Processing

The system handles:
- Real-time station status and availability
- Historical trends and patterns from Parquet files
- Statistical analysis of usage patterns