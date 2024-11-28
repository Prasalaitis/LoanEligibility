import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

db_config = {
    "host": os.getenv("LOANS_DB_HOST"),
    "port": os.getenv("LOANS_DB_PORT"),
    "database": os.getenv("LOANS_DB_NAME"),
    "user": os.getenv("LOANS_DB_USER"),
    "password": os.getenv("LOANS_DB_PASSWORD"),
}
