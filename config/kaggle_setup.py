import os
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load environment variables from .env file
load_dotenv()

# Access the Kaggle API key
kaggle_username = os.getenv("KAGGLE_USERNAME")
kaggle_key = os.getenv("KAGGLE_KEY")

if not kaggle_username or not kaggle_key:
    logging.error("Kaggle API credentials are not set properly.")
    raise EnvironmentError("Kaggle API credentials are not set properly.")

logging.info(f"Kaggle Username: {kaggle_username}")
