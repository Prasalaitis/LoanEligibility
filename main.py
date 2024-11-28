import asyncio
from typing import Dict
import pandas as pd
from src.get_dataset import main as get_dataset_main
from src.push_dataset import push_dataset
from config import LoggerSetup


def setup_logger() -> LoggerSetup:
    """
    Sets up the logger for the main script.

    Returns:
        LoggerSetup: Configured logger setup instance.
    """
    return LoggerSetup(logger_name="MainScript")


async def main() -> None:
    """
    Main entry point for downloading the dataset and setting up the database.

    It downloads the dataset, loads it into DataFrames, and then sets up the
    database using the downloaded dataset.
    """
    logger = setup_logger().logger
    dataset_name = "vikasukani/loan-eligible-dataset"

    try:
        logger.info("Starting dataset download...")
        dataframes: Dict[str, pd.DataFrame] = await get_dataset_main(
            dataset_name
        )
        logger.info("Dataset downloaded and loaded into DataFrames.")

        logger.info("Starting database setup...")
        await push_dataset(dataframes)
        logger.info("Database setup completed.")

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())
