import aiohttp
import asyncio
import pandas as pd
from aiohttp import ClientError
from zipfile import ZipFile
from io import BytesIO
from typing import Dict, List, Optional
from config import kaggle_key, LoggerSetup


class DatasetDownloader:
    """
    Manages downloading datasets from Kaggle using asynchronous requests and handling the response.

    Attributes:
        dataset (str): The name of the dataset to download.
        retries (int): The number of retries if the download fails.
        delay (int): The delay between retries.
        file_extensions (List[str]): List of file extensions to extract from the downloaded ZIP.
        headers (Dict[str, str]): HTTP headers for the Kaggle API request.
        logger (logging.Logger): Logger instance for logging messages.
    """

    def __init__(
        self,
        dataset: str,
        retries: int = 3,
        delay: int = 5,
        file_extensions: Optional[List[str]] = None,
    ):
        """
        Initializes the DatasetDownloader with the given parameters.

        Args:
            dataset (str): The name of the dataset to download.
            retries (int, optional): Number of retries on failure. Defaults to 3.
            delay (int, optional): Delay between retries in seconds. Defaults to 5.
            file_extensions (Optional[List[str]], optional): List of file extensions to extract. Defaults to None.
        """
        self.dataset = dataset
        self.retries = retries
        self.delay = delay
        self.kaggle_api = (
            f"https://www.kaggle.com/api/v1/datasets/download/{self.dataset}"
        )
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {kaggle_key}",
        }
        self.file_extensions = file_extensions or [".xlsx", ".csv"]
        self.logger = LoggerSetup(logger_name="DatasetDownloader").logger

        if not kaggle_key:
            raise ValueError(
                "Kaggle API key not found. Ensure it is set in the environment variables."
            )

    async def fetch_dataset(self, session: aiohttp.ClientSession) -> bytes:
        """
        Fetches the dataset from Kaggle using the provided session.

        Args:
            session (aiohttp.ClientSession): The HTTP session to use for the request.

        Raises:
            Exception: If the dataset cannot be downloaded after the specified retries.

        Returns:
            bytes: The downloaded dataset content.
        """
        for attempt in range(self.retries):
            try:
                async with session.get(self.kaggle_api) as response:
                    response.raise_for_status()
                    return await response.read()
            except ClientError as e:
                self.logger.error(f"Request failed: {e}")
                if attempt < self.retries - 1:
                    self.logger.info(f"Retrying in {self.delay} seconds...")
                    await asyncio.sleep(self.delay)
        self.logger.error(
            f"Failed to download dataset after {self.retries} attempts."
        )
        raise Exception(
            f"Failed to download dataset after {self.retries} attempts."
        )

    async def download_dataset(self) -> bytes:
        """
        Downloads the dataset from Kaggle.

        Raises:
            Exception: If the dataset download fails.

        Returns:
            bytes: The downloaded dataset content.
        """
        async with aiohttp.ClientSession(headers=self.headers) as session:
            self.logger.info(f"Downloading dataset {self.dataset}...")
            data = await self.fetch_dataset(session)
            self.logger.info(
                f"Dataset {self.dataset} downloaded successfully."
            )
            return data

    @staticmethod
    def extract_files(
        zip_data: bytes, file_extensions: List[str]
    ) -> Dict[str, bytes]:
        """
        Extracts files with the specified extensions from the ZIP data.

        Args:
            zip_data (bytes): The ZIP file data.
            file_extensions (List[str]): List of file extensions to extract.

        Raises:
            Exception: If no target files are found in the ZIP archive.

        Returns:
            Dict[str, bytes]: A dictionary of filenames and their content.
        """
        with ZipFile(BytesIO(zip_data)) as zip_file:
            files = {
                f: zip_file.read(f)
                for f in zip_file.namelist()
                if any(f.endswith(ext) for ext in file_extensions)
            }
            if not files:
                raise Exception("No target files found in the ZIP archive.")
        return files

    @staticmethod
    def load_dataframes(files: Dict[str, bytes]) -> Dict[str, pd.DataFrame]:
        """
        Loads the extracted files into pandas DataFrames.

        Args:
            files (Dict[str, bytes]): Dictionary of filenames and their content.

        Raises:
            Exception: If an error occurs while loading a file into a DataFrame.

        Returns:
            Dict[str, pd.DataFrame]: A dictionary of filenames and their corresponding DataFrames.
        """
        dataframes = {}
        logger = LoggerSetup(logger_name="DataFrameLoader").logger
        for file_name, file_data in files.items():
            try:
                if file_name.endswith(".xlsx"):
                    dataframes[file_name] = pd.read_excel(BytesIO(file_data))
                elif file_name.endswith(".csv"):
                    dataframes[file_name] = pd.read_csv(BytesIO(file_data))
                logger.info(
                    f"Loaded file {file_name} with shape: {dataframes[file_name].shape}"
                )
            except Exception as e:
                logger.error(f"Error loading file {file_name}: {e}")
                raise
        return dataframes

    async def run(self) -> Dict[str, pd.DataFrame]:
        """
        Orchestrates the download, extraction, and loading of the dataset into DataFrames.

        Raises:
            Exception: If any step in the process fails.

        Returns:
            Dict[str, pd.DataFrame]: A dictionary of filenames and their corresponding DataFrames.
        """
        zip_data = await self.download_dataset()
        files = self.extract_files(zip_data, self.file_extensions)
        dataframes = self.load_dataframes(files)
        return dataframes


async def main(dataset_name: str) -> Dict[str, pd.DataFrame]:
    """
    Main entry point for downloading and processing the dataset.

    Args:
        dataset_name (str): The name of the dataset to download.

    Returns:
        Dict[str, pd.DataFrame]: A dictionary of filenames and their corresponding DataFrames.
    """
    downloader = DatasetDownloader(dataset_name)
    return await downloader.run()
