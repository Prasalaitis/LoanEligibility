import pandas as pd
import os
from typing import Dict
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from config import db_config, LoggerSetup


class AsyncSQLAlchemyConnection:
    """
    Manages the database connections using SQLAlchemy with asynchronous support.
    Ensures that connections are properly opened and closed, and transactions are
    correctly managed with commits or rollbacks as needed.

    Attributes:
        engine (AsyncEngine): The SQLAlchemy engine for asynchronous connections.
        async_session (sessionmaker): Factory for creating new AsyncSession instances.
        logger (logging.Logger): Logger instance for logging messages.
    """

    def __init__(self, db_config: Dict[str, str]):
        """
        Initializes the AsyncSQLAlchemyConnection with the provided database configuration.

        Args:
            db_config (Dict[str, str]): Dictionary containing database configuration parameters.
        """
        self._setup_logger()
        self._validate_config(db_config)
        db_url = (
            f"postgresql+asyncpg://"
            f"{db_config['user']}:"
            f"{db_config['password']}"
            f"@{db_config['host']}:"
            f"{db_config['port']}/"
            f"{db_config['database']}"
        )
        self.engine = create_async_engine(db_url)
        self.async_session = sessionmaker(
            bind=self.engine, class_=AsyncSession, expire_on_commit=False
        )

    def _setup_logger(self):
        """Sets up the logger for database operations."""
        self.logger = LoggerSetup(
            logger_name="AsyncSQLAlchemyConnection"
        ).logger

    @staticmethod
    def _validate_config(db_config: Dict[str, str]) -> None:
        """
        Validates the database configuration to ensure all required keys are present.

        Args:
            db_config (Dict[str, str]): Dictionary containing database configuration parameters.

        Raises:
            ValueError: If any required configuration key is missing.
        """
        required_keys = ["user", "password", "host", "port", "database"]
        for key in required_keys:
            if key not in db_config:
                raise ValueError(
                    f"Missing required database configuration key: {key}"
                )


class DataFrameToSQL:
    """
    Handles the conversion and insertion of pandas DataFrames into a SQL database.

    Attributes:
        async_session (sessionmaker): Factory for creating new AsyncSession instances.
        logger (logging.Logger): Logger instance for logging messages.
    """

    def __init__(self, async_session: sessionmaker):
        """
        Initializes the DataFrameToSQL with a session factory.

        Args:
            async_session (sessionmaker): Factory for creating new AsyncSession instances.
        """
        self.async_session = async_session
        self.logger = LoggerSetup(logger_name="DataFrameToSQL").logger

    @staticmethod
    def _strip_extension(file_name: str) -> str:
        """
        Removes the file extension from a filename.

        Args:
            file_name (str): The name of the file.

        Returns:
            str: The filename without the extension.
        """
        return os.path.splitext(file_name)[0]

    async def create_tables(self, dataframes: Dict[str, pd.DataFrame]):
        """
        Creates database tables based on the provided DataFrames.

        Args:
            dataframes (Dict[str, pd.DataFrame]): Dictionary of filenames and their corresponding DataFrames.

        Raises:
            Exception: If an error occurs during table creation.
        """
        async with self.async_session() as session:
            for file_name, df in dataframes.items():
                table_name = self._strip_extension(file_name)
                self.logger.info(f"Creating table for DataFrame: {table_name}")
                try:
                    await session.run_sync(
                        lambda sync_session: df.to_sql(
                            table_name,
                            con=sync_session.bind,
                            index=False,
                            if_exists="replace",
                        )
                    )
                    self.logger.info(
                        f"Table '{table_name}' created successfully based on DataFrame schema."
                    )
                except Exception as e:
                    self.logger.error(
                        f"Error creating table '{table_name}': {e}",
                        exc_info=True,
                    )
                    raise

    async def insert_data(self, dataframes: Dict[str, pd.DataFrame]):
        """
        Inserts data into existing database tables from the provided DataFrames.

        Args:
            dataframes (Dict[str, pd.DataFrame]): Dictionary of filenames and their corresponding DataFrames.

        Raises:
            Exception: If an error occurs during data insertion.
        """
        async with self.async_session() as session:
            for file_name, df in dataframes.items():
                table_name = self._strip_extension(file_name)
                self.logger.info(f"Inserting data into table: {table_name}")
                try:
                    await session.run_sync(
                        lambda sync_session: df.to_sql(
                            table_name,
                            con=sync_session.bind,
                            index=False,
                            if_exists="append",
                        )
                    )
                    self.logger.info(
                        f"Data inserted into table '{table_name}' successfully."
                    )
                except Exception as e:
                    self.logger.error(
                        f"Error inserting data into table '{table_name}': {e}",
                        exc_info=True,
                    )
                    raise


async def push_dataset(dataframes: Dict[str, pd.DataFrame]):
    """
    Pushes the dataset into the database by creating tables and inserting data.

    Args:
        dataframes (Dict[str, pd.DataFrame]): Dictionary of filenames and their corresponding DataFrames.

    Raises:
        Exception: If any step in the process fails.
    """
    connection = AsyncSQLAlchemyConnection(db_config)
    df_to_sql = DataFrameToSQL(connection.async_session)
    await df_to_sql.create_tables(dataframes)
    await df_to_sql.insert_data(dataframes)
