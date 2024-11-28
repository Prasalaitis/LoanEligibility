from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Dict
from config import LoggerSetup
from config import db_config as config


class AsyncSQLAlchemyConnection:
    """
    Manages the database connections using SQLAlchemy with asynchronous support. It ensures that connections
    are properly opened and closed, and transactions are correctly managed with commits or rollbacks as needed.
    """

    def __init__(self, db_config: Dict[str, str] = config):
        """
        Initializes the AsyncSQLAlchemyConnection with the provided database configuration.

        :param db_config: Dictionary containing database configuration parameters.
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
        self.AsyncSession = sessionmaker(
            bind=self.engine, expire_on_commit=False, class_=AsyncSession
        )

    def _setup_logger(self):
        """Sets up the logger for database operations."""
        self.logger = LoggerSetup(
            logger_name="AsyncSQLAlchemyConnection"
        ).logger

    @staticmethod
    def _validate_config(db_config: Dict[str, str]) -> None:
        required_keys = ["user", "password", "host", "port", "database"]
        for key in required_keys:
            if key not in db_config:
                raise ValueError(
                    f"Missing required database configuration key: {key}"
                )

    @asynccontextmanager
    async def connect(self) -> AsyncGenerator[AsyncSession, None]:
        """
        An asynchronous context manager that manages a database session. It automatically commits
        the transaction on successful block execution or rolls back if an exception occurs.

        :yield: An asynchronous SQLAlchemy session.
        """
        async with self.AsyncSession() as session:
            try:
                yield session
                await session.commit()
            except Exception as e:
                await session.rollback()
                self.logger.error(
                    f"Database error during transaction: {e}", exc_info=True
                )
                raise
            finally:
                await session.close()
