import os
import psycopg2
from db_operations.AbstractDatabaseOperations import AbstractCopy
from loguru import logger


class DatabaseExporter(AbstractCopy):
    def __init__(self, connection):
        """
        Initialize DatabaseExporter with a database connection.

        :param connection: A psycopg2 database connection object.
        """
        self.connection = connection

    def execute_copy_command(self, code_site_export: str, file_path: str) -> None:
        """Execute the dwh.export_site_data function to export site data."""
        # Ensure the output directory exists
        self._check_dir(file_path)

        try:
            # Use the provided connection
            with self.connection as conn:
                with conn.cursor() as cursor:
                    # Call the function
                    cursor.callproc(
                        "dwh.export_site_data", [file_path, code_site_export]
                    )
                    conn.commit()
                    logger.info(f"Data is being exported to {file_path}")
        except psycopg2.Error as e:
            logger.error(f"An error occurred during the export process: {e}")

    def _check_dir(self, file_path: str):
        # Ensure the output directory exists
        if not os.path.exists(file_path):
            os.makedirs(file_path)
