from config import PG_BINARIES_PATH
from db.src.DBConnectionHandler import DbConnectionHandler
from db_operations.DatabaseExporter import DatabaseExporter
from loguru import logger


def main():
    # Initialize database connection handler
    db_handler = DbConnectionHandler(db1_name="PG-DWH", db2_name="PG-TEST")
    connections = db_handler.get_connections()

    # Assume we only need the connection for db1_name
    source_connection = connections.get("PG-DWH")

    # Initialize the DatabaseExporter with the connection
    exporter = DatabaseExporter(connection=source_connection)

    # Code for the site to export
    code_site_export = "02"

    # Execute the export function
    try:
        exporter.execute_copy_command(code_site_export, PG_BINARIES_PATH)
        logger.success("Data export completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred during data export: {e}")


if __name__ == "__main__":
    main()
