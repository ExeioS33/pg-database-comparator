import os
from config import output_dir
from db.src.DBConnectionHandler import DbConnectionHandler
from db.src.DBComparator import DBObjectComparator


def clean_output_directory(directory):
    """Delete all files in the specified directory, excluding subdirectories."""
    if os.path.exists(directory):
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            try:
                # Only delete files, not directories
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                # Optionally, handle subdirectories if needed
                # elif os.path.isdir(file_path):
                #     shutil.rmtree(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")
    else:
        os.makedirs(directory)


if __name__ == '__main__':
    # Clean directory before each execution
    clean_output_directory(output_dir)

    # Prompt the user for the database names
    input_db1 = input("Enter the name of the first database (db1 | e.g : PG-TEST): ")
    input_db2 = input("Enter the name of the second database (db2 | e.g : PG-DWH): ")

    # Prompt the user for the schema name (optional)
    input_schema = input("Enter the schema name to compare (leave blank for default schema): ") or None

    # Initialize the database connection handler
    db_handler = DbConnectionHandler(input_db1, input_db2)

    # Get the connections
    connections = db_handler.get_connections()

    # Print the object to see its representation and connection
    print(db_handler)

    # Use the connections as needed
    conn1 = connections.get(input_db1)
    conn2 = connections.get(input_db2)

    # Initialize the DBObjectComparator with the schema
    comparator = DBObjectComparator(conn1, conn2, schema=input_schema)

    # Compare objects within the schema
    comparator.compare_objects()

    # Close the connections when done
    db_handler.close_connections()
