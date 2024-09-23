import os
import psycopg2
from dotenv import find_dotenv, load_dotenv
from typing import Any


class DbConnectionHandler:
    def __init__(self, db1_name: str, db2_name: str):
        self.db1_name = db1_name
        self.db2_name = db2_name

    def get_connections(self) -> dict:
        connections = {}

        # For db1_name
        conn1 = self._connect_to_db(self.db1_name)
        if conn1:
            connections[self.db1_name] = conn1
        else:
            print(f"Failed to connect to {self.db1_name}")

        # For db2_name
        conn2 = self._connect_to_db(self.db2_name)
        if conn2:
            connections[self.db2_name] = conn2
        else:
            print(f"Failed to connect to {self.db2_name}")

        return connections

    @staticmethod
    def _connect_to_db(db_name: str) -> Any:
        # Match db_name to connect to specific databases
        load_dotenv(find_dotenv())

        match db_name:
            case 'PG-TIERS':
                raise ValueError("PG-TIERS is not yet implemented")

            case 'PG-DWH':
                # Logic to connect to PG-CUSTOMERS
                try:
                    conn = psycopg2.connect(
                        dbname=os.getenv('DB1_NAME'),
                        user=os.getenv('DB1_USER'),
                        host=os.getenv('DB1_HOST'),
                        port=os.getenv('DB1_PORT')
                    )
                    return conn
                except Exception as e:
                    print(f"Error connecting to {db_name}: {e}")
                    return None

            case 'PG-TEST':
                # Logic to connect to PG-TIERS
                try:
                    conn = psycopg2.connect(
                        dbname=os.getenv('DB2_NAME'),
                        user=os.getenv('DB2_USER'),
                        host=os.getenv('DB2_HOST'),
                        port=os.getenv('DB2_PORT')
                        # Password is omitted, relying on .pgpass
                    )
                    return conn
                except Exception as e:
                    print(f"Error connecting to {db_name}: {e}")
                    return None

            # Add more cases as needed for other databases

            case _:
                print(f"Unknown database name: {db_name}")
                return None

    def close_connections(self) -> bool:
        is_closed = False
        for conn in self.get_connections().values():
            if conn:
                conn.close()
                is_closed = True
        return is_closed

    def __str__(self) -> str:
        connection_status = []
        for db_name in [self.db1_name, self.db2_name]:
            conn = self.get_connections().get(db_name)
            status = 'Connected' if conn else 'Not Connected'
            connection_status.append(f"{db_name}: {status}")
        return f"DbConnectionHandler({', '.join(connection_status)})"
