from contextlib import contextmanager
import os

import psycopg2


class MetadatadbConnection:
    def __init__(self) -> None:
        """Class to connect to a database.

        Args:
            host (str, optional): Hostname of the database server.
            port (str, optional): Port number of the database server.
            dbname (str, optional): Name of the database.
            user (str, optional): Username for the database connection.
            password (str, optional): Password for the database connection.
        """
        self._host = os.getenv("METADATA_HOST", "localhost")
        self._port = os.getenv("METADATA_PORT", "5432")
        self._dbname = os.getenv("METADATA_DATABASE", "mydatabase")
        self._user = os.getenv("METADATA_USERNAME", "myuser")
        self._password = os.getenv("METADATA_PASSWORD", "mypassword")

    @contextmanager
    def managed_cursor(self):
        """Function to create a managed database cursor.

        Yields:
            psycopg2.cursor: A database cursor.
        """
        _conn = psycopg2.connect(
            host=self._host,
            port=self._port,
            dbname=self._dbname,
            user=self._user,
            password=self._password,
        )
        cur = _conn.cursor()
        try:
            yield cur
        finally:
            _conn.commit()
            cur.close()
            _conn.close()

    def __str__(self) -> str:
        return (
            f"{self._db_type}://{self._user}@{self._host}:{self._port}/{self._dbname}"
        )
