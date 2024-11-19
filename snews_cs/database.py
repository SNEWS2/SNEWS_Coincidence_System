from pathlib import Path

from sqlalchemy import create_engine

from .core.logging import getLogger

log = getLogger(__name__)

db_file_path = Path(__file__).parent.parent / "snews_cs.db"


class Database():
    def __init__(self, db_file_path: Path | str) -> None:
        self.db_file_path = db_file_path
        self.engine = create_engine(f"sqlite:///{self.db_file_path}")
        self.connection = self.engine.raw_connection()
        self.cursor = self.connection.cursor()

        pass

    def initialize_database(self, sql_schema_path: Path | str) -> None:
        """
        Initialize the database with the given SQL schema.
        """
        with open(sql_schema_path) as f:
            schema_sql = f.read()
        self.cursor.executescript(schema_sql)
        self.connection.commit()
        return

    def show_tables(self) -> list[tuple[str]]:
        """
        Returns all tables in the SQL database.
        """
        self.cursor.execute(
            """SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"""
        )
        table = self.cursor.fetchall()
        return table

    def get_table_schema(self, table_name: str) -> list[tuple[str]]:
        """
        Returns the schema for a given table.
        """
        self.cursor.execute("""PRAGMA table_info({})""".format(table_name))
        schema = self.cursor.fetchall()
        return schema

    def drop_tables(self, table_names: list[str] | None = None) -> None:
        """
        Drops all tables in the SQL database.
        """
        if table_names is None:
            self.cursor.executescript("""
            PRAGMA writable_schema = 1;
            DELETE FROM sqlite_master WHERE type IN ('table', 'index', 'trigger');
            PRAGMA writable_schema = 0;
            VACUUM;""")
            self.connection.commit()
        else:
            for table_name in table_names:
                self.cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
            self.connection.commit()
