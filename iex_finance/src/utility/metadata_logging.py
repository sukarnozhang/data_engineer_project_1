from database.postgres import PostgresDB
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy import select, func

class MetadataLogging:
    """
    Utility class for handling ETL metadata logging into a Postgres database.
    Provides methods to create a log table if missing, and to fetch the next run ID.
    """

    def __init__(self):
        """
        Initialize with a Postgres database engine.
        """
        self.engine = PostgresDB.create_pg_engine()

    def create_target_table_if_not_exists(self, db_table: str) -> Table:
        """
        Creates the target metadata log table in the database if it does not exist.

        Args:
            db_table (str): Name of the log table.

        Returns:
            sqlalchemy.Table: SQLAlchemy table object linked to the table.
        """
        meta = MetaData()
        target_table = Table(
            db_table, meta,
            Column("run_timestamp", String, primary_key=True),  # Timestamp of the run
            Column("run_id", Integer, primary_key=True),        # Run ID (sequential integer)
            Column("run_status", String, primary_key=True),     # Run status (e.g. started, completed, failed)
            Column("run_log", String)                           # Optional: Captured log output
        )
        meta.create_all(self.engine)  # Creates the table if it doesn't exist (no-op if it does)
        return target_table

    def get_latest_run_id(self, db_table: str) -> int:
        """
        Gets the next run_id (max existing + 1, or 1 if no runs exist).

        Args:
            db_table (str): Name of the log table.

        Returns:
            int: The next run_id to use.
        """
        target_table = self.create_target_table_if_not_exists(db_table=db_table)

        # Query max run_id
        statement = select(func.max(target_table.c.run_id))
        result = self.engine.execute(statement).first()[0]

        return 1 if result is None else result + 1
