from sqlalchemy import create_engine  # Used to create the SQLAlchemy engine (DB connection)
from sqlalchemy.engine import URL     # Helps build the DB connection URL in a safe way
import os 

class PostgresDB:
    # Static method: no need to instantiate the class to use this method
    @staticmethod
    def get_postgres_engine_from_env() -> "sqlalchemy.Engine":
        """
        Creates and returns a SQLAlchemy engine for a PostgreSQL database
        using credentials stored in environment variables.

        Expected environment variables:
        - db_user: Database username
        - db_password: Database password
        - db_server_name: Database server/host address (e.g., AWS RDS endpoint)
        - db_database_name: Target database name

        Raises:
            EnvironmentError: If any required environment variable is missing.
        
        Returns:
            sqlalchemy.Engine: A ready-to-use SQLAlchemy engine for connecting to the DB.
        """

        # List of required environment variables
        required_vars = ["db_user", "db_password", "db_server_name", "db_database_name"]

        # Identify any missing variables
        missing = [var for var in required_vars if not os.environ.get(var)]
        if missing:
            raise EnvironmentError(f"Missing environment variables: {', '.join(missing)}")

        # Build the connection URL
        connection_url = URL.create(
            drivername="postgresql+pg8000",  
            username=os.environ["db_user"],
            password=os.environ["db_password"],
            host=os.environ["db_server_name"],  # AWS RDS endpoint, loaded from env
            port=5432,  
            database=os.environ["db_database_name"],
        )

        # Create and return the SQLAlchemy engine using the connection URL
        return create_engine(connection_url)
