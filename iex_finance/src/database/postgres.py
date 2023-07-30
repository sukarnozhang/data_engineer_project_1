from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float # https://www.tutorialspoint.com/sqlalchemy/sqlalchemy_core_creating_table.htm
from sqlalchemy.engine import URL
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable 
import os 

class PostgresDB():
    
    @staticmethod
    def create_pg_engine():
        """
        Method to create DB engine using following envt variables: 
        db_user
        db_password
        db_server_name
        db_database_name 
        """ 
        db_user = os.environ.get("db_user")
        db_password = os.environ.get("db_password")
        db_server_name = os.environ.get("db_server_name")
        db_database_name = os.environ.get("db_database_name")

        # create connection to database 
        connection_url = URL.create(
            drivername = "postgresql+pg8000", 
            username = db_user,
            password = db_password,
            host = db_server_name, 
            port = 5432,
            database = db_database_name, 
        )
        engine = create_engine(connection_url)
        return engine 