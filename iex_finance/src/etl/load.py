from sqlalchemy import Table, Column, String, Float, Date
from sqlalchemy.dialects import postgresql
import pandas as pd
import os
import logging

class Load:
    @staticmethod
    def load(
        df: pd.DataFrame,
        target_database_engine = None,
        target_table_name: str = None
    ) -> None:
        """
        Load DataFrame to a Postgres database table using upsert (insert or update).

        Parameters:
        - df: pandas DataFrame containing the data to load
        - target_database_engine: SQLAlchemy engine connected to the target database
        - target_table_name: name of the target database table to load data into
        """

        # Define the database table schema mapping
        # This creates an in-memory Table object to assist with SQL generation
        stock_price_table = Table(
            target_table_name,
            Column("stock_code", String, primary_key=True),  # Stock ticker symbol, part of composite PK
            Column("date", Date, primary_key=True),          # Trading date, part of composite PK for uniqueness per day
            Column("min_open", Float),                       # Minimum open price for the day
            Column("max_close", Float),                       # Maximum close price for the day
            Column("daily_high", Float),                      # Highest price during the day
            Column("daily_low", Float),                     # Lowest price during the day
            Column("daily_volume", Float),                    # Total volume traded that day
            Column("daily_trades", Float),                    # Total number of trades executed that day
            Column("daily_return", Float)                     # Net price change (max close - min open)
        )

        # Prepare an INSERT statement using PostgreSQL dialect's insert()
        # The data is converted from the DataFrame to a list of dict records
        insert_stmt = postgresql.insert(stock_price_table).values(df.to_dict(orient='records'))

        # Create an UPSERT statement: on conflict of primary keys ("stock_code", "date")
        # Update all columns except the primary key columns to keep data fresh
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["stock_code", "date"],           # Columns defining the conflict (PK)
            set_={col: insert_stmt.excluded[col]              # Set columns to new values from insert, excluding PKs
                  for col in df.columns if col not in ['stock_code', 'date']}
        )

        # Execute the upsert statement using the provided SQLAlchemy engine
        target_database_engine.execute(upsert_stmt)

        # Log a success message
        logging.info(f"Table {target_table_name} upserted.")


