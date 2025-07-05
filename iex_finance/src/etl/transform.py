import pandas as pd
import os
import logging
import jinja2 as j2

class Transform:
    @staticmethod
    def transform(model: str, engine, models_path: str, target_table: str) -> bool:
        """
        Orchestrates the execution of a SQL model for staging.

        Parameters:
        - model (str): Name of the SQL model (without .sql extension)
        - engine: SQLAlchemy engine for DB connection
        - models_path (str): Directory containing SQL model files
        - target_table (str): Target table name for staging

        Returns:
        - bool: True if successful, False otherwise
        """
        model_file = f"{model}.sql"
        model_path = os.path.join(models_path, model_file)

        if not os.path.isfile(model_path):
            logging.warning(f"Model file {model_file} not found in {models_path}")
            return False

        # Read SQL template
        with open(model_path) as f:
            raw_sql = f.read()

        # Render template with Jinja
        parsed_sql = j2.Template(raw_sql).render(target_table=target_table)

        # Execute SQL
        result = engine.execute(parsed_sql)
        logging.info(f"Successfully built model: {model}, rows affected: {result.rowcount}")
        return True
