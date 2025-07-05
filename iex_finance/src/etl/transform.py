import pandas as pd
import os
import logging
import jinja2 as j2

class Transform:
    @staticmethod
    def transform_pandas(df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform using Pandas (in-memory)
        """
        df['date'] = pd.to_datetime(df['datetime']).dt.date

        daily_df = (
            df.groupby(['stock_code', 'date']).agg(
                min_open=('open', 'min'),
                max_close=('close', 'max'),
                daily_high=('high', 'max'),
                daily_low=('low', 'min'),
                daily_volume=('volume', 'sum'),
                daily_trades=('numberOfTrades', 'sum')
            )
            .reset_index()
        )

        # Compute daily_return as max_close - min_open
        daily_df['daily_return'] = daily_df['max_close'] - daily_df['min_open']
        
        return daily_df

    @staticmethod
    def transform_sql(model: str, engine, models_path: str, target_table: str) -> bool:
        """
        Transform using SQL (Jinja + SQL execution)
        """
        model_file = f"{model}.sql"
        model_path = os.path.join(models_path, model_file)

        if not os.path.isfile(model_path):
            logging.warning(f"Model file {model_file} not found in {models_path}")
            return False

        # Read SQL template
        with open(model_path) as f:
            raw_sql = f.read()

        # Render Jinja template
        template = j2.Template(raw_sql)
        parsed_sql = template.render(target_table=target_table)

        # Execute SQL
        try:
            result = engine.execute(parsed_sql)
            logging.info(f"Successfully executed SQL transform for model: {model}")
            return True
        except Exception as e:
            logging.error(f"Error executing SQL for model {model}: {e}")
            return False
