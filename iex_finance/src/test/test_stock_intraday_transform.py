import os
import pandas as pd
from database.postgres import PostgresDB
from etl.transform import Transform

def test_stocks_intraday_transform():
    """
    Test ETL transformation on intraday stock data.

    This test:
    - Loads mock intraday stock data into a temporary source table.
    - Runs the transformation SQL to process the data.
    - Verifies the output matches expected transformed data.
    - Cleans up test artifacts after completion.
    """

    # --- Setup ---
    engine = PostgresDB.create_pg_engine()
    source_table = "stocks_intraday"
    mock_table = f"mock_{source_table}"  # temporary mock table name
    path_model_transform = "models/transform"

    # Mock input data simulating raw intraday stock metrics
    df_mock_input = pd.DataFrame({
        "stock_code": ["AAPL", "TSLA"],
        "max_open": [151.165, 205.59],
        "min_open": [149.39, 193.18],
        "max_close": [151.225, 205.61],
        "min_close": [149.41, 193.225],
        "max_high": [151.225, 206.2],
        "min_high": [149.44, 193.4],
        "max_low": [151.01, 205.07],
        "min_low": [149.26, 193.03],
        "status_diff": [-1.11, 9.055],
        "mean_trades": [14.59, 20.25],
        "mean_volume": [2095.53, 2442.13]
    })

    # Load mock data into the mock source table in the database
    df_mock_input.to_sql(name=mock_table, con=engine, if_exists="replace", index=False)

    # --- Define expected output after the transformation ---
    df_expected_output = pd.DataFrame({
        "stock_code": ["AAPL", "TSLA"],
        "max_open": [151.165, 205.59],
        "min_open": [149.39, 193.18],
        "max_close": [151.225, 205.61],
        "min_close": [149.41, 193.225],
        "max_high": [151.225, 206.2],
        "min_high": [149.44, 193.4],
        "max_low": [151.01, 205.07],
        "min_low": [149.26, 193.03],
        "status_diff": [-1.11, 9.055],
        "mean_trades": [14.59, 20.25],
        "mean_volume": [2095.53, 2442.13]
    })

    # Run the transform SQL against the mock table
    Transform.transform(
        table_name=mock_table,
        engine=engine,
        models_path=path_model_transform
    )

    # Read the actual transformed data back from the database
    df_actual_output = pd.read_sql_table(mock_table, con=engine)

    # --- Cleanup ---
    engine.execute(f"DROP TABLE IF EXISTS {mock_table}")

    # --- Assert ---
    pd.testing.assert_frame_equal(
        df_actual_output.reset_index(drop=True),
        df_expected_output.reset_index(drop=True),
        check_exact=False,   # Allow small numeric differences
        rtol=1e-3            # Relative tolerance for floating point comparisons
    )
