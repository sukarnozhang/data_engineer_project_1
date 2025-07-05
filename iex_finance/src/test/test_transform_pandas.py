import pandas as pd
from etl.transform import Transform 

def test_transform_pandas_daily_agg():
    # Mock intraday input data
    df_mock_input = pd.DataFrame({
        "stock_code": ["AAPL", "AAPL", "AAPL", "TSLA", "TSLA"],
        "minute": [
            "2023-01-03 09:30:00",
            "2023-01-03 12:00:00",
            "2023-01-03 15:59:00",
            "2023-01-03 10:00:00",
            "2023-01-03 14:30:00"
        ],
        "open": [150.00, 151.00, 152.00, 700.00, 705.00],
        "close": [151.00, 152.00, 153.00, 702.00, 707.00],
        "high": [151.5, 152.5, 153.5, 703.0, 708.0],
        "low": [149.5, 150.5, 151.5, 699.0, 704.0],
        "volume": [1000, 1500, 1200, 2000, 1800],
        "numberOfTrades": [10, 12, 11, 15, 14]
    })

    # Expected daily aggregation output
    df_expected_output = pd.DataFrame({
        "stock_code": ["AAPL", "TSLA"],
        "date": ["2023-01-03", "2023-01-03"],
        "min_open": [150.0, 700.0],
        "max_close": [153.0, 707.0],
        "daily_high": [153.5, 708.0],
        "daily_low": [149.5, 699.0],
        "daily_volume": [3700, 3800],
        "daily_trades": [33, 29],
        "daily_return": [3.0, 7.0]
    })

    # Run transform
    df_actual_output = Transform.transform_pandas(df_mock_input)

    # Assert
    pd.testing.assert_frame_equal(
        df_actual_output.sort_values(["stock_code"]).reset_index(drop=True),
        df_expected_output.sort_values(["stock_code"]).reset_index(drop=True)
    )
