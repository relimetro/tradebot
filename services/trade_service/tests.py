import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch
from datetime import datetime
import builtins

import main

# Sample fixture for raw klines
@pytest.fixture
def sample_klines():
    return [
        [1609459200000, "100", "110", "90", "105", "1000", 1609462800000, "100000", 10, "500", "50000", "0"],
        [1609462800000, "105", "115", "95", "110", "1200", 1609466400000, "130000", 12, "600", "60000", "0"]
    ]

@pytest.fixture
def sample_df(sample_klines):
    return main.process_raw_klines(sample_klines)

# Test feature engineering
def test_add_features(sample_df):
    df = main.add_features(sample_df)
    # Ensure new columns are created
    expected_cols = ["mean_price", "return", "price_change", "volatility", "body_size",
                     "upper_shadow", "lower_shadow", "candle_shape", "previous_close",
                     "true_range", "avg_true_range_14", "buy_ratio", "normalized_volume",
                     "momentum_3h", "momentum_6h", "momentum_12h", "momentum_24h", "momentum_48h",
                     "rolling_return_mean_3", "rolling_return_mean_6", "rolling_return_mean_12",
                     "rolling_return_mean_24", "rolling_return_mean_48", "rolling_volatility_3",
                     "rolling_volatility_6", "rolling_volatility_12", "rolling_volatility_24",
                     "rolling_volatility_48", "sma_6", "sma_12", "sma_24", "sma_48", "ema_6", "ema_12",
                     "ema_24", "ema_48", "sma_ratio_6", "sma_ratio_12", "sma_ratio_24", "sma_ratio_48",
                     "ema_ratio_6", "ema_ratio_12", "ema_ratio_24", "ema_ratio_48", "RSI_14",
                     "MACD", "MACD_signal", "MACD_hist", "BB_middle", "BB_upper", "BB_lower",
                     "BB_width", "stoch_k", "stoch_d", "target"]
    for col in expected_cols:
        assert col in df.columns
    assert not df.isnull().values.any()

# Test run_prediction with mocks
@patch("main.joblib.load")
def test_run_prediction(mock_joblib):
    # Mock scaler and model
    mock_scaler = MagicMock()
    mock_scaler.transform.return_value = np.array([[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]])
    mock_model = MagicMock()
    mock_model.predict.return_value = [0.42]

    mock_joblib.side_effect = [mock_scaler, mock_model]

    row = {"High":1,"Open":1,"Close":1,"avg_true_range_14":1,"momentum_48h":1,
           "rolling_return_mean_24":1,"BB_width":1,"ema_24":1,"MACD":1,"BB_middle":1}

    pred = main.run_prediction(row)
    assert pred == 0.42

# Test save_prediction_to_db
@patch.object(main.col_predictions, "insert_one")
def test_save_prediction_to_db(mock_insert):
    ts = datetime.utcnow()
    main.save_prediction_to_db(0.5, ts)
    mock_insert.assert_called_once()
    assert mock_insert.call_args[0][0]["prediction"] == 0.5

# Test get_balances
@patch.object(main.client, "synced")
@patch.object(main.col_balance, "insert_one")
def test_get_balances(mock_insert, mock_synced):
    mock_synced.return_value = {"balances":[{"asset":"EUR","free":"100"},{"asset":"ETH","free":"0.5"}]}
    balances = main.get_balances()
    assert balances["EUR"] == 100
    assert balances["ETH"] == 0.5
    mock_insert.assert_called_once()

# Test execute_trade logic (buy)
@patch("main.get_balances")
@patch.object(main.client, "synced")
@patch.object(main.col_trades, "insert_one")
def test_execute_trade_buy(mock_insert, mock_synced, mock_balances):
    mock_balances.return_value = {"EUR": 100, "ETH": 0.1}
    mock_synced.return_value = {"orderId": 123}
    main.execute_trade(1.0, 50)
    mock_insert.assert_called_once()

# Test process_raw_klines
def test_process_raw_klines(sample_klines):
    df = main.process_raw_klines(sample_klines)
    assert df.shape[0] == 2
    assert set(["Open", "High", "Low", "Close", "Volume"]).issubset(df.columns)

