"""
Unit tests for Historical Investment Strategy Backtesting Engine.
"""

import os
import sys
import tempfile
from pathlib import Path
import pytest
import numpy as np
import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.backtesting.backtest import BacktestConfig, BacktestEngine, TradeRecord


class TestBacktestEngine:
    """Test suite for backtest portfolio accounting, costs, sizing, and drawdowns."""

    @pytest.fixture
    def sample_feed(self):
        """Create synthetic price feed with predictable movements."""
        rows = []
        base_time = pd.Timestamp("2025-01-01 09:15:00")
        # 100 1-minute bars
        for i in range(100):
            ts = base_time + pd.Timedelta(minutes=i)
            # Stock 1 trends up, model signals on bar 0
            p1 = 100.0 + i * 0.5
            prob1 = 0.35 if i == 0 else 0.10
            rows.append({
                "timestamp": ts,
                "symbol": "STOCK_A",
                "open": p1,
                "high": p1 + 0.5,
                "low": p1 - 0.5,
                "close": p1 + 0.2,
                "prob_positive": prob1
            })

            # Stock 2 trends down
            p2 = 200.0 - i * 0.2
            prob2 = 0.10
            rows.append({
                "timestamp": ts,
                "symbol": "STOCK_B",
                "open": p2,
                "high": p2 + 0.5,
                "low": p2 - 0.5,
                "close": p2 - 0.1,
                "prob_positive": prob2
            })

        return pd.DataFrame(rows)

    def test_transaction_cost_and_slippage_accounting(self, sample_feed):
        """
        Verify entry fill includes slippage upward, exit fill includes slippage downward,
        and brokerage + STT fees are deducted from net PnL.
        """
        cfg = BacktestConfig(
            initial_capital=100_000.0,
            slippage_rate=0.001,      # 10 bps
            brokerage_rate=0.0005,    # 5 bps
            stt_sell_rate=0.0010,     # 10 bps
            holding_period_bars=10
        )
        engine = BacktestEngine(cfg)
        trades, equity_df, metrics = engine.run_simulation(sample_feed, recommendation_threshold=0.25)

        assert len(trades) >= 1
        t = trades[0]

        # Entry raw price was at bar 1 open (100.5)
        # Entry fill price must be raw * (1 + 0.001)
        assert t.entry_fill_price == pytest.approx(t.entry_raw_price * 1.001, rel=1e-5)

        # Exit fill price must be raw * (1 - 0.001)
        assert t.exit_fill_price == pytest.approx(t.exit_raw_price * 0.999, rel=1e-5)

        # Net PnL must equal Gross PnL - Entry Fee - Exit Fee
        expected_net = t.gross_pnl - t.entry_fee - t.exit_fee
        assert pytest.approx(t.net_pnl, abs=0.05) == expected_net

    def test_position_sizing_and_capital_depletion(self):
        """
        Verify that new entry signals are rejected when cash is depleted,
        preventing double-counting capital or using illegal leverage.
        """
        # Low capital of ₹10,000, max allocation ₹2,000 per trade
        cfg = BacktestConfig(
            initial_capital=10_000.0,
            max_allocation_per_trade=0.20,
            max_open_positions=10,
            min_trade_amount=1_000.0
        )
        engine = BacktestEngine(cfg)

        # 6 stocks with simultaneous buy signals
        rows = []
        ts = pd.Timestamp("2025-01-01 09:15:00")
        ts_next = pd.Timestamp("2025-01-01 09:16:00")
        for sym in ["S1", "S2", "S3", "S4", "S5", "S6"]:
            rows.append({
                "timestamp": ts,
                "symbol": sym,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "prob_positive": 0.40  # All signal
            })
            rows.append({
                "timestamp": ts_next,
                "symbol": sym,
                "open": 100.5,
                "high": 101.5,
                "low": 99.5,
                "close": 101.0,
                "prob_positive": 0.10
            })
        feed = pd.DataFrame(rows)

        trades, equity_df, metrics = engine.run_simulation(feed, recommendation_threshold=0.25)

        # Final cash must never be negative
        min_cash = equity_df["cash"].min()
        assert min_cash >= 0.0, f"Cash balance became negative: {min_cash}"

    def test_max_open_positions_limit(self):
        """
        Verify that active positions never exceed max_open_positions.
        """
        cfg = BacktestConfig(
            initial_capital=500_000.0,
            max_open_positions=3  # Limit to max 3
        )
        engine = BacktestEngine(cfg)

        rows = []
        base = pd.Timestamp("2025-01-01 09:15:00")
        for i in range(10):
            ts = base + pd.Timedelta(minutes=i)
            for sym in ["A", "B", "C", "D", "E"]:
                rows.append({
                    "timestamp": ts,
                    "symbol": sym,
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "prob_positive": 0.30 if i == 0 else 0.10  # All 5 signal at bar 0
                })

        feed = pd.DataFrame(rows)
        trades, equity_df, metrics = engine.run_simulation(feed, recommendation_threshold=0.25)

        max_active = equity_df["open_positions_count"].max()
        assert max_active <= 3, f"Exceeded max positions limit: {max_active} > 3"

    def test_drawdown_calculation(self):
        """
        Verify maximum drawdown calculation against manual series.
        """
        cfg = BacktestConfig(initial_capital=100.0)
        engine = BacktestEngine(cfg)

        eq_records = [
            {"timestamp": pd.Timestamp("2025-01-01"), "cash": 100, "positions_value": 0, "total_equity": 100.0, "open_positions_count": 0},
            {"timestamp": pd.Timestamp("2025-01-02"), "cash": 120, "positions_value": 0, "total_equity": 120.0, "open_positions_count": 0}, # peak 120
            {"timestamp": pd.Timestamp("2025-01-03"), "cash": 90, "positions_value": 0, "total_equity": 90.0, "open_positions_count": 0},   # -25% DD from 120
            {"timestamp": pd.Timestamp("2025-01-04"), "cash": 110, "positions_value": 0, "total_equity": 110.0, "open_positions_count": 0},
        ]
        df = pd.DataFrame(eq_records)
        df["drawdown"] = (df["total_equity"] - df["total_equity"].cummax()) / df["total_equity"].cummax()
        df["benchmark_equity"] = [100.0, 100.0, 100.0, 100.0]

        metrics = engine._compute_performance_metrics([], df)
        # Max drawdown from 120 to 90 is -25.0%
        assert pytest.approx(metrics["max_drawdown_pct"], abs=0.1) == -25.0

    def test_results_saving_and_plotting(self, sample_feed):
        """
        Verify that save_results creates trades, equity curves, summary JSON, and PNG charts.
        """
        cfg = BacktestConfig(initial_capital=100_000.0)
        engine = BacktestEngine(cfg)
        trades, equity_df, metrics = engine.run_simulation(sample_feed, recommendation_threshold=0.25)

        with tempfile.TemporaryDirectory() as tmp_dir:
            engine.save_results(trades, equity_df, metrics, output_dir=tmp_dir)

            assert os.path.exists(os.path.join(tmp_dir, "equity_curve.csv"))
            assert os.path.exists(os.path.join(tmp_dir, "equity_curve.parquet"))
            assert os.path.exists(os.path.join(tmp_dir, "backtest_summary.json"))
            assert os.path.exists(os.path.join(tmp_dir, "equity_curve.png"))
