"""
Historical Investment Strategy Backtesting Engine for SmartStock.

Simulates a realistic, chronological investment strategy using predictions from the
trained Spark MLlib model evaluated on the out-of-sample held-out test data.

Features:
- Strict Look-Ahead Prevention: Signal generated at candle t close, fills at candle t+1 open.
- Real-world Execution Frictional Costs: Configurable slippage, brokerage, exchange fees, and STT.
- Portfolio Accounting: Tracks dynamic cash balance, open positions, mark-to-market equity, and drawdowns.
- Capital Allocation & Risk Policy: Max 20% equity per position, max 5 simultaneous positions, zero margin debt.
- Overlapping Signal Handling: Rejects new entries when capital is exhausted or position cap is reached.
- Benchmark Comparison: Compares performance against an Equal-Weighted Buy-and-Hold portfolio and Cash risk-free rate.
- Artifacts: Saves trade logs (Parquet/CSV), equity curves (Parquet/CSV), summary JSON, and visualization plots.
"""

import argparse
import json
import math
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Configure Hadoop on Windows
hadoop_dir = project_root / "hadoop"
if hadoop_dir.exists():
    os.environ["HADOOP_HOME"] = str(hadoop_dir.resolve())
    hadoop_bin = str((hadoop_dir / "bin").resolve())
    if hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.preprocessing.clean_data import SparkSessionManager
from src.training.evaluate_models import ModelEvaluator, ModelPredictor
from src.training.split_data import DataSplitter


@dataclass
class BacktestConfig:
    """
    Configuration parameters for backtesting execution, costs, and risk management.
    """
    initial_capital: float = 1_000_000.0          # Initial portfolio cash in INR (₹10 Lakhs)
    max_allocation_per_trade: float = 0.20        # Maximum 20% portfolio equity per trade
    max_open_positions: int = 5                   # Maximum 5 concurrent open positions (0% debt leverage)
    holding_period_bars: int = 60                 # 60-minute holding horizon matching the trained model
    slippage_rate: float = 0.0005                 # 5 bps slippage per side (0.05%)
    brokerage_rate: float = 0.0005                # 5 bps brokerage / exchange fee per side (0.05%)
    stt_sell_rate: float = 0.0010                 # 10 bps Securities Transaction Tax on sale (0.10%)
    risk_free_rate_annual: float = 0.065          # 6.50% annual Indian risk-free benchmark
    min_trade_amount: float = 5_000.0             # Minimum order size in INR to execute


@dataclass
class TradeRecord:
    """
    Detailed audit trail for an executed round-trip trade.
    """
    trade_id: int
    symbol: str
    entry_signal_time: str
    entry_fill_time: str
    entry_raw_price: float
    entry_fill_price: float
    shares: int
    entry_notional: float
    entry_fee: float
    exit_fill_time: str
    exit_raw_price: float
    exit_fill_price: float
    exit_notional: float
    exit_fee: float
    gross_pnl: float
    net_pnl: float
    return_pct: float
    duration_bars: int


class BacktestEngine:
    """
    Chronological event-driven portfolio backtesting engine.
    """

    def __init__(
        self,
        config: Optional[BacktestConfig] = None
    ):
        """
        Initialize backtesting engine.
        """
        self.config = config or BacktestConfig()

    def run_simulation(
        self,
        df_feed: pd.DataFrame,
        recommendation_threshold: float = 0.25
    ) -> Tuple[List[TradeRecord], pd.DataFrame, Dict[str, Any]]:
        """
        Execute chronological backtest on historical price feed with model predictions.

        Args:
            df_feed: Pandas DataFrame containing ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'prob_positive']
            recommendation_threshold: Minimum probability to trigger a buy order

        Returns:
            Tuple of (trades_list, equity_curve_df, summary_metrics_dict)
        """
        # Ensure timestamp is datetime and sort chronologically
        df_feed = df_feed.copy()
        df_feed["timestamp"] = pd.to_datetime(df_feed["timestamp"])
        df_feed = df_feed.sort_values(by=["timestamp", "symbol"]).reset_index(drop=True)

        # Portfolio state
        cash = self.config.initial_capital
        open_positions: Dict[str, Dict[str, Any]] = {}  # symbol -> position dict
        pending_orders: List[Dict[str, Any]] = []       # Orders generated at bar t, fills at bar t+1
        trades: List[TradeRecord] = []
        trade_id_counter = 1

        # Tracking equity curve
        equity_records = []
        unique_timestamps = df_feed["timestamp"].unique()

        # Pre-group feed by timestamp for high-performance chronological traversal
        grouped_feed = {ts: group for ts, group in df_feed.groupby("timestamp")}

        print(f"  Simulating across {len(unique_timestamps):,} discrete chronological timesteps...")

        for ts in unique_timestamps:
            bar_df = grouped_feed[ts]
            bar_map = {row["symbol"]: row for _, row in bar_df.iterrows()}

            # -------------------------------------------------------------
            # 1. Execute Pending Orders from Previous Bar (Fill at t+1 Open)
            # -------------------------------------------------------------
            remaining_orders = []
            for order in pending_orders:
                sym = order["symbol"]
                if sym in bar_map:
                    bar = bar_map[sym]
                    raw_open = float(bar["open"])
                    fill_price = raw_open * (1.0 + self.config.slippage_rate)

                    # Compute allocation based on capital available
                    target_alloc = order["target_allocation"]
                    shares = math.floor(target_alloc / fill_price)

                    if shares > 0 and (shares * fill_price) <= cash:
                        notional = shares * fill_price
                        entry_fee = notional * self.config.brokerage_rate
                        cash -= (notional + entry_fee)

                        open_positions[sym] = {
                            "trade_id": order["trade_id"],
                            "symbol": sym,
                            "entry_signal_time": order["signal_time"],
                            "entry_fill_time": ts,
                            "entry_raw_price": raw_open,
                            "entry_fill_price": fill_price,
                            "shares": shares,
                            "entry_notional": notional,
                            "entry_fee": entry_fee,
                            "entry_bar_idx": 0
                        }
                else:
                    # Stock candle missing in this bar, cancel order to avoid stale fills
                    pass
            pending_orders = []

            # -------------------------------------------------------------
            # 2. Check Exits for Open Positions (Fixed 60-Minute Horizon)
            # -------------------------------------------------------------
            symbols_to_close = []
            for sym, pos in open_positions.items():
                pos["entry_bar_idx"] += 1
                # If holding horizon reached or stock is present on final bar
                if pos["entry_bar_idx"] >= self.config.holding_period_bars and sym in bar_map:
                    bar = bar_map[sym]
                    raw_close = float(bar["close"])
                    exit_fill_price = raw_close * (1.0 - self.config.slippage_rate)
                    exit_notional = pos["shares"] * exit_fill_price
                    exit_fee = exit_notional * (self.config.brokerage_rate + self.config.stt_sell_rate)

                    gross_pnl = exit_notional - pos["entry_notional"]
                    net_pnl = gross_pnl - pos["entry_fee"] - exit_fee
                    return_pct = net_pnl / pos["entry_notional"] if pos["entry_notional"] > 0 else 0.0

                    cash += (exit_notional - exit_fee)

                    trade = TradeRecord(
                        trade_id=pos["trade_id"],
                        symbol=sym,
                        entry_signal_time=str(pos["entry_signal_time"]),
                        entry_fill_time=str(pos["entry_fill_time"]),
                        entry_raw_price=pos["entry_raw_price"],
                        entry_fill_price=pos["entry_fill_price"],
                        shares=pos["shares"],
                        entry_notional=round(pos["entry_notional"], 2),
                        entry_fee=round(pos["entry_fee"], 2),
                        exit_fill_time=str(ts),
                        exit_raw_price=raw_close,
                        exit_fill_price=exit_fill_price,
                        exit_notional=round(exit_notional, 2),
                        exit_fee=round(exit_fee, 2),
                        gross_pnl=round(gross_pnl, 2),
                        net_pnl=round(net_pnl, 2),
                        return_pct=round(return_pct, 6),
                        duration_bars=pos["entry_bar_idx"]
                    )
                    trades.append(trade)
                    symbols_to_close.append(sym)

            for sym in symbols_to_close:
                del open_positions[sym]

            # -------------------------------------------------------------
            # 3. Mark-to-Market Portfolio Valuation
            # -------------------------------------------------------------
            positions_mkt_val = 0.0
            for sym, pos in open_positions.items():
                cur_p = float(bar_map[sym]["close"]) if sym in bar_map else pos["entry_raw_price"]
                positions_mkt_val += pos["shares"] * cur_p

            current_equity = cash + positions_mkt_val

            equity_records.append({
                "timestamp": ts,
                "cash": round(cash, 2),
                "positions_value": round(positions_mkt_val, 2),
                "total_equity": round(current_equity, 2),
                "open_positions_count": len(open_positions)
            })

            # -------------------------------------------------------------
            # 4. Generate New Entry Orders (Model Prediction >= Threshold)
            # -------------------------------------------------------------
            # Sort candidates by predicted probability descending (pick strongest signals first)
            candidate_bars = [
                row for _, row in bar_df.iterrows()
                if row.get("prob_positive", 0.0) >= recommendation_threshold
            ]
            candidate_bars.sort(key=lambda x: x.get("prob_positive", 0.0), reverse=True)

            for bar in candidate_bars:
                sym = bar["symbol"]
                # Enforce position limits: max 1 position per symbol, max 5 positions total
                total_active = len(open_positions) + len(pending_orders)
                if sym in open_positions or any(o["symbol"] == sym for o in pending_orders):
                    continue
                if total_active >= self.config.max_open_positions:
                    break

                # Position sizing: max 20% of current equity, bounded by available cash
                target_alloc = min(
                    current_equity * self.config.max_allocation_per_trade,
                    cash * 0.95  # Leave 5% buffer for slippage and brokerage
                )

                if target_alloc >= self.config.min_trade_amount:
                    pending_orders.append({
                        "trade_id": trade_id_counter,
                        "symbol": sym,
                        "signal_time": ts,
                        "target_allocation": target_alloc
                    })
                    trade_id_counter += 1

        # -------------------------------------------------------------
        # 5. Liquidate Any Remaining Open Positions at Dataset End
        # -------------------------------------------------------------
        final_ts = unique_timestamps[-1]
        final_bar_map = {row["symbol"]: row for _, row in grouped_feed[final_ts].iterrows()}
        for sym, pos in list(open_positions.items()):
            cur_close = float(final_bar_map[sym]["close"]) if sym in final_bar_map else pos["entry_raw_price"]
            exit_fill_price = cur_close * (1.0 - self.config.slippage_rate)
            exit_notional = pos["shares"] * exit_fill_price
            exit_fee = exit_notional * (self.config.brokerage_rate + self.config.stt_sell_rate)
            gross_pnl = exit_notional - pos["entry_notional"]
            net_pnl = gross_pnl - pos["entry_fee"] - exit_fee
            return_pct = net_pnl / pos["entry_notional"] if pos["entry_notional"] > 0 else 0.0
            cash += (exit_notional - exit_fee)

            trade = TradeRecord(
                trade_id=pos["trade_id"],
                symbol=sym,
                entry_signal_time=str(pos["entry_signal_time"]),
                entry_fill_time=str(pos["entry_fill_time"]),
                entry_raw_price=pos["entry_raw_price"],
                entry_fill_price=pos["entry_fill_price"],
                shares=pos["shares"],
                entry_notional=round(pos["entry_notional"], 2),
                entry_fee=round(pos["entry_fee"], 2),
                exit_fill_time=str(final_ts),
                exit_raw_price=cur_close,
                exit_fill_price=exit_fill_price,
                exit_notional=round(exit_notional, 2),
                exit_fee=round(exit_fee, 2),
                gross_pnl=round(gross_pnl, 2),
                net_pnl=round(net_pnl, 2),
                return_pct=round(return_pct, 6),
                duration_bars=pos["entry_bar_idx"]
            )
            trades.append(trade)

        equity_df = pd.DataFrame(equity_records)
        equity_df["drawdown"] = (
            equity_df["total_equity"] - equity_df["total_equity"].cummax()
        ) / equity_df["total_equity"].cummax()

        # Compute benchmark: Equal-Weighted Buy-and-Hold
        benchmark_equity = self._compute_buy_and_hold_benchmark(df_feed, unique_timestamps)
        equity_df["benchmark_equity"] = benchmark_equity

        # Compute summary metrics
        metrics = self._compute_performance_metrics(trades, equity_df)

        return trades, equity_df, metrics

    def _compute_buy_and_hold_benchmark(
        self,
        df_feed: pd.DataFrame,
        unique_timestamps: np.ndarray
    ) -> List[float]:
        """
        Compute mark-to-market equity for equal-weighted Buy-and-Hold portfolio.
        """
        start_ts = unique_timestamps[0]
        start_bars = df_feed[df_feed["timestamp"] == start_ts]
        symbols = start_bars["symbol"].unique()

        if len(symbols) == 0:
            return [self.config.initial_capital] * len(unique_timestamps)

        alloc_per_stock = self.config.initial_capital / len(symbols)
        shares_map = {}
        for _, r in start_bars.iterrows():
            sym = r["symbol"]
            p = float(r["open"])
            shares_map[sym] = alloc_per_stock / p if p > 0 else 0.0

        pivoted_close = df_feed.pivot(index="timestamp", columns="symbol", values="close").ffill().bfill()

        bench_equity = []
        for ts in unique_timestamps:
            if ts in pivoted_close.index:
                row_prices = pivoted_close.loc[ts]
                val = sum(shares_map.get(s, 0.0) * row_prices.get(s, 0.0) for s in symbols)
                bench_equity.append(round(val, 2))
            else:
                bench_equity.append(bench_equity[-1] if bench_equity else self.config.initial_capital)

        return bench_equity

    def _compute_performance_metrics(
        self,
        trades: List[TradeRecord],
        equity_df: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Calculate quantitative performance, risk, and trade-level statistics.
        """
        init_cap = self.config.initial_capital
        final_equity = equity_df["total_equity"].iloc[-1]
        net_profit = final_equity - init_cap
        total_return = net_profit / init_cap

        # Time duration
        start_dt = equity_df["timestamp"].iloc[0]
        end_dt = equity_df["timestamp"].iloc[-1]
        days = (end_dt - start_dt).total_seconds() / 86400.0

        # Annualized Return (CAGR)
        if days >= 30 and (1.0 + total_return) > 0:
            cagr = ((1.0 + total_return) ** (365.25 / days)) - 1.0
        else:
            cagr = total_return

        # Maximum Drawdown
        max_drawdown = float(equity_df["drawdown"].min())

        # Daily Returns for Sharpe / Volatility
        daily_equity = equity_df.set_index("timestamp")["total_equity"].resample("D").last().dropna()
        daily_returns = daily_equity.pct_change().dropna()

        if len(daily_returns) > 5 and daily_returns.std() > 0:
            vol_ann = float(daily_returns.std() * np.sqrt(252))
            rf_daily = self.config.risk_free_rate_annual / 252.0
            excess_daily = daily_returns - rf_daily
            sharpe = float((excess_daily.mean() / daily_returns.std()) * np.sqrt(252))

            downside_std = daily_returns[daily_returns < 0].std()
            sortino = (
                float((excess_daily.mean() / downside_std) * np.sqrt(252))
                if downside_std > 0 else sharpe
            )
        else:
            vol_ann = 0.0
            sharpe = 0.0
            sortino = 0.0

        # Benchmark Buy & Hold metrics
        bench_final = equity_df["benchmark_equity"].iloc[-1]
        bench_return = (bench_final - init_cap) / init_cap

        # Trade metrics
        trade_count = len(trades)
        if trade_count > 0:
            trade_pnls = [t.net_pnl for t in trades]
            trade_returns = [t.return_pct for t in trades]
            winning_trades = [t for t in trades if t.net_pnl > 0]
            losing_trades = [t for t in trades if t.net_pnl < 0]

            win_rate = len(winning_trades) / trade_count
            gross_gains = sum(t.gross_pnl for t in winning_trades)
            gross_losses = abs(sum(t.gross_pnl for t in losing_trades))
            profit_factor = round(gross_gains / gross_losses, 2) if gross_losses > 0 else float("inf")

            total_fees = sum(t.entry_fee + t.exit_fee for t in trades)
            total_turnover = sum(t.entry_notional + t.exit_notional for t in trades)
            avg_trade_pnl = float(np.mean(trade_pnls))
            avg_trade_return = float(np.mean(trade_returns))
        else:
            win_rate = 0.0
            profit_factor = 0.0
            total_fees = 0.0
            total_turnover = 0.0
            avg_trade_pnl = 0.0
            avg_trade_return = 0.0

        return {
            "initial_capital": init_cap,
            "final_equity": round(final_equity, 2),
            "net_profit": round(net_profit, 2),
            "total_return_pct": round(total_return * 100.0, 2),
            "cagr_pct": round(cagr * 100.0, 2),
            "max_drawdown_pct": round(max_drawdown * 100.0, 2),
            "annualized_volatility_pct": round(vol_ann * 100.0, 2),
            "sharpe_ratio": round(sharpe, 2),
            "sortino_ratio": round(sortino, 2),
            "benchmark_buy_and_hold_return_pct": round(bench_return * 100.0, 2),
            "alpha_vs_benchmark_pct": round((total_return - bench_return) * 100.0, 2),
            "total_trades": trade_count,
            "winning_trades": len([t for t in trades if t.net_pnl > 0]),
            "losing_trades": len([t for t in trades if t.net_pnl < 0]),
            "win_rate_pct": round(win_rate * 100.0, 2),
            "profit_factor": profit_factor,
            "total_frictional_fees_paid": round(total_fees, 2),
            "total_volume_turnover": round(total_turnover, 2),
            "average_trade_pnl": round(avg_trade_pnl, 2),
            "average_trade_return_pct": round(avg_trade_return * 100.0, 4),
            "backtest_duration_days": round(days, 1),
            "start_timestamp": str(start_dt),
            "end_timestamp": str(end_dt)
        }

    def save_results(
        self,
        trades: List[TradeRecord],
        equity_df: pd.DataFrame,
        metrics: Dict[str, Any],
        output_dir: str = "data/backtesting"
    ):
        """
        Save trade log, equity curve, summary metrics, and visual performance charts.
        """
        out_p = Path(output_dir)
        out_p.mkdir(parents=True, exist_ok=True)

        # 1. Save Trade Log
        trades_dicts = [asdict(t) for t in trades]
        trades_df = pd.DataFrame(trades_dicts)
        if len(trades_df) > 0:
            trades_df.to_csv(out_p / "trades.csv", index=False)
            trades_df.to_parquet(out_p / "trades.parquet", index=False)

        # 2. Save Equity Curve
        equity_df.to_csv(out_p / "equity_curve.csv", index=False)
        equity_df.to_parquet(out_p / "equity_curve.parquet", index=False)

        # 3. Save Summary JSON
        with open(out_p / "backtest_summary.json", "w") as f:
            json.dump(metrics, f, indent=2)

        # 4. Generate Visualization Chart
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt

            fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 10), sharex=True, gridspec_kw={'height_ratios': [3, 1.5, 1]})

            # Subplot 1: Portfolio Equity vs Benchmark
            ax1.plot(equity_df["timestamp"], equity_df["total_equity"], label="Strategy Equity (Model)", color="#1f77b4", linewidth=1.8)
            ax1.plot(equity_df["timestamp"], equity_df["benchmark_equity"], label="Buy-and-Hold Benchmark", color="#ff7f0e", linestyle="--", linewidth=1.5)
            ax1.set_ylabel("Portfolio Value (INR)")
            ax1.set_title("SmartStock - Historical Strategy Backtest (Out-of-Sample Test Period)")
            ax1.grid(True, linestyle=":", alpha=0.6)
            ax1.legend(loc="upper left")

            # Subplot 2: Underwater Drawdown
            ax2.fill_between(equity_df["timestamp"], equity_df["drawdown"] * 100.0, 0, color="#d62728", alpha=0.35)
            ax2.plot(equity_df["timestamp"], equity_df["drawdown"] * 100.0, color="#d62728", linewidth=1.2)
            ax2.set_ylabel("Drawdown (%)")
            ax2.grid(True, linestyle=":", alpha=0.6)

            # Subplot 3: Open Positions Count
            ax3.plot(equity_df["timestamp"], equity_df["open_positions_count"], color="#2ca02c", linewidth=1.2)
            ax3.set_ylabel("Active Trades")
            ax3.set_xlabel("Timestamp")
            ax3.set_ylim(-0.5, 5.5)
            ax3.grid(True, linestyle=":", alpha=0.6)

            plt.tight_layout()
            chart_path = out_p / "equity_curve.png"
            plt.savefig(chart_path, dpi=200)
            plt.close(fig)
            print(f"[OK] Saved performance chart to: {chart_path}")
        except Exception as e:
            print(f"[WARN] Could not render matplotlib chart: {e}")

        print(f"[OK] Saved trades to:        {out_p / 'trades.parquet'}")
        print(f"[OK] Saved equity curve to:  {out_p / 'equity_curve.parquet'}")
        print(f"[OK] Saved summary to:       {out_p / 'backtest_summary.json'}")


def run_backtest_pipeline(
    features_dir: str = "data/features/stock_features",
    labels_dir: str = "data/labels/stock_labels",
    model_dir: str = "models/saved/best_model",
    output_dir: str = "data/backtesting",
    eval_universe: str = "future_test",
    initial_capital: float = 1_000_000.0,
    max_positions: int = 5,
    sample_fraction: Optional[float] = None
) -> Dict[str, Any]:
    """
    Load test dataset, generate model predictions using saved model, and run backtest simulation.
    """
    start_time = time.time()
    print("=" * 80)
    print("SmartStock - Historical Strategy Backtesting Engine")
    print("=" * 80)
    print(f"Evaluation Universe: {eval_universe}")
    print(f"Initial Capital:     INR {initial_capital:,.2f}")
    print(f"Max Concurrent Pos:  {max_positions}")
    print(f"Model Directory:     {model_dir}")

    spark = SparkSessionManager.get_or_create(app_name="SmartStock-Backtester")

    try:
        # 1. Load Split Test Data
        print("\n[1/4] Loading out-of-sample test dataset...")
        splitter = DataSplitter(spark)
        joined_df, feature_cols = splitter.load_joined_dataset(features_dir, labels_dir)
        splits = splitter.split(joined_df)

        if eval_universe == "unseen_test":
            target_test_df = splits["unseen_test"]
            print("  [Universe] Selected Unseen-Stock Holdouts (Design A)")
        else:
            target_test_df = splits["future_test"]
            print("  [Universe] Selected Future-Period Out-of-Sample (Design B, 2025-2026)")

        if sample_fraction and 0.0 < sample_fraction < 1.0:
            target_test_df = target_test_df.sample(withReplacement=False, fraction=sample_fraction, seed=42)

        # 2. Run Model Inference (using saved pipeline and tuned threshold)
        print("\n[2/4] Generating predictions using saved Spark MLlib PipelineModel...")
        predictor = ModelPredictor(model_dir)
        tuned_threshold = predictor.threshold
        print(f"  [OK] Loaded model with validation-tuned threshold: {tuned_threshold:.2f}")

        # Predict probabilities
        scored_df = predictor.predict(target_test_df)

        # Extract only columns needed for backtesting simulation into Pandas
        feed_cols = ["timestamp", "symbol", "open", "high", "low", "close", "prob_positive"]
        print("\n[3/4] Exporting feed for chronological portfolio simulation...")
        feed_pandas = scored_df.select(*feed_cols).toPandas()
        print(f"  [OK] Exported {len(feed_pandas):,} candle observations.")

        # 3. Execute Simulation
        print("\n[4/4] Executing chronological portfolio simulation with frictional accounting...")
        cfg = BacktestConfig(
            initial_capital=initial_capital,
            max_open_positions=max_positions
        )
        engine = BacktestEngine(cfg)
        trades, equity_df, metrics = engine.run_simulation(
            feed_pandas,
            recommendation_threshold=tuned_threshold
        )

        # 4. Save Artifacts
        engine.save_results(trades, equity_df, metrics, output_dir=output_dir)

        duration = time.time() - start_time
        metrics["pipeline_duration_seconds"] = round(duration, 2)

        print("\n" + "=" * 80)
        print("STRATEGY BACKTEST RESULTS SUMMARY")
        print("=" * 80)
        print(f"Duration:             {duration:.2f}s")
        print(f"Total Return:         {metrics['total_return_pct']:+.2f}%")
        print(f"CAGR:                 {metrics['cagr_pct']:+.2f}%")
        print(f"Benchmark Return:     {metrics['benchmark_buy_and_hold_return_pct']:+.2f}%")
        print(f"Alpha vs Benchmark:   {metrics['alpha_vs_benchmark_pct']:+.2f}%")
        print(f"Max Drawdown:         {metrics['max_drawdown_pct']:.2f}%")
        print(f"Sharpe Ratio:         {metrics['sharpe_ratio']:.2f}")
        print(f"Total Trades:         {metrics['total_trades']:,}")
        print(f"Win Rate:             {metrics['win_rate_pct']:.2f}%")
        print(f"Profit Factor:        {metrics['profit_factor']:.2f}")
        print(f"Total Fees Paid:      INR {metrics['total_frictional_fees_paid']:,.2f}")
        print("=" * 80)

        return metrics

    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description="SmartStock - Strategy Backtesting Engine")
    parser.add_argument("--features-dir", type=str, default="data/features/stock_features")
    parser.add_argument("--labels-dir", type=str, default="data/labels/stock_labels")
    parser.add_argument("--model-dir", type=str, default="models/saved/best_model")
    parser.add_argument("--output-dir", type=str, default="data/backtesting")
    parser.add_argument("--universe", type=str, choices=["future_test", "unseen_test"], default="future_test")
    parser.add_argument("--initial-capital", type=float, default=1_000_000.0)
    parser.add_argument("--max-positions", type=int, default=5)
    parser.add_argument("--sample-fraction", type=float, default=None)
    args = parser.parse_args()

    run_backtest_pipeline(
        features_dir=args.features_dir,
        labels_dir=args.labels_dir,
        model_dir=args.model_dir,
        output_dir=args.output_dir,
        eval_universe=args.universe,
        initial_capital=args.initial_capital,
        max_positions=args.max_positions,
        sample_fraction=args.sample_fraction
    )


if __name__ == "__main__":
    main()
