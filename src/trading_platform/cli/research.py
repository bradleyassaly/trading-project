from trading_platform.backtests.engine import run_backtest
from trading_platform.experiments.tracker import log_experiment

if __name__ == "__main__":
    stats = run_backtest("SPY")
    print(stats)

    exp_id = log_experiment(stats)
    print(f"Saved experiment: {exp_id}")