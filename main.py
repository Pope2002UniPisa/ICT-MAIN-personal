from src.simulate_missing_markets import simulate_missing_markets
from src.cross_market_prepare import prepare_cross_market_inputs
from src.cross_market_engine import build_opportunity_tables
from src.trade_simulation import simulate_trading
from src.validate_simulation import validate_simulation


def main():
    print("[MAIN] Step 1 - Simulating missing markets...")
    simulate_missing_markets(seed=42)

    print("[MAIN] Step 2 - Building cross-market inputs...")
    prepare_cross_market_inputs()

    print("[MAIN] Step 3 - Detecting cross-market opportunities...")
    build_opportunity_tables()

    print("[MAIN] Step 4 - Running trade simulation...")
    simulate_trading()

    print("[MAIN] Step 5 - Validating simulation...")
    validate_simulation()

    print("[MAIN] Done.")

if __name__ == "__main__":
    main()

# find data/processed -mindepth 2 -name "*.parquet" | while read f; do python src/open_parquet_preview.py "$f"; done
