from src.simulate_missing_markets import simulate_missing_markets
from src.cross_market_prepare import prepare_cross_market_inputs
from src.cross_market_engine import build_opportunity_tables


def main() -> None:
    print("[MAIN] Step 1 - Simulating missing markets...")
    simulate_missing_markets(seed=42)

    print("[MAIN] Step 2 - Building cross-market inputs...")
    prepare_cross_market_inputs()

    print("[MAIN] Step 3 - Detecting cross-market opportunities...")
    build_opportunity_tables()

    print("[MAIN] Done.")


if __name__ == "__main__":
    main()