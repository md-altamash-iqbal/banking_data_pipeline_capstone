
"""Orchestrate the Week 8 banking data pipeline."""

from src.capstone.bronze import ingest_bronze
from src.capstone.silver import run_silver_validation
from src.capstone.gold import build_gold


def run_pipeline():
    """Run Bronze, Silver, and Gold in dependency order."""
    print("\n========== BANKING CAPSTONE PIPELINE ==========")

    print("\n[1/3] Starting Bronze ingestion...")
    ingest_bronze()
    print("[1/3] Bronze ingestion completed.")

    print("\n[2/3] Starting Silver validation...")
    run_silver_validation()
    print("[2/3] Silver validation completed.")

    print("\n[3/3] Starting Gold publication...")
    build_gold()
    print("[3/3] Gold publication completed.")

    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    run_pipeline()
