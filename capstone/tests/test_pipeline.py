
import pytest

import src.capstone.pipeline as pipeline


def test_pipeline_runs_stages_in_order(monkeypatch):
    calls = []

    monkeypatch.setattr(
        pipeline, "ingest_bronze",
        lambda: calls.append("bronze"),
    )
    monkeypatch.setattr(
        pipeline, "run_silver_validation",
        lambda: calls.append("silver"),
    )
    monkeypatch.setattr(
        pipeline, "build_gold",
        lambda: calls.append("gold"),
    )

    pipeline.run_pipeline()

    assert calls == ["bronze", "silver", "gold"]


def test_pipeline_stops_when_bronze_fails(monkeypatch):
    calls = []

    def fail_bronze():
        calls.append("bronze")
        raise RuntimeError("Simulated Bronze failure")

    monkeypatch.setattr(pipeline, "ingest_bronze", fail_bronze)
    monkeypatch.setattr(
        pipeline, "run_silver_validation",
        lambda: calls.append("silver"),
    )
    monkeypatch.setattr(
        pipeline, "build_gold",
        lambda: calls.append("gold"),
    )

    with pytest.raises(RuntimeError, match="Simulated Bronze failure"):
        pipeline.run_pipeline()

    assert calls == ["bronze"]


def test_pipeline_stops_when_silver_fails(monkeypatch):
    calls = []

    monkeypatch.setattr(
        pipeline, "ingest_bronze",
        lambda: calls.append("bronze"),
    )

    def fail_silver():
        calls.append("silver")
        raise RuntimeError("Simulated Silver failure")

    monkeypatch.setattr(
        pipeline, "run_silver_validation", fail_silver
    )
    monkeypatch.setattr(
        pipeline, "build_gold",
        lambda: calls.append("gold"),
    )

    with pytest.raises(RuntimeError, match="Simulated Silver failure"):
        pipeline.run_pipeline()

    assert calls == ["bronze", "silver"]
