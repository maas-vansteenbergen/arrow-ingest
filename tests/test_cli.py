from __future__ import annotations

from arrow_ingest import LoadResult, cli


def test_runs_configured_pipeline_and_reports_result(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(
        cli,
        "run_pipeline",
        lambda config_path: LoadResult(
            rows_written=2,
            column_count=3,
            destination=f"resolved/{config_path}",
        ),
    )

    cli.main(["run", "pipelines/products.yaml"])

    assert capsys.readouterr().out == (
        "Wrote 2 rows with 3 columns to resolved/pipelines/products.yaml\n"
    )
