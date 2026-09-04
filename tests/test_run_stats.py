from wrc_pipeline.scraper.run_stats import RunStats


def test_summary_reports_records_missing_from_an_unstable_listing():
    stats = RunStats()
    stats.slice("2024-01", "labour-court").found = 45
    stats.slice("2024-01", "labour-court").listed = 44
    stats.slice("2024-01", "labour-court").duplicate_rows = 1
    stats.slice("2024-01", "labour-court").downloaded = 44
    stats.add_failure("2024-02", "labour-court", "https://example/x", "HTTP 500", "LCR1")

    summary = stats.summary()

    assert summary["slices"][0]["missing_from_listing"] == 1
    assert summary["totals"]["scraped"] == 44
    assert summary["totals"]["failed"] == 1
    assert summary["failures"] == [
        {
            "partition": "2024-02",
            "body": "labour-court",
            "url": "https://example/x",
            "error": "HTTP 500",
            "identifier": "LCR1",
        }
    ]
