from pathlib import Path

from wrc_pipeline.transform import extract_decision_html, processed_key

CASE_PAGE = b"""
<html><body>
<header><nav><a href="/">Home</a></nav></header>
<div class="container mb-4">
  <div class="col-sm-3"><a href="/en/search/">Return to Search</a></div>
  <div class="col-sm-9">
    <h1>ADJ-00047352</h1>
    <p>The complainant was unfairly dismissed.</p>
    <script>track()</script>
  </div>
</div>
<footer>Cookie policy</footer>
</body></html>
"""


def test_extraction_keeps_the_decision_and_drops_the_site_chrome():
    html, quality = extract_decision_html(CASE_PAGE, "ADJ-00047352")
    text = html.decode()

    assert "The complainant was unfairly dismissed." in text
    assert "<title>ADJ-00047352</title>" in text
    for chrome in ("Home", "Return to Search", "track()", "Cookie policy"):
        assert chrome not in text
    assert quality["extraction"] == "content-selector"


def test_extraction_falls_back_to_the_body_and_flags_short_content():
    html, quality = extract_decision_html(b"<html><body><p>hello</p></body></html>", "x")

    assert b"<p>hello</p>" in html
    assert quality["extraction"] == "fallback-body"
    assert "warning" in quality


def test_processed_key_renames_to_a_safe_identifier():
    def key(identifier):
        return processed_key(
            {
                "_id": "/en/cases/2024/january/adj-00047352.html",
                "identifier": identifier,
                "file_path": "labour-court/2024-01-01/adj-00047352.html",
            },
            ".html",
        )

    assert key("ADJ-00047352") == "labour-court/2024-01-01/ADJ-00047352.html"
    assert key("IR - SC - 00001595") == "labour-court/2024-01-01/IR-SC-00001595.html"
    assert key("UD962/2014") == "labour-court/2024-01-01/UD962-2014.html"
    assert key("") == "labour-court/2024-01-01/adj-00047352.html"


def test_processed_key_can_add_the_page_name_for_colliding_identifiers():
    record = {
        "_id": "/en/cases/2025/january/adj-000549811.html",
        "identifier": "ADJ-00054981",
        "file_path": "workplace-relations-commission/2025-01-29/adj-000549811.html",
    }

    assert processed_key(record, ".html", with_slug=True) == (
        "workplace-relations-commission/2025-01-29/ADJ-00054981-adj-000549811.html"
    )


def test_extraction_on_a_real_case_page_keeps_the_decision_and_drops_the_site_chrome():
    raw = (Path(__file__).parent / "fixtures" / "case_page_html_decision.html").read_bytes()

    html, quality = extract_decision_html(raw, "PWD2355")
    text = html.decode()

    assert quality["extraction"] == "content-selector"
    assert quality["content_chars"] > 1000
    assert "PWD2355" in text
    for chrome in ("Return to Search", "Gaeilge", "cookie_policy", "Google Tag Manager"):
        assert chrome not in text
