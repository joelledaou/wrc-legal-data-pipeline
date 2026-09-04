import hashlib

from wrc_pipeline.scraper.pipelines import HTML_COMMENT_RE, object_key
from wrc_pipeline.scraper.spiders.decisions import DecisionsSpider


def test_hash_ignores_server_comments():
    first = b"<html><body>decision</body></html><!-- Elapsed time: 12ms -->"
    second = (
        b"<!-- cached or not being index.aspx page --><html><body>decision</body></html><!-- Elapsed time: 98ms -->"
    )

    hashes = {hashlib.sha256(HTML_COMMENT_RE.sub(b"", page)).hexdigest() for page in (first, second)}

    assert len(hashes) == 1


def test_object_key_is_named_after_the_case_page():
    record = {
        "body": "employment-appeals-tribunal",
        "partition_date": "2009-12-01",
        "doc_url": "https://www.workplacerelations.ie/en/cases/2009/december/pw42_2009.html",
    }

    assert object_key(record, ".pdf") == "employment-appeals-tribunal/2009-12-01/pw42_2009.pdf"


def test_corrupt_documents_are_detected():
    problem = DecisionsSpider._content_problem

    assert problem(b"%PDF-1.4 ... %%EOF", ".pdf") is None
    assert problem(b"<html><body>not a pdf</body></html>", ".pdf") == "missing %PDF header"
    assert problem(b"%PDF-1.4 truncated", ".pdf") == "missing %%EOF trailer (truncated download)"
    assert problem(b"<!DOCTYPE html><html></html>", ".html") is None
    assert problem(b"Service unavailable", ".html") == "no HTML document markup found"
    assert problem(b"", ".doc") == "empty response body"
