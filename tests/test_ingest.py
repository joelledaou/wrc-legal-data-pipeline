import hashlib

from wrc_pipeline.scraper.pipelines import HTML_COMMENT_RE, MongoMinioStorePipeline, object_key
from wrc_pipeline.scraper.run_stats import RunStats
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


class FakeCollection:
    def __init__(self, existing):
        self.existing, self.writes = existing, []

    def find_one(self, query, projection):
        return self.existing

    def update_one(self, query, update, upsert):
        self.writes.append(update["$set"]["file_path"])


class FakeMinio:
    def __init__(self):
        self.uploads = []

    def put_object(self, bucket, key, data, length, content_type):
        self.uploads.append(key)


def store(existing):
    pipeline = MongoMinioStorePipeline.__new__(MongoMinioStorePipeline)
    pipeline.cfg = type("Cfg", (), {"landing_bucket": "wrc-landing"})()
    pipeline.landing, pipeline.minio = FakeCollection(existing), FakeMinio()
    pipeline.crawler = type("Crawler", (), {"spider": type("Spider", (), {"run_stats": RunStats()})()})()
    record = {
        "record_id": "/en/cases/2009/december/ud676_2009.html",
        "partition_label": "2009-12-03",
        "body": "employment-appeals-tribunal",
        "partition_date": "2009-12-03",
        "identifier": "UD676/2009",
        "doc_url": "https://www.workplacerelations.ie/en/cases/2009/december/ud676_2009.html",
    }
    pipeline._store(record, b"%PDF-1.4 same content %%EOF", "application/pdf", ".pdf")
    return pipeline


def test_unchanged_content_is_not_re_uploaded_under_the_same_key():
    same_hash = hashlib.sha256(b"%PDF-1.4 same content %%EOF").hexdigest()
    key = "employment-appeals-tribunal/2009-12-03/ud676_2009.pdf"

    pipeline = store({"file_hash": same_hash, "file_path": key})

    assert pipeline.minio.uploads == []
    assert pipeline.landing.writes == [key]


def test_unchanged_content_is_uploaded_again_when_its_key_moves_to_another_partition():
    same_hash = hashlib.sha256(b"%PDF-1.4 same content %%EOF").hexdigest()

    pipeline = store({"file_hash": same_hash, "file_path": "employment-appeals-tribunal/2009-12-01/ud676_2009.pdf"})

    assert pipeline.minio.uploads == ["employment-appeals-tribunal/2009-12-03/ud676_2009.pdf"]
    assert pipeline.landing.writes == ["employment-appeals-tribunal/2009-12-03/ud676_2009.pdf"]
