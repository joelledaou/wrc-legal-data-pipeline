"""Parser tests against saved copies of real site pages (tests/fixtures)."""

from datetime import date
from pathlib import Path

import scrapy
from scrapy.http import HtmlResponse

from wrc_pipeline.partitions import Partition
from wrc_pipeline.scraper.items import DecisionItem
from wrc_pipeline.scraper.spiders.decisions import DecisionsSpider

FIXTURES = Path(__file__).parent / "fixtures"
BASE_URL = "https://www.workplacerelations.ie"
PARTITION = Partition("2024-01", date(2024, 1, 1), date(2024, 1, 31))


def make_spider() -> DecisionsSpider:
    # force_refetch bypasses the "already stored?" lookup, so no database is needed.
    return DecisionsSpider(start_date="2024-01-01", end_date="2024-02-01", bodies="labour-court",
                           force_refetch="true")


def load(fixture: str, url: str) -> HtmlResponse:
    return HtmlResponse(url=url, body=(FIXTURES / fixture).read_bytes(), encoding="utf-8")


def test_first_search_page_fans_out_to_the_remaining_pages_and_one_request_per_row():
    spider = make_spider()
    response = load("search_page1.html", f"{BASE_URL}/en/search/?decisions=1&body=3&pageNumber=1")

    outputs = list(spider.parse_search_page(response, PARTITION, "labour-court", 3, page=1))

    search_requests = [r for r in outputs if r.callback == spider.parse_search_page]
    document_requests = [r for r in outputs if r.callback == spider.parse_document]
    assert [r.cb_kwargs["page"] for r in search_requests] == [2, 3, 4, 5]  # 45 results, 10 per page
    assert all("body=3" in r.url and "pageNumber=" in r.url for r in search_requests)
    assert len(document_requests) == 10
    assert spider.run_stats.slice("2024-01", "labour-court").found == 45
    assert spider.run_stats.slice("2024-01", "labour-court").listed == 10


def test_search_rows_yield_fully_populated_records():
    spider = make_spider()
    response = load("search_page1.html", f"{BASE_URL}/en/search/?decisions=1&body=3&pageNumber=1")

    requests = [r for r in spider.parse_search_page(response, PARTITION, "labour-court", 3, page=1)
                if r.callback == spider.parse_document]

    assert requests[0].cb_kwargs["record"] == {
        "record_id": "/en/cases/2024/february/lcr22912.html",
        "identifier": "LCR22912",
        "title": "LCR22912",
        "description": "SONOMA VALLEY (REPRESENTED BY ANNE O'CONNELL, SOLICITOR) AND A WORKER",
        "published_date": "2024-01-30",
        "body": "labour-court",
        "doc_url": f"{BASE_URL}/en/cases/2024/february/lcr22912.html",
        "partition_date": "2024-01-01",
        "partition_label": "2024-01",
    }
    assert all(r.cb_kwargs["record"]["identifier"] and r.cb_kwargs["record"]["published_date"] for r in requests)


def test_a_row_seen_twice_is_counted_as_a_duplicate_not_requested_again():
    spider = make_spider()
    response = load("search_page1.html", f"{BASE_URL}/en/search/?decisions=1&body=3&pageNumber=1")

    first = list(spider.parse_search_page(response, PARTITION, "labour-court", 3, page=1))
    second = list(spider.parse_search_page(response, PARTITION, "labour-court", 3, page=2))

    assert len([r for r in first if r.callback == spider.parse_document]) == 10
    assert second == []
    assert spider.run_stats.slice("2024-01", "labour-court").duplicate_rows == 10


def test_case_page_with_a_pdf_attachment_follows_the_attachment():
    spider = make_spider()
    response = load("case_page_with_pdf_attachment.html", f"{BASE_URL}/en/cases/2009/december/ud1567_2008.html")
    record = {"identifier": "34532", "body": "employment-appeals-tribunal", "partition_label": "2009-12-01"}

    outputs = list(spider.parse_document(response, record))

    assert len(outputs) == 1 and isinstance(outputs[0], scrapy.Request)
    request = outputs[0]
    # The page links the file twice (preview thumbnail with a query string, and download); one clean URL wins.
    assert request.url == f"{BASE_URL}/en/eat_import/2009/12/15216258-5815-41c9-8feb-978518fd00ca.pdf"
    assert request.cb_kwargs["record"]["attachment_url"] == request.url
    assert request.cb_kwargs["page_response"] is response
    assert request.dont_filter


def test_html_decision_page_becomes_the_document_itself():
    spider = make_spider()
    response = load("case_page_html_decision.html", f"{BASE_URL}/en/cases/2024/january/pwd2355.html")
    record = {"identifier": "PWD2355", "body": "labour-court", "partition_label": "2024-01"}

    outputs = list(spider.parse_document(response, record))

    assert len(outputs) == 1 and isinstance(outputs[0], DecisionItem)
    item = outputs[0]
    assert item["file_ext"] == ".html"
    assert item["file_url"] == response.url
    assert item["content"] == response.body
    assert "attachment_url" not in item  # the header/footer PDFs (cookie policy, search guide) are ignored
