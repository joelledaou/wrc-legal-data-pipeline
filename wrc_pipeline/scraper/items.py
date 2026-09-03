"""Item definition for one scraped decision/determination record."""

import scrapy


class DecisionItem(scrapy.Item):
    # Metadata extracted from the search results list
    record_id = scrapy.Field()        # URL path of the document — natural dedup key (Mongo _id)
    identifier = scrapy.Field()       # e.g. "ADJ-00047352" (Ref no on the site)
    title = scrapy.Field()            # heading text of the search result
    description = scrapy.Field()      # e.g. "Declan Holden V Ger Brennan Construction"
    published_date = scrapy.Field()   # ISO date the decision was published
    body = scrapy.Field()             # which tribunal: labour-court, workplace-relations-commission, ...
    doc_url = scrapy.Field()          # absolute link to the document (the "View Page" link)
    file_url = scrapy.Field()         # URL the stored bytes came from: doc_url, or its PDF/DOC attachment
    attachment_url = scrapy.Field()   # PDF/DOC linked from the page's decision content, when there is one
    attachment_error = scrapy.Field() # set when the attachment could not be fetched and the page was stored instead
    partition_date = scrapy.Field()   # ISO start date of the partition this record was scraped under
    partition_label = scrapy.Field()  # e.g. "2024-01"

    # Document payload (dropped by the pipeline after storing)
    content = scrapy.Field()          # raw response bytes (HTML page or PDF/DOC file)
    content_type = scrapy.Field()     # response Content-Type
    file_ext = scrapy.Field()         # .html / .pdf / .doc / .docx
