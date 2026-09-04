import scrapy


class DecisionItem(scrapy.Item):
    record_id = scrapy.Field()  # document URL path, used as the Mongo _id
    identifier = scrapy.Field()  # the result heading, e.g. "ADJ-00047352" or "MN49/2009"
    title = scrapy.Field()
    ref_no = scrapy.Field()  # the site's "Ref no", an internal number for older bodies
    description = scrapy.Field()  # e.g. "Declan Holden V Ger Brennan Construction"
    published_date = scrapy.Field()  # ISO date
    body = scrapy.Field()  # labour-court, workplace-relations-commission, ...
    doc_url = scrapy.Field()  # case page linked from the search result
    file_url = scrapy.Field()  # where the stored bytes came from: doc_url or its attachment
    attachment_url = scrapy.Field()  # PDF/DOC linked from the case page, if any
    attachment_error = scrapy.Field()  # set when the attachment failed and the page was stored instead
    partition_date = scrapy.Field()  # ISO start date of the partition
    partition_label = scrapy.Field()  # e.g. "2024-01"

    # payload, dropped by the pipeline once stored
    content = scrapy.Field()
    content_type = scrapy.Field()
    file_ext = scrapy.Field()  # .html / .pdf / .doc / .docx
