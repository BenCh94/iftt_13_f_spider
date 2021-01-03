## SEC 13-F Filling parser

#### Set up to parse file created by IFTT SEC webhooks

See IFTT info here[https://ifttt.com/sec].

Given a csv file of 13-F fillings the spider will crawl and return information table parsed to a pandas dataframe.

parsing code for 13-F from (Brian Caffey)[https://briancaffey.github.io/2018/01/30/reading-13f-sec-filings-with-python.html]

run:
```scrapy crawl 13f_spider```