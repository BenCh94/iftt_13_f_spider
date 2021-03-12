""" SEC scraping for 13-f Fillings """
import logging
import scrapy
import re
from bs4 import BeautifulSoup
import pandas as pd

class SecSpider(scrapy.Spider):
    """ Spider collects links to sec site and downloads and parses contents of 13-f fillings """
    name = '13f_spider'

    custom_settings = {
        'AUTOTHROTTLE_ENABLED': True,
    }

    def start_requests(self):
        self.fund_cik = '0001067983'
        search_url = f'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={self.fund_cik}&type=13F'
        yield scrapy.Request(search_url)

    def parse(self, response):
        print('parsing search results page')
        logging.info(response.xpath("//*[@id='documentsbutton']/@href"))
        filings = response.xpath("//*[@id='documentsbutton']/@href").extract()
        print(filings)
        print(len(filings))
        for i, link in enumerate(filings):
            print(link)
            print(i)
            absolute_url = response.urljoin(link)
            yield scrapy.Request(absolute_url, callback=self.parse_filing)


    def parse_filing(self, response):
        """ Parse filling page and download info table """
        logging.info(response.xpath("//*[contains(text(), 'INFORMATION TABLE')]/preceding-sibling::td/a[contains(text(), '.html')]/@href"))
        info_tables = response.xpath("//*[contains(text(), 'INFORMATION TABLE')]/preceding-sibling::td/a[contains(text(), '.html')]/@href").extract()
        date_string = response.xpath("//*[contains(text(), 'Period of Report')]/following-sibling::div[1]//text()").extract()[0]
        logging.info(date_string)
        for i, link in enumerate(info_tables):
            print(link)
            print(i)
            absolute_url = response.urljoin(link)
            yield scrapy.Request(absolute_url, callback=self.parse_info_table, meta={'file_name': f'{self.fund_cik}_{date_string}', 'date_string': date_string})

    def parse_info_table(self, response):
        """ Parse info table page and convert to pandas df """
        file_name = response.meta.get('file_name')
        soup = BeautifulSoup(response.text, 'lxml')
        rows = soup.find_all('tr')[11:]
        positions = []
        for row in rows:
            dic = {}
            position = row.find_all('td')
            dic["NAME_OF_ISSUER"] = position[0].text
            dic["TITLE_OF_CLASS"] = position[1].text
            dic["CUSIP"] = position[2].text
            dic["VALUE"] = int(position[3].text.replace(',', ''))*1000
            dic["SHARES"] = int(position[4].text.replace(',', ''))
            dic["SHARE TYPE"] = position[5].text
            dic["PUT_CALL"]=position[6].text
            dic["INVESTMENT_DISCRETION"] = position[7].text
            dic["DATE_STRING"] = response.meta.get('date_string')
            positions.append(dic)           
        df = pd.DataFrame(positions)
        print(df)
        df.to_csv(f'output_results/{file_name}_result.csv')


# Fund CIK
# Point 72 Asset Management 001603466
# Berkshire Hathaway 0001067983
# Citadel  0001423053
# Pershing Square 0001336528
