""" SEC scraping for 13-f Fillings """
import logging
import scrapy
from bs4 import BeautifulSoup
import pandas as pd

class SecSpider(scrapy.Spider):
    """ Spider collects links to sec site and downloads and parses contents of 13-f fillings """
    name = '13f_spider'

    iftt_file = pd.read_csv('SEC13_fFilings_1.csv', header=None)
    sample = iftt_file.head()
    links = sample[sample.columns[2]]
    start_urls = links.tolist()

    def parse(self, response):
        """ Parse filling page and download info table """
        logging.info(response.xpath("//*[contains(text(), 'INFORMATION TABLE')]/preceding-sibling::td/a[contains(text(), '.html')]/@href"))
        info_tables = response.xpath("//*[contains(text(), 'INFORMATION TABLE')]/preceding-sibling::td/a[contains(text(), '.html')]/@href").extract()
        for link in info_tables:
            absolute_url = response.urljoin(link)
            yield scrapy.Request(absolute_url, callback=self.parse_info_table)

    def parse_info_table(self, response):
        """ Parse info table page and convert to pandas df """
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
            positions.append(dic)           
        df = pd.DataFrame(positions)
        print(df)
