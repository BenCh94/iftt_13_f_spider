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
        iftt_file = pd.read_csv('SEC13_fFilings_4.csv', header=None)
        for index, row in iftt_file.iterrows():
            print(f'Row number: {index}')
            yield scrapy.Request(row[2], meta={'filer':row[1], 'date_string': row[0]})

    def parse(self, response):
        """ Parse filling page and download info table """
        logging.info(response.xpath("//*[contains(text(), 'INFORMATION TABLE')]/preceding-sibling::td/a[contains(text(), '.html')]/@href"))
        info_tables = response.xpath("//*[contains(text(), 'INFORMATION TABLE')]/preceding-sibling::td/a[contains(text(), '.html')]/@href").extract()
        filer = re.sub(r'[^\sa-zA-Z0-9]', '', response.meta.get('filer')).lower().strip().replace(" ", "_")
        date_string = response.meta.get('date_string')
        for i, link in enumerate(info_tables):
            print(link)
            print(i)
            absolute_url = response.urljoin(link)
            yield scrapy.Request(absolute_url, callback=self.parse_info_table, meta={'file_name': f'{filer}_{i}', 'date_string': date_string})

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
        df.to_csv(f'output_results/file_4/{file_name}_result.csv')