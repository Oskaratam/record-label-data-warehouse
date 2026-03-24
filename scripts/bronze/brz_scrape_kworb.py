import requests
import re
import datetime as dt
from enum import Enum
from scripts.utils.base_etl import BaseEtl
from bs4 import BeautifulSoup, Tag
from scripts.utils.db_client import DatabaseClient
from scripts.utils.decorators import with_metadata
from scripts.utils.etl_config import KWORB_WORLDWIDE_CHART_URL, KWORB_RADIO_CHART_URL
from scripts.exceptions import WatermarkInvalidException

class KworbOption(Enum):
    WorldwideChart = 'worldwide_chart'
    RadioChart = 'radio_chart'

class KworbScraper(BaseEtl): 

    def __init__(self, chartType: KworbOption = KworbOption.WorldwideChart, 
                 source_system: str = 'kworb.net',
                data_category: str = 'worldwide_chart',
                db_client: DatabaseClient | None = None):
        self.chartType = chartType
        data_category = str(chartType.value)
        super().__init__(source_system, data_category, db_client)
  

    @with_metadata
    def _get_data(self, watermark: str | None = None) -> dict:
        return self.scrape_chart(self.chartType, watermark)
        
    @classmethod
    def get_html(cls, url) -> bytes:
        response = requests.get(url)
        return response.content

    def scrape_chart(self, chartType: KworbOption, watermark: str | None =None) -> dict :
        match chartType:
            case chartType.WorldwideChart:
                html_content : bytes = KworbScraper.get_html(KWORB_WORLDWIDE_CHART_URL)
            case chartType.RadioChart:
                html_content : bytes = KworbScraper.get_html(KWORB_RADIO_CHART_URL)

        soup = BeautifulSoup(html_content, 'html.parser') 
        chart = soup.find_all('table')[0]
        
        page_title = soup.find("span", class_="pagetitle")
        scrape_date_str = None

        if page_title:
            scrape_date = None

            if page_title:
                scrape_date_str = re.search('[0-9]{2,4}/[0-9]{2,4}/[0-9]{2,4}', page_title.get_text())
                if scrape_date_str != None:
                    scrape_date_str = scrape_date_str.group()
                    scrape_date = dt.datetime.strptime(scrape_date_str, "%Y/%m/%d")

            if watermark and scrape_date:
                if scrape_date > dt.datetime.strptime(watermark, "%Y/%m/%d"):
                    return {"raw_data" : [str(chart.thead) + str(chart.tbody)], "new_watermark": scrape_date_str}
                else:
                    raise WatermarkInvalidException()  #To not load data if newer data is in db
    
        return {"raw_data" : [str(chart.thead) + str(chart.tbody)], "new_watermark": scrape_date_str}                

        
if __name__ == "__main__":
    scraper = KworbScraper()
    scraper.run()