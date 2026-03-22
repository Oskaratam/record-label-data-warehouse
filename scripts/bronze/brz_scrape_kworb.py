import requests
from scripts.utils.base_etl import BaseEtl
from bs4 import BeautifulSoup
from scripts.utils.etl_config import KWORB_WORLDWIDE_CHART_URL, KWORB_RADIO_CHART_URL

class KworbScraper(BaseEtl): 

    def __init__(self):
        pass

    def _get_data(self, watermark: str) -> dict:
        return self.scrape_worldwide_chart()
        
    @classmethod
    def get_html(cls, url) -> bytes:
        response = requests.get(url)
        if response.status_code == 200:
            return response.content
        else:
            return response.content

    def scrape_worldwide_chart(self) -> dict :
        html_content : bytes = KworbScraper.get_html(KWORB_WORLDWIDE_CHART_URL)
        soup = BeautifulSoup(html_content, 'html.parser') 
        chart = soup.find_all('table')[0]

        print(str(chart.thead) + str(chart.tbody))
        
        return {"raw_data" : str(chart.thead) + str(chart.tbody) , "new_watermark": None}

        





if __name__ == "__main__":
    scraper = KworbScraper()
    scraper.scrape_worldwide_chart()