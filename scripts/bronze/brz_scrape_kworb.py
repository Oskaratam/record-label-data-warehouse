import requests
from scripts.utils.base_etl import BaseEtl
from bs4 import BeautifulSoup
from scripts.utils.etl_config import KWORB_WORLDWIDE_CHART_URL, KWORB_RADIO_CHART_URL

class KworbScraper(BaseEtl): 

    def __init__(self):
        pass
        
    @classmethod
    def get_html(cls, url):
        response = requests.get(url)
        if response.status_code == 200:
            return response.content

    def scrape_worldwide_chart(self):
        html_content = KworbScraper.get_html(KWORB_WORLDWIDE_CHART_URL)
        soup = BeautifulSoup(html_content, 'html.parser') # type: ignore
        chart = soup.find_all('table')[0]
        songs = chart.find_all("tr")
        for attribute in songs[1].find_all("td"): 
            print(attribute.text)



if __name__ == "__main__":
    scraper = KworbScraper()
    scraper.scrape_worldwide_chart()