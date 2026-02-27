import os
from dotenv import load_dotenv
import requests
from scripts.utils.base_etl import BaseEtl 
from scripts.utils.db_config import YOUTUBE_API_PLAYLIST_SEARCH_PHRASES
load_dotenv('../../.env')
class YoutubeVideosEtl(BaseEtl):
    def __init__(self, source_system, urls, db_client=None):
        super().__init__(source_system, urls, db_client)
    
    def _get_data(self, urls, watermark):
        api_key = os.getenv("YOUTUBE_API_KEY")

        playlist_parameters = {
            "part" : "id",
            "type" : "playlist",
            "fields" : "items/id/playlistId",
            "q" : YOUTUBE_API_PLAYLIST_SEARCH_PHRASES[0],
            "maxResulst" : 5,
            "key" : api_key
        }
        response = requests.get(urls[0], playlist_parameters)
        playlist_ids = [item["id"]["playlistId"] for item in response.json()['items']]
        print(playlist_ids)
        
        video_ids = []
        playlist_items_parameters = {
            "part" : "snippet",
            "fields" : "items(id,snippet(publishedAt))",
            "playlistId" : "",
            "maxResults" : 50,
            "key" : api_key
        }
        for id in playlist_ids:
            playlist_items_parameters["playlistId"] = id
            response = requests.get(urls[1], playlist_items_parameters)
            print(response.json())
        

if __name__ == '__main__':
    etl = YoutubeVideosEtl("youtube_api",
                            ["https://www.googleapis.com/youtube/v3/search",
                            "https://www.googleapis.com/youtube/v3/playlistItems"]
                        )
    etl.run()
    