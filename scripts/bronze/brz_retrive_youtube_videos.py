import os
from dotenv import load_dotenv
import requests
from datetime import datetime
from scripts.utils.base_etl import BaseEtl 
from scripts.utils.db_client import DatabaseClient
from scripts.utils.db_config import YOUTUBE_API_PLAYLIST_SEARCH_PHRASES, YOUTUBE_PLAYLIST_ITEMS_URL, YOUTUBE_SEARCH_URL
load_dotenv('../../.env')

class YoutubeVideosEtl(BaseEtl):

    def __init__(self, source_system: str,  db_client: DatabaseClient | None = None):
        super().__init__(source_system, db_client)
        self.API_KEY = os.getenv("YOUTUBE_API_KEY")
    
    def _get_data(self,  watermark: str):
        playlist_videos = self._get_playlist_items(self._get_relevant_playlists(), watermark)
          
    def _get_relevant_playlists(self) -> list[str]:
        playlist_parameters = {
            "part" : "id",
            "type" : "playlist",
            "fields" : "items/id/playlistId",
            "q" : YOUTUBE_API_PLAYLIST_SEARCH_PHRASES[0],
            "maxResults" : 5,
            "key" : self.API_KEY 
        }
        response = requests.get(YOUTUBE_SEARCH_URL, playlist_parameters)
        print(response.json())
        playlist_ids = [item["id"]["playlistId"] for item in response.json()['items']]
        print(playlist_ids)
        return playlist_ids
    
    def _get_playlist_items(self, playlist_ids: list[str], watermark: str) -> list[str]:
        videos = []
        playlist_items_parameters = {
            "part" : "snippet",
            "fields" : "items(snippet(publishedAt,resourceId(videoId)))",
            "playlistId" : "",
            "maxResults" : 50,
            "key" : self.API_KEY 
        }
        is_valid_watermark = self.is_valid_date(watermark)
        watermark_dt: datetime | None = datetime.fromisoformat(watermark) if is_valid_watermark else None


        if(self.is_valid_date(watermark)):
            watermark_datetime = datetime.fromisoformat(watermark)

            for id in playlist_ids:
                playlist_items_parameters["playlistId"] = id
                response = requests.get(YOUTUBE_PLAYLIST_ITEMS_URL, playlist_items_parameters)
                videos.extend(video["id"] for video in response.json()["items"]
                            if datetime.fromisoformat(
                                video["snippet"]["publishedAt"]
                                ) > watermark_datetime
                            )
        else:
            print("!!!!!!!!!!!!!!WARNING!!!!!!!!!!!!!!!!!!")
            print("Loading videos without watermark value")
            for id in playlist_ids:
                playlist_items_parameters["playlistId"] = id
                response = requests.get(YOUTUBE_PLAYLIST_ITEMS_URL,
                                         playlist_items_parameters
                                        )
                videos.extend(video for video in response.json()["items"])
        print(videos)
        return videos
    
    


if __name__ == '__main__':
    etl = YoutubeVideosEtl("youtube_api")
    etl.run()
    