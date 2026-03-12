import os
from dotenv import load_dotenv
import requests
from datetime import datetime
from scripts.utils.decorators import with_metadata
from scripts.utils.base_etl import BaseEtl 
from scripts.utils.db_client import DatabaseClient
from scripts.utils.etl_config import (YOUTUBE_API_PLAYLIST_SEARCH_PHRASES,
                                    YOUTUBE_PLAYLIST_ITEMS_URL,
                                    YOUTUBE_SEARCH_URL,
                                    YOUTUBE_VIDEO_DETAILS_URL)
load_dotenv('../../.env')

class YoutubeVideosEtl(BaseEtl):
    def __init__(self, source_system: str = 'youtube_api',
                data_category: str = 'youtube_videos',
                db_client: DatabaseClient | None = None):
        super().__init__(source_system, data_category, db_client)
        self.API_KEY = os.getenv("YOUTUBE_API_KEY")
    
    @with_metadata
    def _get_data(self,  watermark: str):
        playlist_videos = self._get_playlist_items(self._get_relevant_playlists(), watermark)
        video_details = self._get_video_details(playlist_videos)
        freshest_date = sorted(video_details, key = lambda x: x.get('snippet', {}).get('publishedAt', ""))[-1]['snippet']['publishedAt']
        return {"raw_data" : video_details, "new_watermark": freshest_date}
          
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
        response.raise_for_status()
        playlist_ids = [item["id"]["playlistId"] for item in response.json()['items']]
        print("!Playlists Loaded Successfully!")
        print("--------------------------------")
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

        if(not BaseEtl.is_valid_date(watermark)):
           print("!!!!!!!!!!!!!!WARNING!!!!!!!!!!!!!!!!!!")
           print("Loading videos without watermark value")
           print()

        for id in playlist_ids:
                playlist_items_parameters["playlistId"] = id
                response = requests.get(YOUTUBE_PLAYLIST_ITEMS_URL, playlist_items_parameters)
                response.raise_for_status()
                if(self.is_valid_date(watermark)):
                    watermark_datetime = datetime.fromisoformat(watermark)
                    videos.extend(video["snippet"]["resourceId"]["videoId"] for video in response.json()["items"]
                            if datetime.fromisoformat(
                                video["snippet"]["publishedAt"]
                                ) > watermark_datetime
                            )
                else:
                    videos.extend(video["snippet"]["resourceId"]["videoId"] for video in response.json()["items"])
        print("!Playlist Items Loaded Successfully!")
        print("--------------------------------")
        return videos
    
    def _get_video_details(self, video_ids: list[str]) -> list[dict]:
        video_details = []
        video_details_params = {
            "id" : "",
            "part" : "snippet,contentDetails,statistics,topicDetails",
            "fields" : "items(snippet(publishedAt,title,categoryId),contentDetails(duration),statistics,topicDetails)",
            "key" : self.API_KEY 
        }
        
        for i in range(0, len(video_ids), 50):
            batch_ids = video_ids[i:i+50]
            video_details_params["id"] = ','.join(batch_ids)
            response = requests.get(YOUTUBE_VIDEO_DETAILS_URL, video_details_params)
            response.raise_for_status()
            video_details.extend(response.json()['items'])
        print("!Video Details Loaded Successfully!")
        print("--------------------------------")
        return video_details
      

if __name__ == '__main__':
    etl = YoutubeVideosEtl()
    etl.run()
    