import requests
import os
from pathlib import Path
from dotenv import load_dotenv

# Always load .env relative to the script's file location
env_path = Path(__file__).resolve().parent / ".env"
print("Loading:", env_path)

load_dotenv(env_path)

api_key = os.getenv("api_key")
print("API KEY:", api_key)

def get_playlist_id():
    
    try: 
        

        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle=MrBeast&key={api_key}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        # print(json.dumps(data,indent = 4))

        channel_items = data["items"][0]
        channel_playlist_id = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]
        print(channel_playlist_id)
        return channel_playlist_id
        
    except requests.exceptions.RequestException as e:
        raise e
    
    
    
def getvideoids(playlistid):
    
    
    videoids=[]
    pageToken = None

    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults=50&playlistId={playlistid}&key={api_key}"

    try:
        
        while True:
            
            url=base_url
            
            if pageToken:
                url += f"&pageToken={pageToken}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
        
            for item in data.get("items", []):
                videoid = item["contentDetails"]["videoId"]
                videoids.append(videoid)
                
            pageToken=data.get("nextPageToken")
            
            if not pageToken:
                break
            
        return videoids
            
    except requests.exceptions.RequestException as e:
        raise e
        
        
if __name__ == "__main__":

    playlistid=get_playlist_id()
    getvideoids(playlistid)
    
    
    
        
