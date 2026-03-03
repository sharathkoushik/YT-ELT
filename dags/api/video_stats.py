import json

import requests
import os
from pathlib import Path
from dotenv import load_dotenv
from datetime import date

from airflow.decorators import task
from airflow.models import Variable

# Always load .env relative to the script's file location
env_path = Path(__file__).resolve().parent / ".env"
print("Loading:", env_path)

load_dotenv(env_path)

api_key = os.getenv("api_key")
print("API KEY:", api_key)
maxResults = 50


@task 
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
    
    
@task
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
        

@task
def extract_video_data(video_ids):
    
    extracted_data=[]
    
    def batch_list(videoidlist, batchsize):
        for videoid in range(0, len(videoidlist), batchsize):
            yield videoidlist[videoid: videoid + batchsize]
            
            
    try: 
        
        for batch in batch_list(video_ids, maxResults):
            videoidstring = ",".join(batch)
            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={videoidstring}&key={api_key}"

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            for item in data.get('items',[]):
                videoid = item['id']
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']

                video_data = {
                    "video_id": videoid,
                    "title": snippet['title'],
                    "publishedAt": snippet['publishedAt'],
                    "duration": contentDetails['duration'],
                    "viewcount": statistics.get('viewCount',None),
                    "likecount": statistics.get('likeCount',None),
                    "commentcount": statistics.get('commentCount',None)
                
                }    
            
                extracted_data.append(video_data)
        return extracted_data
                    
    except requests.exceptions.RequestException as e:
        raise e
        

@task  
def save_to_json(extracted_data):
    file_path = f"./data/YT_data_{date.today()}.json"
    with open(file_path,"w", encoding="utf-8") as json_outfile:
        json.dump(extracted_data, json_outfile, indent =4, ensure_ascii=False)
        
                
if __name__ == "__main__":

    playlistid = get_playlist_id()
    video_ids = getvideoids(playlistid)
    video_data = (extract_video_data(video_ids))
    save_to_json(video_data)
    
    
    
    

    
    
    
        
