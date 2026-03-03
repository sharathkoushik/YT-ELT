

from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, getvideoids, extract_video_data, save_to_json

local_tz = pendulum.timezone("Asia/Kolkata")
default_args = {
     
    "owner": "dataengineers",
    "email": "test@test.com",
    "start_date": datetime(2025,1,1,tzinfo=local_tz)
    
    
    
}

with DAG(
    
    dag_id='produce_json',
    default_args = default_args,
    schedule = '0 14 * * *',
    catchup=False
    
)as dag:
    
    playlistid = get_playlist_id()
    video_ids = getvideoids(playlistid)
    video_data = (extract_video_data(video_ids))
    save_to_json_task = save_to_json(video_data)
    
    
    playlistid >> video_ids >> video_data >> save_to_json_task