import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    
    client_id = os.environ.get('spotify_client_id')
    client_secret = os.environ.get('spotify_client_secret')
    s3_bucket = os.environ.get('s3_bucket')
    data_to_process_key = os.environ.get('data_to_process_key')

    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF?si=3c950d065c274b3f"
    playlist_uri = playlist_link.split("/")[-1].split("?")[0]
    data = sp.playlist_tracks(playlist_uri)
    
    client = boto3.client('s3')
    
    filename = "spotify_raw_" + str(datetime.now()) + ".json"
    
    client.put_object(
        Bucket = s3_bucket,
        Key = data_to_process_key + filename,
        Body = json.dumps(data)
        )