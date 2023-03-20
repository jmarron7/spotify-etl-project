import json
import boto3
import pandas as pd
import os
from datetime import datetime
from io import StringIO

# Initial transformation of Spotify data to only gather data about Albums
def album_data_transform(data):
    album_list =[]

    for row in data['items']:
        album_name = row['track']['album']['name']
        album_id = row['track']['album']['id']
        album_group = row['track']['album']['album_group']
        album_type = row['track']['album']['album_type']
        album_num_tracks = row['track']['album']['total_tracks']
        album_artists_list = []
        album_artists_id_list = []
        for idx,artist in enumerate(row['track']['album']['artists']):
            album_artists_list.append(row['track']['album']['artists'][idx]['name'])
            album_artists_id_list.append(row['track']['album']['artists'][idx]['id'])
        album_artists = ", ".join(album_artists_list)
        album_artists_ids = ", ".join(album_artists_id_list)
    
        release_date = row['track']['album']['release_date']
        album_image = row['track']['album']['images'][1]['url']
    
        album_data = {'album_id': album_id,
                      'album_name': album_name,
                      'album_artists': album_artists,
                      'album_artists_id': album_artists_ids,
                      'album_group': album_group,
                      'album_type': album_type,
                      'total_tracks': album_num_tracks,
                      'release_date': release_date,
                      'album_image': album_image
                    }
        album_list.append(album_data)
    return album_list

# Initial transformation of Spotify data to only gather data about Artists
def artist_data_transform(data):
    artist_list =[]

    for row in data['items']:
        for key, value in row.items():
            if key == "track":
                for artist in value['artists']:
                    artist_dict = {'artist_id': artist['id'],
                                   'artist_name': artist['name']}
                    artist_list.append(artist_dict)
    return artist_list

# Initial transformation of Spotify data to only gather data about Songs   
def song_data_transform(data):
    song_list =[]

    for row in data['items']:
        track_id = row['track']['id']
        track_name = row['track']['name']
        album_id = row['track']['album']['id']
        added_at = row['added_at']
    
        track_artists_id_list = []
        for idx,artist in enumerate(row['track']['artists']):
            track_artists_id_list.append(row['track']['artists'][idx]['id'])
        track_artists_ids = ", ".join(track_artists_id_list)
        
        track_number = row['track']['track_number']
        track_duration = row['track']['duration_ms']
        is_explicit = row['track']['explicit']
        popularity = row['track']['popularity']
    
        song_data = {'track_id': track_id,
                      'track_name': track_name,
                      'track_number': track_number,
                      'album_id': album_id,
                      'track_artists_id': track_artists_ids,
                      'track_duration': track_duration,
                      'added_at': added_at,
                      'is_explicit': is_explicit,
                      'popularity': popularity,
                    }
        song_list.append(song_data)
    return song_list
    
def lambda_handler(event, context):
    
    s3 = boto3.client('s3')
    Bucket = os.environ.get('s3_bucket')
    Key = "raw_data/data_to_process/"
    
    spotify_data = []
    spotify_keys = []
    
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == 'json':
            response = s3.get_object(Bucket=Bucket, Key=file_key)
            content = response['Body']
            json_object = json.loads(content.read())
            spotify_data.append(json_object)
            spotify_keys.append(file_key)
    
    for data in spotify_data:
        # Separate Spotify Data into sections (Album, Artist, and Song)
        album_list = album_data_transform(data)
        artist_list = artist_data_transform(data)
        song_list = song_data_transform(data)
        
        # Create DataFrames for each dataset
        album_df = pd.DataFrame.from_dict(album_list)
        album_df = album_df.drop_duplicates(subset=['album_id'])
        
        artist_df = pd.DataFrame.from_dict(artist_list)
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])
        
        song_df = pd.DataFrame.from_dict(song_list)
        album_df['release_date'] = pd.to_datetime(album_df['release_date'])
        song_df['added_at'] = pd.to_datetime(song_df['added_at'])
        
        # Save each dataset to their respective files and put into respective folders
        album_key = "transformed_data/album_data/album_transformed_" + str(datetime.now()) + ".csv"
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body=album_content)
        
        artist_key = "transformed_data/artist_data/artist_transformed_" + str(datetime.now()) + ".csv"
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)
        
        song_key = "transformed_data/song_data/song_transformed_" + str(datetime.now()) + ".csv"
        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index=False)
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=song_key, Body=song_content)
        
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed_data/' + key.split("/")[-1])
        s3_resource.Object(Bucket, key).delete()
            