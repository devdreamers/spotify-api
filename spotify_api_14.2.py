import sys
import os
import logging
import boto3
import requests
import base64
import json
import pymysql
from datetime import datetime
import pandas as pd
import jsonpath ## pip3 install jsonpath --user

## spotify access info
client_id = "55c44a45388048d88dd64df9ec727859"
client_secret = "754c1097a62a4d3fb4949b36be5ac517"
## DB연결정보
host = "fastcampus.c7qmculsyi1n.ap-northeast-2.rds.amazonaws.com"
port = 3306
username = "sean"
database = "production"
password = "password1"

def main():
    ## DB 연결 
    try:
        conn = pymysql.connect(host, user=username, passwd=password, db=database, port=port, use_unicode=True, charset='utf8')
        cursor = conn.cursor()
    except:
        logging.error("could not connect to RDS")
        sys.exit(1)

    ## access token을 받아옴
    headers = get_headers(client_id, client_secret)

    ## RDS 아티스트 ID를 가져오고
    cursor.execute("SELECT id FROM artist LIMIT 10")

    top_track_keys = {
        "id" : "id",
        "name" : "name",
        "popularity" : "popularity",
        "external_url" : "external_urls.spotify"
    }

    ## Top Tracks Spotify 가져오고
    top_tracks = []
    for (id, ) in cursor.fetchall():
        URL = "https://api.spotify.com/v1/artists/{}/top-tracks".format(id)
        params = {
            'country' : 'US'
        }
        r = requests.get(URL, params = params, headers = headers)
        raw = json.loads(r.text)

        for i in raw['tracks']:
            top_track = {}
            for k, v in top_track_keys.items():
                top_track.update({k : jsonpath.jsonpath(i,v)})
                top_track.update({'artist_id':id})
                top_tracks.append(top_track)

    ## track_ids
    track_ids = [i['id'][0] for i in top_tracks]
    
    #print(track_ids)
    #sys.exit(0)

    ## List of dictionaries
    top_tracks = pd.DataFrame(top_tracks)
    top_tracks.to_parquet('top-tracks.parquet', engine='pyarrow', compression='snappy')
    
    dt = datetime.utcnow().strftime("%Y-%m-%d")

    s3 = boto3.resource('s3')
    #object = s3.object('spotify-artists-api', 'dt={}/top-tracks.json'.format(dt)) #json 형식으로 저장
    object = s3.Object('spotify-artists-api', 'top-tracks/dt={}/top-tracks.parquet'.format(dt))    # parquet 형식으로 저장
    data = open('top-tracks.parquet', 'rb')
    object.put(Body=data)
    
    ## S3 import
    tracks_batch = [track_ids[i : i+100] for i in range(0, len(track_ids), 100)]

    audio_features = []
    for i in tracks_batch:
        ids = ','.join(i)
        URL = "https://api.spotify.com/v1/audio-features/?ids={}".format(ids)

        r = requests.get(URL, headers=headers)
        raw = json.loads(r.text)

        audio_features.extend(raw['audio_features'])

    audio_features = pd.DataFrame(audio_features)
    audio_features.to_parquet('audio-features.parquet', engine='pyarrow', compression='snappy')

    s3 = boto3.resource('s3')
    #object = s3.object('spotify-artists-api', 'dt={}/top-tracks.json'.format(dt)) #json 형식으로 저장
    object = s3.Object('spotify-artists-api', 'audio-features/dt={}/top-tracks.parquet'.format(dt))    # parquet 형식으로 저장
    data = open('audio-features.parquet', 'rb')
    object.put(Body=data)        





def get_headers(client_id, client_secret):
    endpoint = "https://accounts.spotify.com/api/token"
    encoded = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii')

    headers = {
        "Authorization" : "Basic {}".format(encoded)
    }

    payload = {
        "grant_type" : "client_credentials"
    }

    r = requests.post(endpoint, data=payload, headers=headers)

    access_token = json.loads(r.text)['access_token']

    headers = {
        "Authorization" : "Bearer {}".format(access_token)
    }

    return headers


if __name__=='__main__':
    main()   