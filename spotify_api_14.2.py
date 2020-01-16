import sys
import os
import logging
import boto3
import requests
import base64
import json
import pymysql
from datetime import datetime


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
    cursor.execute("SELECT id FROM artist")
    
    dt = datetime.utcnow().strftime("%Y-%m-%d")
    print(dt)

    sys.exit(0)

    with open('top_tracks.json', 'w') as f:
        for i in top_tracks:
            json.dump(i,f)
            f.write(os.linesep)

    s3 = boto3.resource('s3')
    object = s3.object('spotify-artists-api', 'dt={}/top-tracks.json'.format(dt))

    ## S3 import



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