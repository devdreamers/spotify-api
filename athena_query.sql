## database 생성
CREATE DATABASE IF NOT EXISTS production

## table 생성
CREATE EXTERNAL TABLE IF NOT EXISTS top_tracks(
  id string,
  artist_id string,
  name string,
  popularity int,
  external_url string
) PARTITIONED BY (dt string)
STORED AS PARQUET LOCATION 's3://spotify-artists-api/top-tracks/' tblproperties("parquet.compress"="snappy")

## 파티션 자동 추가
MSCK REPAIR TABLE top_tracks

## 조회 쿼리
SELECT * FROM top_tracks
WHERE CAST(dt AS date) >= CURRENT_DATE - INTERVAL '7' DAY
LIMIT 10


## table 생성
CREATE EXTERNAL TABLE IF NOT EXISTS audio_features(
  id string,
  danceability DOUBLE,
  energy DOUBLE,
  key int,
  loudness DOUBLE,
  mode int,
  speechiness DOUBLE,
  acousticness DOUBLE,
  instrumentalness DOUBLE
) PARTITIONED BY (dt string)
STORED AS PARQUET LOCATION 's3://spotify-artists-api/audio-features/' tblproperties("parquet.compress"="snappy")


## 파티션 자동 추가
MSCK REPAIR TABLE audio_features

## 조회 쿼리
SELECT * FROM audio_features
WHERE CAST(dt AS date) >= CURRENT_DATE - INTERVAL '7' DAY
LIMIT 10
