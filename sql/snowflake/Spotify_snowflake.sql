CREATE DATABASE spotify_db;

CREATE OR REPLACE storage integration s3_init
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::869446329669:role/spotify-snowflake-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-etl-project-atulkumar')
    COMMENT = 'Creating connection to s3';

DESC integration s3_init;

CREATE OR REPLACE file format csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if =('NULL','null')
    empty_field_as_null = TRUE;

CREATE OR REPLACE stage spotify_db.public.spotify_stage
    URL = 's3://spotify-etl-project-atulkumar/transformed_data/'
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = csv_fileformat;

LIST @spotify_db.public.spotify_stage;

CREATE OR REPLACE TABLE tbl_album(
    album_id STRING,
    name STRING,
    release_date DATE ,
    total_tracks INT,
    url STRING
    
);

CREATE OR REPLACE TABLE tbl_artists(
    artist_id STRING,
    artist_name STRING,
    external_url STRING 
    
);

CREATE OR REPLACE TABLE tbl_songs(
    song_id STRING,
    song_name STRING,
    duration_ms INT,
    url STRING,
    popularity INT,
    song_added DATE,
    album_id STRING,
    artist_id STRING
);


COPY INTO tbl_songs
FROM @spotify_stage/songs_data/songs_transformed_2025-08-07/run-1754607402053-part-r-00000;


COPY INTO tbl_artists
FROM @spotify_stage/artist_data/artist_transformed_2025-08-07/run-1754607404815-part-r-00000;

COPY INTO tbl_album
FROM @spotify_stage/album_data/album_transformed_2025-08-07/run-1754603007901-part-r-00000;

-- create snowpipe 

CREATE OR REPLACE SCHEMA pipe;

CREATE OR REPLACE pipe spotify_db.pipe.tbl_songs_pipe
auto_ingest = TRUE
AS
COPY INTO spotify_db.public.tbl_songs
FROM @spotify_db.public.spotify_stage/songs_data/;


CREATE OR REPLACE pipe spotify_db.pipe.tbl_artists_pipe
auto_ingest = TRUE
AS
COPY INTO spotify_db.public.tbl_artists
FROM @spotify_db.public.spotify_stage/artist_data/;


CREATE OR REPLACE pipe spotify_db.pipe.tbl_album_pipe
auto_ingest = TRUE
AS
COPY INTO spotify_db.public.tbl_album
FROM @spotify_db.public.spotify_stage/album_data/;

DESC pipe spotify_db.pipe.tbl_album_pipe

SELECT count(*) FROM spotify_db.public.tbl_album;

SELECT SYSTEM$PIPE_STATUS('spotify_db.pipe.tbl_album_pipe')