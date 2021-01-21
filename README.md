# AWS Data Lake - Creating Tables with Spark

The goal of this project is to extract JSON formatted songplay data from an S3 data lake, transform the data using Spark, then store the tables back on the S3 data lake in the form of Parquet files (a columnar-storage file format).

Spark handles the majority of this work. Using a schema-on-read approach, the data is read into DataFrames, then the data is parsed to create fact and dimension tables housed in Parquet files. The table schemas are as follows:

**Fact** - `songplays`

```
songplays
|-- start_time: long
|-- user_id: string
|-- level: string
|-- song_id: string
|-- artist_id: string
|-- session_id: long
|-- location: string
|-- user_agent: string
|-- songplay_id: integer
|-- year: integer
|-- month: integer
```

**Dimensions** - `songs`, `artists`, `time`, `users`

```
songs                        artists
 |-- song_id: string          |-- artist_id: string
 |-- title: string            |-- name: string
 |-- duration: double         |-- location: string
 |-- year: integer            |-- latitude: double
 |-- artist_id: string        |-- longitude: double
```

```
time                         users
 |-- start_time: long         |-- user_id: string
 |-- hour: integer            |-- first_name: string
 |-- day: integer             |-- last_name: string
 |-- week: integer            |-- gender: string
 |-- weekday: integer         |-- level: string
 |-- year: integer
 |-- month: integer
```

## Files and S3

### Files

 `etl.py` - This is responsible for performing ETL with Spark on the data housed in S3. In order to run, this file must be executed with access to a Spark cluster. In this case, the code was executed on an EC2 instance generated using Amazon's [Elastic Map Reduce](https://aws.amazon.com/emr/) (EMR) service. 

**Additional Files and Directories**

`dl.cfg` - Holds AWS credentials to access S3

`/data` directory - holds the sub-directories `/song_data` and  `/log-data`. These directories can be used to test `etl.py` on small batches of files before switching to the much larger S3 versions of the directories. More details on file contents below.

### S3 Directories

`/song_data` - Contains songs from the [Million Song Dataset](http://millionsongdataset.com/). Files are JSON formatted, partitioned by the first three letters of the song's track id.

**Full File Path**

- `song_data/A/[A-Z]/[A-Z]/[unique-id].json` 
- `song_data/A/A/A/TRAAAAK128F9318786.json`

**File Contents**

```json
{
	"num_songs": 1,
	"artist_id": "ARD7TVE1187B99BFB1",
	"artist_latitude": null,
	"artist_longitude": null,
	"artist_location": "California - LA",
	"artist_name": "Casual",
	"song_id": "SOMZWCG12A8C13C480",
	"title": "I Didn't Mean To",
	"duration": 218.93179,
	"year": 0
}
```



`/log_data` - Contains user activity logs. Files are JSON formatted, partitioned by year and month.

**Full File Path**

- `log-data/[yyyy]/[mm]/[yyyy-mm-dd]-events.json`
- `log-data/2018/11/2018-11-01-events.json`

**File Contents**

```json
{
	"artist":null, 
	"auth":"LoggedIn",
	"firstName":"Walter",
	"gender":"M",
	"itemInSession":0,
	"lastName":"Frye",
	"length":null,
	"level":"free",
	"location":"San Francisco-Oakland-Hayward, CA",
	"method":"GET",
	"page":"Home",
	"registration":1540919166796.0,
	"sessionId":38,"song":null,
	"status":200,
	"ts":1541105830796,
	"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
	"userId":"39"
}
```



## Running the Scripts

### Initial Setup

These files are run within the AWS ecosystem, so familiarity with core services are assumed.

2. Create a Hadoop cluster using EMR and ensure Spark is installed on it (instructions can be found [here](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-launch.html)). Make note of the EC2 key pair to SSH onto the EC2 instance. 
2. Create an S3 bucket to write the Parquet files to. 
3. SSH onto the master node of the cluster and clone this repository or download these files onto it.
4. Fill in `dl.cfg` with your AWS credentials (ensure the profile has read/write permissions to S3).

```
[AWS]
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
```

5. In `etl.py`, locate the variable `output_data`. Specify your S3 bucket here (line 176).

```python
def main() -> None:
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    
    # enter your S3 bucket here 
    # e.g. "s3a://my-bucket-name/"
    output_data = ""

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)
```



Now, that the initial setup is done, you can run the code in the terminal window:

1. `python etl.py` 

#### ***Note***

It is recommended to remove wildcards in the `song_data` file path if you want to reduce load times (it can take well over an hour depending on EMR cluster size). This can be found on line 48 of `etl.py`.

```python
# get filepath to song data file
song_data = os.path.join(input_data, 'song_data/*/*/*/*') # e.g. "song_data/A/A/*/*"
```

