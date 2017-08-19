# Python Tweets Collector

# Dependencies
* Docker
* Conda env: `twitter-data`

## Start App
<pre>
docker-compose up
source activate twitter-data
python tweets_collector.py
</pre>

## Example Crate Queries
Format timestamp
<pre>
SELECT date_format('%Y-%m-%d %H:%i:%S', created_at) as date_time 
FROM tweets_test_python;
</pre>

Retrieve data for a specific date-range
<pre>
SELECT  date_format('%Y-%m-%d %H:%i:%S', created_at) as date_time,
        text
FROM tweets_test_python
WHERE 
    created_at >= '2016-11-01' AND created_at <= '2016-11-30'
</pre>
