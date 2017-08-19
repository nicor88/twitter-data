""" Send tweets from a twitter stream to Kinesis

>>> TweetsCollector.run(keywords=['aws'])

"""
import datetime as dt
import json
import logging
import os

import boto3
from botocore.client import Config
from dateutil.parser import parse
import ruamel_yaml as yaml
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

import settings

# setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"
os.environ["AWS_PROFILE"] = "nicor88-aws-dev"


class TweetsCollector:
    def __init__(self, *, keywords):
        self.keywords = keywords
        configs = self.configs_loader()
        twitter_auth = configs['twitter']

        # set twitter auth
        self.auth = OAuthHandler(twitter_auth['consumer_key'], twitter_auth['consumer_secret'])
        self.auth.set_access_token(twitter_auth['access_token'], twitter_auth['access_secret'])

    @classmethod
    def run(cls, *, keywords):
        c = cls(keywords=keywords)
        return c.get_tweets()

    def get_tweets(self):
        twitter_stream = Stream(self.auth, SendTweetsToKinesis(keywords=self.keywords))

        twitter_stream.filter(track=self.keywords)
        return self

    @staticmethod
    def configs_loader():
        settings_path = os.path.realpath(os.path.dirname(settings.__file__))
        configs_file = os.path.join(settings_path, 'configs.yml')
        try:
            with open(configs_file, 'r') as f:
                configs = yaml.load(f)
                return configs
        except IOError:
            logger.error('No configs.yml found')


class SendTweetsToKinesis(StreamListener):
    def __init__(self, keywords):
        super(StreamListener, self).__init__()
        self.kinesis = boto3.client('kinesis', config=Config(connect_timeout=1000))
        self.keywords = keywords

    def on_data(self, data):
        tweet = json.loads(data)
        logger.info(tweet)
        tweet_to_send = self.create_tweet_for_kinesis(name='twitter', tweet=tweet,
                                                      keywords=self.keywords)
        logger.info(tweet_to_send)
        res = self.put_tweet_to_kinesis(stream_name='DevStreamES', tweet=tweet_to_send)
        logger.info(res)

    def on_error(self, status):
        logger.error(status)
        return True

    @staticmethod
    def create_tweet_for_kinesis(*, tweet, name='event_name', producer='stream_to_stream', keywords):
        def __clean_tweet(tweet_to_clean):
            attrs = ['created_at', 'lang', 'geo', 'coordinates', 'place', 'retweeted', 'source',
                     'text', 'timestamp_ms']
            user_attrs = ['name', 'screen_name', 'location', 'url', 'description',
                          'followers_count', 'created_at', 'utc_offset', 'time_zone', 'lang']
            clean = {a: tweet_to_clean[a] for a in attrs}
            # clean['created_at'] = parse(tweet_to_clean['created_at']).replace(tzinfo=None)
            clean['created_at'] = dt.datetime.fromtimestamp(int(clean['timestamp_ms'])/1000).isoformat()
            clean['user'] = {a: tweet_to_clean['user'][a] for a in user_attrs}
            clean['hashtags'] = [el['text'] for el in tweet_to_clean['entities']['hashtags']]
            return clean

        record = __clean_tweet(tweet)
        record['name'] = name
        record['meta'] = {'created_at': dt.datetime.now().isoformat(), 'producer': producer,
                          'keywords': ','.join(keywords)}

        if 'created_at' not in record.keys():
            record['created_at'] = record['meta']['created_at']

        return record

    def put_tweet_to_kinesis(self, *, stream_name, tweet, partition_key='created_at'):
        res = self.kinesis.put_record(StreamName=stream_name,
                                      Data=json.dumps(tweet),
                                      PartitionKey=partition_key)
        return res

if __name__ == '__main__':
    TweetsCollector.run(keywords=['python'])
