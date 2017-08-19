""" Send tweets from a twitter stream to Kinesis

>>> TweetsCollector.run(keywords=['aws'])

"""
import datetime as dt
import json
import logging
import os

import boto3
from botocore.client import Config
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
        twitter_stream = Stream(self.auth, SendTweetsToKinesis())
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
            logger.debug('No configs.yml found')


class SendTweetsToKinesis(StreamListener):
    def __init__(self, ):
        super(StreamListener, self).__init__()
        self.kinesis = boto3.client('kinesis', config=Config(connect_timeout=1000))

    def on_data(self, data):
        tweet = json.loads(data)
        tweet_to_send = self.create_tweet_for_kinesis(tweet=tweet, name='twitter')
        logger.info(tweet_to_send)
        res = self.put_tweet_to_kinesis(stream_name='DevStreamES', tweet=tweet_to_send)

    def on_error(self, status):
        logger.error(status)
        return True

    @staticmethod
    def create_tweet_for_kinesis(*, tweet, name='event_name', producer='send_tweets_to_kinesis'):
        # assert tweet as dict
        record = tweet
        record['name'] = name
        record['meta'] = {'created_at': dt.datetime.now().isoformat(), 'producer': producer}
        if 'created_at' not in record.keys():
            record['created_at'] = record['meta']['created_at']

        return record

    def put_tweet_to_kinesis(self, *, stream_name, tweet):
        res = self.kinesis.put_record(StreamName=stream_name,
                                 Data=json.dumps(
                                     self.create_tweet_for_kinesis(tweet=tweet,
                                                              name='tweets')),
                                 PartitionKey='created_at')
        logger.info(res)
        return res
