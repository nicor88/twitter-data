""" Send tweets from a twitter stream to Kinesis

Examples
--------
>>> TweetsCollector.run(keywords=['python'])

"""
import datetime as dt
import json
import logging
import os

import boto3
from botocore.client import Config
from dateutil.parser import parse
import pytz
import ruamel_yaml as yaml
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

import settings

os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"
os.environ["AWS_PROFILE"] = "nicor88-aws-dev"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('sender')


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
    def create_tweet_for_kinesis(*, tweet, name='twitter', producer='stream_to_stream', keywords):
        def get_user_created(user_created, time_zone):
            matched_tz = [a for a in set(pytz.all_timezones_set) if time_zone in a]
            parsed_time = parse(user_created)
            parsed_time = parsed_time.replace(tzinfo=None)
            if len(matched_tz) > 0:
                user_created_tz = pytz.timezone(matched_tz[0])
                user_created_time = user_created_tz.localize(parsed_time, is_dst=None)
            else:
                user_created_time = parsed_time.isoformat()

            return user_created_time

        def __clean_tweet(tweet_to_clean):
            attrs = ['created_at', 'lang', 'geo', 'coordinates', 'place', 'retweeted', 'source',
                     'text', 'timestamp_ms']
            user_attrs = ['name', 'screen_name', 'location', 'url', 'description',
                          'followers_count', 'created_at', 'utc_offset', 'time_zone', 'lang']
            clean = {a: tweet_to_clean[a] for a in attrs}
            # clean['created_at'] = parse(tweet_to_clean['created_at']).replace(tzinfo=None)
            created_at = dt.datetime.fromtimestamp(int(clean['timestamp_ms'])/1000)
            logger.debug(f'Before utc {created_at.isoformat()}')
            created_at = created_at.astimezone(pytz.utc)
            logger.debug(f'Before utc {created_at.isoformat()}')
            clean['created_at'] = created_at.isoformat()
            clean['user'] = {a: tweet_to_clean['user'][a] for a in user_attrs}
            clean['user']['created_at'] = get_user_created(clean['user']['created_at'],
                                                           clean['user']['time_zone'])
            logger.info(f'User created time {clean["user"]["created_at"]}')
            clean['hashtags'] = [el['text'] for el in tweet_to_clean['entities']['hashtags']]

            return clean

        record = __clean_tweet(tweet)
        record['name'] = name
        record['meta'] = {'created_at': dt.datetime.utcnow().isoformat(), 'producer': producer,
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
