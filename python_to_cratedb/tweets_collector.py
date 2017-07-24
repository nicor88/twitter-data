"""
Class to collect tweets with specific keywords and save to crateDb

Examples
--------
# Example
>>> crate_host = 'localhost:4200'
>>> keywords = ['referendum']
>>> collector = TweetsCollector.collect_and_persist(host=crate_host, keywords=keywords)

"""

import logging
import os
import yaml
import json
import _datetime as dt
from pytz import timezone
import dateutil.parser as dateparser
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import python_to_cratedb.settings as settings

from crate import client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TweetsCollector:
    def __init__(self, *, host, keywords):
        self.crate_host = host
        self.keywords = keywords
        configs = self.configs_loader()
        twitter_auth = configs['twitter']

        # set twitter auth
        self.auth = OAuthHandler(twitter_auth['consumer_key'], twitter_auth['consumer_secret'])
        self.auth.set_access_token(twitter_auth['access_token'], twitter_auth['access_secret'])

        # connect to crate
        try:
            crate_connection = client.connect(self.crate_host)

            self.crate_cursor = crate_connection.cursor()
            logger.info('connected to ' + self.crate_host)
        except:
            logger.info('Error connecting to ' + self.crate_host)

    @classmethod
    def collect_and_persist(cls, *, host, keywords):
        c = cls(host=host, keywords=keywords)
        return c.create_tweets_table().get_tweets()

    def get_tweets(self):
        twitter_stream = Stream(self.auth, PersistTweets(crate_cursor=self.crate_cursor))
        twitter_stream.filter(track=self.keywords)
        return self

    def create_tweets_table(self):
        table_statement = self.tweets_table()
        try:
            self.crate_cursor.execute(table_statement)
        except Exception as e:
            logger.info(e)
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

    @staticmethod
    def tweets_table():
        return """create table tweets_test_python (
                    id string,
                    created_at timestamp,
                    retweeted boolean,
                    source string,
                    language string,
                    sentiment_tweet_score float,
                    text string index using fulltext with (analyzer = "standard"),
                    user object(dynamic) as (
                        id string,
                        created_at timestamp,
                        description string,
                        location string,
                        followers_count integer,
                        statuses_count integer,
                        friends_count integer,
                        verified boolean
                    )
                )"""


class PersistTweets(StreamListener):
    def __init__(self, *, crate_cursor):
        super(StreamListener, self).__init__()
        self.crate_cursor = crate_cursor

    def on_data(self, data):
        tweet = json.loads(data)
        insert_statement, data_to_insert = self.insert_tweet(tweet=tweet)
        try:
            self.crate_cursor.execute(insert_statement, data_to_insert)
            logger.info('tweet inserted')
        except Exception as e:
            logger.info(e)

    def on_error(self, status):
        print(status)
        return True

    @staticmethod
    def insert_tweet(*, tweet):
        logger.info(tweet['user'])
        user = tweet['user']
        clean_user = dict()
        clean_user['id'] = user['id_str']
        clean_user['name'] = user['name']
        clean_user['description'] = user['description']
        clean_user['followers_count'] = user['followers_count']
        clean_user['friends_count'] = user['friends_count']
        clean_user['location'] = user['location']
        clean_user['statuses_count'] = user['statuses_count']
        clean_user['created_at'] = dateparser.parse(user['created_at'])
        clean_user['created_at'] = clean_user['created_at'].replace(tzinfo=None)
        # todo manage timezone

        clean_user['verified'] = user['verified']
        insert_statement = """INSERT INTO tweets_test_python
            (id,
            created_at,
            retweeted,
            source,
            text,
            language,
            user
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)"""
        tweet = (tweet['id_str'],
                 tweet['timestamp_ms'],
                 tweet['retweeted'],
                 tweet['source'],
                 tweet['text'],
                 tweet['lang'],
                 clean_user
                 )
        return insert_statement, tweet

# to stop the listener try
# PersistTweets.running = False

if __name__ == "__main__":
    crate_host = 'localhost:4200'
    keywords = ['aws']
    collector = TweetsCollector.collect_and_persist(host=crate_host, keywords=keywords)
