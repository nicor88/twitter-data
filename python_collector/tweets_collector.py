"""
Class to collect tweets with specific keywords and save to crateDb

Examples
--------
# Example
>>> host = 'localhost:4200'
>>> keywords = ['referendum']
>>> collector = TweetsCollector.collect_and_save(host=host, keywords=keywords)

"""

import logging
import os
import yaml
import _datetime as dt
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import python_collector.settings

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
        except:
            logger.info('Error connecting to ' + self.crate_host)

    @classmethod
    def collect_and_save(cls, *, host, keywords):
        c = cls(host=host, keywords=keywords)
        return c.get_tweets()

    def get_tweets(self):
        twitter_stream = Stream(self.auth, PersistTweets(crate_cursor=self.crate_cursor))
        twitter_stream.filter(track=self.keywords)
        return self

    def create_tweets_table(self):
        table_statement = self.tweets_table()
        try:
            self.crate_cursor.execute(table_statement)
        except:
            logger.error('Impossible to create the table')

    @staticmethod
    def configs_loader():
        settings_path = os.path.realpath(os.path.dirname(python_collector.settings.__file__))
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
        # todo here persist data on crate
        print(data)

    def on_error(self, status):
        print(status)
        return True

# to stop the listener try
# PersistTweets.running = False


def test_insert():
    insert_statement = """INSERT INTO tweets_test_python
        (id, created_at, retweeted, source) VALUES (?, ?, ?, ?)"""
    data = ('id test', dt.datetime.now(), 'false', 'source test')
    return insert_statement, data

if __name__ == "__main__":
    crate_host = 'localhost:4200'
    crate_connection = client.connect(crate_host)
    crate_cursor = crate_connection.cursor()
    statement, data = test_insert()
    crate_cursor.execute(statement, data)