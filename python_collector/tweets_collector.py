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
            self.crate = client.connect(self.crate_host)
        except:
            logger.info('Error connecting to ' + self.crate_host)

    @classmethod
    def collect_and_save(cls, *, host, keywords):
        c = cls(host=host, keywords=keywords)
        return c.get_tweets()

    def get_tweets(self):
        twitter_stream = Stream(self.auth, PersistTweets(crate=self.crate))
        twitter_stream.filter(track=self.keywords)
        return self

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


class PersistTweets(StreamListener):
    def __init__(self, *, crate):
        super(StreamListener, self).__init__()
        self.crate = crate

    def on_data(self, data):
        # todo here persist data on crate
        print(data)

    def on_error(self, status):
        print(status)
        return True
