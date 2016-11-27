"""
Class to collect tweets with specific keywords and save to crateDb

"""

import logging
from crate import client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CRATE_HOST = 'localhost:4200'


class TweetsCollector:
    def __init__(self, *, host):
        self.crate_host = host

        # connect to crate
        try:
            connection = client.connect(self.crate_host)
        except:
            logger.info('Error connecting to ' + self.crate_host)

    @classmethod
    def collect_and_save(cls, *, host):
        c = cls(host=host)
        return c.get_tweets()

    def get_tweets(self):
        return self
