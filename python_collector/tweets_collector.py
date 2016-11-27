"""
Class to collect tweets with specific keywords and save to crateDb

Examples
--------
# Example
>>> host = 'localhost:4200'
>>> collector = TweetsCollector(host=host)

"""

import logging
import os
import yaml
import python_collector.settings
from crate import client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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

    @staticmethod
    def config_loader():
        settings_path = os.path.realpath(os.path.dirname(python_collector.settings.__file__))
        configs_file = os.path.join(settings_path, 'configs.yml')
        try:
            with open(configs_file, 'r') as f:
                configs = yaml.load(f)
                return configs
        except IOError:
            logger.debug('No configs.yml found')
