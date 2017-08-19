"""
Start a daemon to send tweets to Kinesis

Example: start the daemon
--------
python stream_to_stream/sender_daemon.py start &>/dev/null &

Example: stop the daemon
--------
python stream_to_stream/sender_daemon.py stop &>/dev/null &
"""

import datetime as dt

from stream_to_stream.logger import configure_logger
from stream_to_stream.daemon import UnixDaemon
from stream_to_stream.send_tweets_to_kinesis import TweetsCollector


class Sender(UnixDaemon):
    def __init__(self):
        super().__init__()
        self.daemon_description = 'Daemon to start morning tasks scheduler'
        self.app_name = 'daily_tasks_scheduler'
        self.logger = configure_logger()

    def add_parse_arguments(self, parser):
        super().add_parse_arguments(parser)

    def run(self):
        self.logger.info(f'Started at {dt.datetime.now().isoformat()}')
        # TODO consider to retrive from env varibales as setup of the application
        TweetsCollector.run(keywords=['Sardegna'])

if __name__ == '__main__':
    Sender().action()
