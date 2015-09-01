import ConfigParser
import logging
import os
import sys

logging.basicConfig()
logger = logging.getLogger("TwitterStream.config")
logger.setLevel(logging.INFO)


class Config(object):
    def __init__(self, configfile=None):
        if configfile is None:
            self.configfile = os.path.join(os.path.dirname(__file__),
                                           '..',
                                           'twitter_stream.config')
        self.config = ConfigParser.ConfigParser()
        self.config.read(self.configfile)

        try:
            self.CONSUMER_KEY = self.config.get('Twitter Keys',
                                                'CONSUMER_KEY')
            self.CONSUMER_SECRET = self.config.get('Twitter Keys',
                                                   'CONSUMER_SECRET')
            self.ACCESS_TOKEN = self.config.get('Twitter Keys',
                                                'ACCESS_TOKEN')
            self.ACCESS_TOKEN_SECRET = self.config.get('Twitter Keys',
                                                       'ACCESS_TOKEN_SECRET')
        except ConfigParser.NoSectionError as e:
                logger.warn(e)
                sys.exit(1)

    def getElasticsearchHosts(self):
        """Look in config for Elasticsearch section, return hosts in a format
            that elasticsearch-py can understand.
        """
        try:
            hosts = self.config.get('Elasticsearch', 'hosts').split(',')
            hosts = [x.strip() for x in hosts]
            return [{"host": x.split(':')[0], "port": x.split(':')[1]}
                    for x in hosts]

        except ConfigParser.NoSectionError:
            logger.warn(
                "No Elasticsearch section found in config, ",
                "using localhost:9200.")
            return [{"host": "localhost", "port": 9200}]
