#!/bin/env python
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import os
import json
import logging
from pprint import pprint
from optparse import OptionParser
import sys
from HTMLParser import HTMLParser
import errno
from datetime import datetime
from elasticsearch import Elasticsearch
import ConfigParser

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

es = Elasticsearch()
config = ConfigParser.ConfigParser()
config.read('twitter_stream.config')

CONSUMER_KEY = config.get('Twitter Keys', 'CONSUMER_KEY')
CONSUMER_SECRET = config.get('Twitter Keys', 'CONSUMER_SECRET')
ACCESS_TOKEN = config.get('Twitter Keys', 'ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = config.get('Twitter Keys', 'ACCESS_TOKEN_SECRET')

twitter_time_format = "%a %b %d %H:%M:%S +0000 %Y"

def make_sure_path_exists(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

class SaveAndIndexTweetsListener(StreamListener):
    """ A listener that saves tweets to a specified directory
    """
    def __init__(self):
        super(SaveAndIndexTweetsListener, self).__init__()
        self._saveDir = '.'
        self._tweetCounter = 0

    @property
    def saveDir(self):
        return self._saveDir

    @saveDir.setter
    def saveDir(self, value):
        if not os.path.exists(value):
            raise Exception("Directory %s not found!" % value)
        self._saveDir = value

    def on_data(self, raw_data):
        """Run when data comes through. Write raw_data to file.
        """
        super(SaveAndIndexTweetsListener, self).on_data(raw_data)
        data = json.loads(HTMLParser().unescape(raw_data))
        created_at = datetime.strptime(data['created_at'], twitter_time_format)
        save_dir = os.path.join(os.path.abspath(self._saveDir),
                                str(created_at.year),
                                str(created_at.month),
                                str(created_at.day),
                                str(created_at.hour))
        make_sure_path_exists(save_dir)
        tweet_file = os.path.join(save_dir, 'tweets.json')

        with open(tweet_file, 'a') as f:
            f.write(raw_data)
            self._tweetCounter += 1
            logger.info("Saved %d tweets." % self._tweetCounter)
            f.close()

    def on_status(self, status):
        """Run when a status comes through. The status will be indexed into
        elasticsearch by Username, created_at, and status.
        """
        body = {
            "text": status.text,
            "user": status.user.screen_name,
            "retweeted": status.retweeted,
            "retweet_count": status.retweet_count,
            "timestamp": status.created_at
        }

        if status.coordinates is not None:
            body["coordinates"] = status.coordinates["coordinates"]
        try:
            es.index(index="tweets",
                     doc_type="tweet",
                     id=status.id,
                     body=body)
        except Exception as e:
            logger.exception(e)

    def on_error(self, status):
        logger.warn(status)


def parseOptions():
    parser = OptionParser()
    parser.add_option("-q", "--query", dest="query",
                      help="Quoted, comma-sepparated list of queries to listen for.",
                      metavar='"QUERY"')
    parser.add_option("-d", "--dir", dest="directory",
                      default=".", metavar="DIR",
                      help="Directory to save the tweets to.")

    return parser.parse_args()

if __name__ == '__main__':
    (options, args) = parseOptions()
    l = SaveAndIndexTweetsListener()
    l.saveDir = options.directory
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    if not options.query:
        print "Query required."
        sys.exit(1)

    query = [x.strip() for x in options.query.split(',')]
    logger.info("Listening for tweets containing: %s" % ', '.join(query))
    stream = Stream(auth, l)
    stream.filter(track=query)
