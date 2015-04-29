#!/usr/bin/env python
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import API
import os
import json
import logging
from optparse import OptionParser
import sys
from HTMLParser import HTMLParser
import errno
from datetime import datetime
import ConfigParser

this_dir = os.path.abspath(os.path.dirname(__file__))

logging.basicConfig()
logger = logging.getLogger(os.path.join(this_dir, 'twitter_stream'))
logger.setLevel(logging.INFO)

CONFIGFILE = 'twitter_stream.config'
config = ConfigParser.ConfigParser()
config.read(CONFIGFILE)


def getElasticsearchHosts():
    """Look in config for Elasticsearch section, return hosts in a format that
        elasticsearch-py can understand.
    """
    try:
        hosts = config.get('Elasticsearch', 'hosts').split(',')
        hosts = [x.strip() for x in hosts]
        return [{"host": x.split(':')[0], "port": x.split(':')[1]}
                for x in hosts]

    except ConfigParser.NoSectionError:
        logger.warn(
            "No Elasticsearch section found in config, using localhost:9200.")
        return [{"host": "localhost", "port": 9200}]

try:
    CONSUMER_KEY = config.get('Twitter Keys', 'CONSUMER_KEY')
    CONSUMER_SECRET = config.get('Twitter Keys', 'CONSUMER_SECRET')
    ACCESS_TOKEN = config.get('Twitter Keys', 'ACCESS_TOKEN')
    ACCESS_TOKEN_SECRET = config.get('Twitter Keys', 'ACCESS_TOKEN_SECRET')
except ConfigParser.NoSectionError as e:
        logger.warn(e)
        sys.exit(1)


class TweetIndexer(Elasticsearch):
    """Subclass of Elasticsearch that has the index and doc type set"""
    def __init__(self, hosts, **kwargs):
        super(TweetIndexer, self).__init__(hosts)
        self._kwargs = kwargs

    def index(self, **kwargs):
        these_args = self._kwargs.copy()
        these_args.update(kwargs)
        super(TweetIndexer, self).index(**these_args)

class DirNotFoundException(Exception): pass

class TweetSaver(object):
    """A utility to append tweets to a json file
        tweet_saver = TweetSaver(save_dir="/path/to/save/tweets")
        Will create the following file tree:
        <save_dir>/YYYY/MM/DD/HH/tweets.json
        based on the created_at field in the tweet.
    """
    def __init__(self, save_dir="."):
        self._saveDir = None
        self.saveDir = save_dir
        self._tweetCounter = 0
        self._twitter_time_format = "%a %b %d %H:%M:%S +0000 %Y"

    def _make_sure_path_exists(self, path):
        try:
            os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

    @property
    def saveDir(self):
        return self._saveDir


    @saveDir.setter
    def saveDir(self, value):
        if not os.path.exists(value):
            raise DirNotFoundException("Directory %s not found!" % value)
        self._saveDir = value

    def saveTweet(self, tweet):
        """Appends tweet text (raw) to a tweets.json file in
        <self.saveDir>/YYYY/MM/DD/HH/tweets.json based on created_at field.
        """
        try:
            data = json.loads(HTMLParser().unescape(tweet))
            created_at = datetime.strptime(data['created_at'],
                                           self._twitter_time_format)
            save_dir = os.path.join(os.path.abspath(self._saveDir),
                                    str(created_at.year),
                                    str(created_at.month).zfill(2),
                                    str(created_at.day).zfill(2),
                                    str(created_at.hour).zfill(2))
            self._make_sure_path_exists(save_dir)
            tweet_file = os.path.join(save_dir, 'tweets.json')

            with open(tweet_file, 'a') as f:
                if (options.ppretty):
                    mydata = json.loads(tweet)
                    f.write(json.dumps(mydata, indent=4))
                else:
                    f.write(tweet)
                self._tweetCounter += 1
                # logger.info("Saved %d tweets." % self._tweetCounter)
                sys.stdout.write("\rSaved %d tweets." % self._tweetCounter)
                sys.stdout.flush()
                f.close()


        except Exception, e:
            logger.exception(e)
            return


class SaveTweetsListener(StreamListener):
    """ A listener that saves tweets to a specified directory
    """
    def __init__(self, tweet_saver=None, api=None):

        super(SaveTweetsListener, self).__init__(api=api)
        self._tweet_saver = tweet_saver

        if tweet_saver is None:
            raise Exception("Need a tweet saver!")

    def on_data(self, raw_data):
        """Run when data comes through. Write raw_data to file.
        """
        super(SaveTweetsListener, self).on_data(raw_data)
        self._tweet_saver.saveTweet(raw_data)

    def on_error(self, status):
        logger.warn(status)


class SaveAndIndexTweetsListener(StreamListener):
    """ A listener that saves tweets to a specified directory, and indexes them
    in an elasticsearch cluster.
    """
    def __init__(self, tweet_saver=None,
                 elasticsearch=None,
                 api=None):

        super(SaveAndIndexTweetsListener, self).__init__(api=api)
        self._tweet_saver = tweet_saver
        self._elasticsearch = elasticsearch

        if tweet_saver is None:
            raise Exception("Need a tweet saver!")

        if elasticsearch is None:
            raise Exception("Need elasticsearch!")

    def on_data(self, raw_data):
        """Run when data comes through. Write raw_data to file.
        """
        super(SaveAndIndexTweetsListener, self).on_data(raw_data)
        self._tweet_saver.saveTweet(raw_data)

    def on_status(self, status):
        """Run when a status comes through. The status will be indexed into
        elasticsearch by Username, created_at, and status.
        """
        body = {
            "text": status.text,
            "user": status.user.screen_name,
            "retweeted": status.retweeted,
            "retweet_count": status.retweet_count,
            "created_at": status.created_at
        }

        if status.coordinates is not None:
            body["coordinates"] = status.coordinates["coordinates"]
        try:
            self._elasticsearch.index(id=status.id,
                                      timestamp=status.created_at,
                                      body=body)
        except Exception as e:
            logger.exception(e)

    def on_error(self, status):
        logger.warn(status)


def parseOptions():
    parser = OptionParser()
    parser.add_option("-q", "--query", dest="query",
                      help="Quoted, comma-sepparated list of queries.",
                      metavar='"Phillies, Red Sox"')
    parser.add_option("-d", "--dir", dest="directory",
                      default=".", metavar="DIR",
                      help="Directory to save the tweets to.")
    parser.add_option("-I", dest="index_tweets", action="store_true",
                      help="Save tweets to an elasticsearch index")
    parser.add_option("-i", "--index", dest="index", default="default",
                      help="Index to save tweets to for elasticsearch.")
    parser.add_option("-t", "--type", dest="type", default="tweet",
                      help="Document type.")
    parser.add_option("-p", "--pretty", action="store_true",
                      dest="ppretty", default=False,
                      help="Store results in human-readable format.")

    return parser.parse_args()

if __name__ == '__main__':
    try:
        (options, args) = parseOptions()
        tweet_saver = TweetSaver(save_dir=options.directory)

        if config.has_section('Proxy'):
            api = API(proxy=config.get('Proxy', 'https_proxy'))
        else:
            api = API()

        if options.index_tweets:
            from elasticsearch import Elasticsearch
            elasticsearch = TweetIndexer(getElasticsearchHosts(),
                                         index=options.index,
                                         doc_type=options.type)

            l = SaveAndIndexTweetsListener(tweet_saver=tweet_saver,
                                           elasticsearch=elasticsearch,
                                           api=api)
        else:
            l = SaveTweetsListener(tweet_saver=tweet_saver, api=api)

        auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

        if not options.query:
            print "Query required."
            sys.exit(1)

        query = [x.strip() for x in options.query.split(',')]
        print("Listening for tweets containing: %s" % ', '.join(query))
        stream = Stream(auth, l)
        stream.filter(track=query)

    except DirNotFoundException, e:
        logger.warn(e)
        sys.exit(1)

    except KeyboardInterrupt:
        logger.warn("Keyboard interrupt... exiting.")
        sys.exit(1)

    except Exception:
        raise
