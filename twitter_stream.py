#!/usr/bin/env python
from tweepy import OAuthHandler
from tweepy import Stream
import logging
from optparse import OptionParser
import sys
from TwitterStream.tweetsaver import TweetSaver
from TwitterStream.tweetlistener import TweetListener
from TwitterStream import config
from TwitterStream.tweetsaver import DirNotFoundException

logging.basicConfig()
logger = logging.getLogger("twitter_stream.py")
logger.setLevel(logging.INFO)


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
        tweet_saver = TweetSaver(save_dir=options.directory,
                                 pretty=options.ppretty)
        c = config.Config()

        if options.index_tweets:
            try:
                from TwitterStream.tweetindexer import TweetIndexer
            except ImportError:
                logger.warn("In order to index tweets to elasticsearch" +
                " you need to install the elasticsearch-py package: \n" +
                "   run:  `pip install elasticsearch` to install.")
                sys.exit(1)

            indexer = TweetIndexer(c.getElasticsearchHosts(),
                                   index=options.index,
                                   doc_type=options.type)
            listener = TweetListener(tweet_saver=tweet_saver,
                                     indexer=indexer)
        else:
            listener = TweetListener(tweet_saver=tweet_saver)

        auth = OAuthHandler(c.CONSUMER_KEY, c.CONSUMER_SECRET)
        auth.set_access_token(c.ACCESS_TOKEN, c.ACCESS_TOKEN_SECRET)

        if not options.query:
            print "Query required."
            sys.exit(1)

        query = [x.strip() for x in options.query.split(',')]
        print("Listening for tweets containing: %s" %
              ', '.join(query))
        stream = Stream(auth, listener)
        stream.filter(track=query)

    except DirNotFoundException, e:
        logger.warn(e)
        sys.exit(1)

    except KeyboardInterrupt:
        logger.warn("Keyboard interrupt... exiting.")
        sys.exit(1)

    except Exception:
        raise
