import logging
from tweepy.streaming import StreamListener
from tweepy import API
from .config import Config
import json

logging.basicConfig()
logger = logging.getLogger("TwitterStream.tweetlistener")
logger.setLevel(logging.INFO)


class TweetListener(StreamListener):
    """ A listener that saves tweets to a specified directory
    """
    def __init__(self, tweet_saver=None, indexer=None):

        if tweet_saver is None:
            raise Exception("Need a tweet saver!")

        self._indexer = indexer
        self._tweet_saver = tweet_saver
        self._config = Config()

        if self._config.config.has_section('Proxy'):
            api = API(proxy=self._config.config.get('Proxy', 'https_proxy'))
        else:
            api = API()

        super(TweetListener, self).__init__(api=api)

    def on_data(self, raw_data):
        """Run when data comes through. Write raw_data to file.
        """
        super(TweetListener, self).on_data(raw_data)
        self._tweet_saver.saveTweet(raw_data)

    def on_status(self, status):
        """Run When status comes through, if self._elasticsearch is set, index,
            otherwise do nothing.
        """
        if self._indexer is not None:
            try:
                body = {
                    "text": status.text,
                    "user": status.user.screen_name,
                    "retweeted": status.retweeted,
                    "retweet_count": status.retweet_count,
                    "created_at": status.created_at
                }

                if status.coordinates is not None:
                    body["coordinates"] = status.coordinates["coordinates"]

                self._indexer.index(id=status.id, timestamp=status.created_at,
                                    body=body)
            except Exception as e:
                logger.exception(e)

        else:
            pass

    def on_error(self, status):
        error_messages = {
            400: "Return Code 400: Bad Request",
            401: "Return Code 401: Unauthorized",
            403: "Return Code 403: Forbidden",
            420: "Return Code 420: Too many requests in too short a time."
        }
        logger.warn(error_messages.get(status, "Unknown Erorr: %s" % status))
        return False
