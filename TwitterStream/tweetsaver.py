import logging
import os
import errno
import json
from datetime import datetime
from HTMLParser import HTMLParser
import sys

logging.basicConfig()
logger = logging.getLogger("TwitterStream.tweetsaver")
logger.setLevel(logging.INFO)


class DirNotFoundException(Exception):
    pass


class TweetSaver(object):
    """A utility to append tweets to a json file
        tweet_saver = TweetSaver(save_dir="/path/to/save/tweets", pretty=True)
        Will create the following file tree:
        <save_dir>/YYYY/MM/DD/HH/tweets.json
        based on the created_at field in the tweet, pretty True/False if the
        data should be saved with white-space in-tact.
    """
    def __init__(self, save_dir=".", pretty=False):
        self._saveDir = None
        self.saveDir = save_dir
        self._tweetCounter = 0
        self._twitter_time_format = "%a %b %d %H:%M:%S +0000 %Y"
        self.pretty = pretty

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
                if self.pretty:
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
            raise
