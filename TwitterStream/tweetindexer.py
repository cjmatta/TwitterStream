from elasticsearch import Elasticsearch


class TweetIndexer(Elasticsearch):
    """Subclass of Elasticsearch that has the index and doc type set"""
    def __init__(self, hosts, **kwargs):
        super(TweetIndexer, self).__init__(hosts)
        self._kwargs = kwargs

    def index(self, **kwargs):
        these_args = self._kwargs.copy()
        these_args.update(kwargs)
        super(TweetIndexer, self).index(**these_args)
