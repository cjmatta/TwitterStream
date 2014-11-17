# TwitterStream
This is a simple python script that listens to Twitter and saves tweets to the local filesystem. It also will index tweets in ElasticSearch if you include the `-i` flag.

## Usage
Copy the `twitter_stream.config.example` to `twitter_stream.config` and edit it to include your Twitter API keys.

```
Usage: twitter_stream.py [options]

Options:
  -h, --help            show this help message and exit
  -q "Phillies, Red Sox", --query="Phillies, Red Sox"
                        Quoted, comma-sepparated list of queries.
  -d DIR, --dir=DIR     Directory to save the tweets to.
  -I                    Save tweets to an elasticsearch index
  -i INDEX, --index=INDEX
                        Index to save tweets to for elasticsearch.
  -t TYPE, --type=TYPE  Document type.
```
## Requirements
The following packages need to be installed before using:
* [tweepy](https://github.com/tweepy/tweepy)
* [elasticsearch-py](https://github.com/elasticsearch/elasticsearch-py)

Also, elasticsearch should be running somewhere (right now it only looks on the localhost)
