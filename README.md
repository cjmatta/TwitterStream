## Usage
Copy the `twitter_stream.config.example` to `twitter_stream.config` and edit it to include your Twitter API keys.

```
Usage: twitter_stream.py [options]

Options:
  -h, --help            show this help message and exit
  -q "QUERY", --query="QUERY"
      Quoted, comma-sepparated list of queries to listen for.
  -d DIR, --dir=DIR     Directory to save the tweets to.
```
## Requirements
* [tweepy](https://github.com/tweepy/tweepy)
* [elasticsearch-py](https://github.com/elasticsearch/elasticsearch-py)
