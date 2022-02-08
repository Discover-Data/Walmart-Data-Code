import mysql.connector as c
import yaml, tweepy

with open('config.yaml', 'r') as f:
    storage = yaml.safe_load(f)
# Create API Connection
client = tweepy.Client(bearer_token=storage['twitter'].get('bearer_token'))

# Get Tweets, manually limiting number for testing
resp = client.search_recent_tweets("Justin Bieber",
                                   max_results=100,
                                   tweet_fields=['created_at', 'lang', 'author_id', 'public_metrics'])
# For ease of use, filter out non english tweets
data = [tweet for tweet in resp.data if tweet['lang'] == 'en']
# Remove detected keywords related to 'music': in this case the words music, spotify, song, and listen
filter_list = ['music', 'spotify', 'song', 'songs', 'listen']
final_tweets = []
# ID Consumption is duplicate protection
consumed_ids = []
for tweet in data:
    if all(filt not in tweet['text'] for filt in filter_list) and tweet['id'] not in consumed_ids:
        # Process
        tweet_dictionary = {
            'id': tweet['id'],
            'author_id': tweet['author_id'],
            'created_at': tweet['created_at'],
            'text': tweet['text'],
            'retweet_count': tweet['public_metrics']['retweet_count'],
            'reply_count': tweet['public_metrics']['reply_count'],
            'like_count': tweet['public_metrics']['like_count'],
            'quote_count': tweet['public_metrics']['quote_count']
        }
        final_tweets.append(tweet_dictionary)
        consumed_ids.append(tweet['id'])
print(f"All Consumed Tweets: {len(data)}\nAll Unique Tweets:{len(consumed_ids)}\n")
# Create DB Connection
db = c.connect(
    host='localhost',
    user='root',
    password=storage['mysql'].get('password'),
    database=storage['mysql'].get('database')
)
cursor = db.cursor()
cursor.executemany("""
INSERT INTO tweets (id, author_id, created_at, text, retweet_count, reply_count, like_count, quote_count)
VALUES (%(id)s, %(author_id)s, %(created_at)s, %(text)s, %(retweet_count)s,
        %(reply_count)s, %(like_count)s, %(quote_count)s)
""", final_tweets)
db.commit()