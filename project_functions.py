import snscrape
import pandas as pd
import json
import tweepy
import os
import preprocessor as p
import re
from gensim.parsing.preprocessing import remove_stopwords

# Input API and Access tokens

consumer_key = 'SbKSnTmMAarrMpYxjYCXUFJnf'
consumer_secret = 'LKIUvARO8EGTJ7LsJSSs1BG1pFZvNXOHYw1ExeqXHAof0u5GlG'
access_token = '1324933189824110593-hhqL3P8GrzgRCWoIbbdlPSMbHrhTi6'
access_token_secret = '2l0FA1jC8GjjEawIk04JrL1qgF5ItzNb9xvCJfHpZHMcl'

# Set Authorization and API

auth = tweepy.OAuthHandler(consumer_key, consumer_secret) 
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth, wait_on_rate_limit=True)

# Test for functional authorization

try:
    redirect_url = auth.get_authorization_url()
except tweepy.TweepError:
    print('Error! Failed to get request token.')



def json_to_csv(file_name):
    """
    Takes in json file and writes it into a csv file of the same name.
    """
    file_name = str(file_name)
    table = []
    with open((f'data/{file_name}.json'), 'r', encoding="utf8") as f:
        for line in f:
            table.append(line)
    pd.DataFrame(table).to_csv('data/{file_name}.csv'.format(file_name=file_name))
    
    return table



# define function to fetch tweets
def fetch_tw(ids,filename):
    # create list of statuses using API, and empty pandas df to store tweets 
    list_of_tw_status = api.statuses_lookup(ids, tweet_mode= "extended")
    empty_data = pd.DataFrame()
    
    # for loop to access statuses, username, and timestamp - then update empty_data table
    for status in list_of_tw_status:
            tweet_elem = {"tweet_id": status.id,
                     "screen_name": status.user.screen_name,
                     # replace line breaks, commas (for csv)
                     "tweet":status.full_text.replace('\n','').replace(',',''),
                     "date":status.created_at}
            empty_data = empty_data.append(tweet_elem, ignore_index = True)
            
    # write dataframe to csv
    empty_data.to_csv("data/{filename}_tweets.csv".format(filename=filename), mode="a")


#define function to create a tweet table (calling above functions)
def tweet_table(username):
    os.system(f'snscrape twitter-user {username} > data/{username}.json')
    # groundwork for function - add in more usernames
 #   !snscrape twitter-user {username} > {username}.json
    df = json_to_csv(username)

    # call up csv file
    df = pd.read_csv('data/{username}.csv'.format(username=username))

    # Add id column
    af = lambda x: x["0"].split("/")[-1][:-1]
    df['id'] = df.apply(af, axis=1)
    df.head()

    # Drop unnecessary column and rename existing

    df = df.drop(columns=['Unnamed: 0'])
    df.columns = ['tweet_url', 'id']

    # create list of  tweet id's

    ids = df['id'].tolist()
    total_count = len(ids)
    chunks = (total_count - 1) // 50 + 1

    # loop through 50-Tweet cycle to bypass API error

    for i in range(chunks-1):
            batch = ids[i*50:(i+1)*50]
            result = fetch_tw(batch,username)
            
def preprocess_tweet(row):
    text = row['tweet']
    #Removes all cashtags
    text = re.sub(r'[$][a-zA-Z]+', '', text)
    #clean to normal text removes hashtags and emojis
    text = p.clean(text)
    #Removes all symbols
    text = re.sub(r'[^\w]', ' ', text) 
    # lowercases all words
    text = text.lower()
    # Removes numbers
    text = re.sub(r'\d+', '', text) 
    # Removing RT (Retweet?)
    text = re.sub('RT[\s]+', '', text)
    # Removing hyperlink
    text = re.sub('https?:\/\/\S+', '', text) 
    #removes stopwords
    text = remove_stopwords(text) 
    #Removes words between 1 and 2 characters short
    text = re.sub(r'\W*\b\w{1,2}\b', '', text) 
    
    return text

