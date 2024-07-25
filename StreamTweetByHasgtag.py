from __future__ import print_function
import tweepy
import json
import datetime

import time
import os
from pymongo import MongoClient
i=0
total=0
dbdate=datetime.date.today()
#dbname=datetime.datetime.strftime(an,'%Y%m%d')
print ("DB DATE NOW = " + str(dbdate))




MONGO_HOST= 'mongodb://localhost/'  # assuming you have mongoDB installed locally
                                             # and a database called 'twitterdb'

WORDS = ['karaköy','karakoy','#karaköy','#karakoy']

CONSUMER_KEY = ""
CONSUMER_SECRET = ""
ACCESS_TOKEN = ""
ACCESS_TOKEN_SECRET = ""
start_time=time.strftime('%X %x %Z')






class StreamListener(tweepy.StreamListener): 


    #This is a class provided by tweepy to access the Twitter Streaming API. 

    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")
 
    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print('An Error has occured: ' + repr(status_code))
        return False
 
    def on_data(self, data):
        global total
        global i
        global dbdate
        #This is the meat of the script...it connects to your mongoDB and stores the tweet
        try:
            if(i==10):
                #clear screen for windows
                #os.system('cls')
                i=0
                #clear screen for linux
                #if(dbdate!=datetime.date.today()):
					#mongodb dump json export
                    #logger("### Starting DB DUMP \n")
                    #p = os.popen('mongodump --db twitterdb_' + str(dbdate) + ' --out /mnt/hdd/dbdump',"r")
                    #while 1:
                    #    line = p.readline()
                    #   if not line: break
                    #    print (line)
                    #logger("### Deleting old database")
                    #client.drop_database('twitterdb_' + str(dbdate))
                    #dbdate=datetime.date.today()
                os.system('clear')
             
            client = MongoClient(MONGO_HOST)
            
            # Use twitterdb database. If it doesn't exist, it will be created.
			
            db = client['twitterdb_karakoy_hashtag_' + str(datetime.date.today())]
    
            # Decode the JSON from Twitter
            datajson = json.loads(data)
            #splitjson = json.loads(data)
            
            # Pull important data from the tweet to store in the database.
            #tweet_id = splitjson['id_str']  # The Tweet ID from Twitter in string format
            #username = splitjson['user']['screen_name']  # The username of the Tweet 
            #userlang = splitjson['user']['lang']  # The userlang of the Tweet author
            #userid = splitjson['user']['id_str']  # The userid of the Tweet author
            #usertimezone = splitjson['user']['time_zone']  # The user time zone of the Tweet author
            #userlocation = splitjson['user']['location']  # The user location of the Tweet author
            #followers = splitjson['user']['followers_count']  # The number of followers the Tweet author has
            #text = splitjson['text']  # The entire body of the Tweet
            #hashtags = splitjson['entities']['hashtags']  # Any hashtags used in the Tweet
            dt = datajson['created_at']  # The timestamp of when the Tweet was created
            #language = splitjson['lang'] # The language of the Tweet
            created = datetime.datetime.strptime(dt, '%a %b %d %H:%M:%S +0000 %Y')
            #tweet = {'id':tweet_id, 'userid':userid, 'username':username, 'userlang':userlang, 'userlocation':userlocation, 'usertimezone':usertimezone, 'text':text, 'hashtags':hashtags, 'language':language, 'created':created}
            #db.twitter_search_split.insert(tweet) #collection name
            i=i+1
            #grab the 'created_at' data from the Tweet to use for display
            created_at = datajson['created_at']
            total+=1
            #print out a message to the screen that we have collected a tweet
            print("###  Tweet collected at " + str(created) +'  ###\n' +"Server Time: " + time.strftime('%X') + "     Started : " + start_time + "  Collected : "+ str(total) )
            
           

            
            #insert the data into the mongoDB into a collection called twitter_search
            #if twitter_search doesn't exist, it will be created.
            db.twitter_search.insert(datajson)
        except Exception as e:
           print(e)

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
#Set up the listener. The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.
listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True)) 
streamer = tweepy.Stream(auth=auth, listener=listener)
print("Tracking: " + str(WORDS))

def start_stream():
    while True:
        try:
            streamer = tweepy.Stream(auth=auth, listener=listener)
            streamer.filter(track=WORDS)
        except: 
            continue

start_stream()



