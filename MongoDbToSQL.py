#gerekli kutuphanelerin cagirilmasi basladi
import pymongo
import json
import mysql.connector
import pprint

#bitti
myclient = pymongo.MongoClient("mongodb://localhost:27017/")  #mongo sunucusu ip adresinin tanımlanması
mydb = myclient["test"] #sunucu uzerindeki veri tabanin adinin tanimlanmasi 
mycol = mydb["twitter_search"] #veritabani uzerindeki yiginin belirtilmesi
#mysql baglanti bilgisi basladi
mydb = mysql.connector.connect(
  host="",
  user="",
  passwd="",
  database=""
)
mysqlcursor = mydb.cursor()
#bitti
class tweet:
  id=""
  text=""
  source=""
  country_code=""
  hashtags=""
  lang=""
  created_at=""
  timestamp=""
  retweeted_id=""
  retweeted_text=""
  retweeted_source=""
  retweeted_country_code=""
  retweeted_hashtags=""
  retweeted_lang=""
  retweeted_created_at=""







counter = 0
countert = 0
#bitti
#mongo sorgusu





for x in mycol.find(({}),{"timestamp_ms":1,"source":1,"place":1,"retweeted_status":1,"text":1,"lang" :1 ,"created_at" : 1,"user.location":1,"entities.hashtags.text":1,"id":1,"_id":1},no_cursor_timeout=True) : #"entities.hashtags.text":1 ,"_id":0
 #tagların temizlik islemleri basladi
 countert = countert + 1
 print("Query Number : " + str(countert))
 if('extended_tweet' in x) :
   tweet.id=x["id"]
   tweet.text=x["extended_tweet"]["full_text"]
   tweet.source=x["source"]
   
   if x["place"]:
     tweet.country_code=x["place"]["country_code"]   
   else:
     tweet.country_code="Non"
   for temp in x["extended_tweet"]["entities"]["hashtags"] :
     tweet.hashtags=tweet.hashtags+temp["text"]+","  
   tweet.lang=x["lang"]
   tweet.created_at=x["created_at"]
   tweet.timestamp=x["timestamp_ms"]
 
 else:
   tweet.id=x["id"]
   tweet.text=x["text"]
   tweet.source=x["source"]
   
   if x["place"]:
     tweet.country_code=x["place"]["country_code"]   
   else:
     tweet.country_code="Non"     
   
   for temp in x["entities"]["hashtags"] :
     tweet.hashtags=tweet.hashtags+temp["text"]+","  
   tweet.lang=x["lang"]
   tweet.created_at=x["created_at"]
   tweet.timestamp=x["timestamp_ms"]

     
 


 if('retweeted_status' in x) :
   newdata=x["retweeted_status"]
   if('extended_tweet' in newdata) :
     tweet.retweeted_id=newdata["id"]
     tweet.retweeted_text=newdata["extended_tweet"]["full_text"]
     tweet.retweeted_source=newdata["source"]
     if newdata["place"]:
       tweet.retweeted_country_code=newdata["place"]["country_code"]
     else:
       tweet.retweeted_country_code="Non"
     
     for temp in newdata["extended_tweet"]["entities"]["hashtags"] :
       tweet.retweeted_hashtags=tweet.retweeted_hashtags+temp["text"]+","
           


     tweet.retweeted_lang=newdata["lang"]
     tweet.retweeted_created_at=newdata["created_at"]
     
     
   else:
     tweet.retweeted_id=newdata["id"]
     tweet.retweeted_text=["text"]
     tweet.retweeted_source=newdata["source"]     
     if newdata["place"]:
      tweet.retweeted_country_code=newdata["place"]["country_code"]
     else:
      tweet.retweeted_country_code="Non"
     #tweet.retweeted_hashtags=newdata["entities"]["hashtags"]
     tweet.retweeted_lang=newdata["lang"]
     tweet.retweeted_created_at=newdata["created_at"]


   sql = "INSERT INTO end_10 (t_id,t_text,t_sorces,t_country_code,t_hashtags,t_lang,t_created_at,t_timestamp,retweeted_id,retweeted_text,retweeted_source,retweeted_country_code,retweeted_hashtags,retweeted_lang,retweeted_created) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
   val = (int(tweet.id), str(tweet.text),str(tweet.source),str(tweet.country_code),str(tweet.hashtags),str(tweet.lang),str(tweet.created_at),int(tweet.timestamp),int(tweet.retweeted_id),str(tweet.retweeted_text),str(tweet.retweeted_source),str(tweet.retweeted_country_code),str(tweet.retweeted_hashtags),str(tweet.retweeted_lang),str(tweet.retweeted_created_at))

   mysqlcursor.execute(sql, val)
   mydb.commit()
   print(mysqlcursor.rowcount, "record inserted.")
   tweet.id=""
   tweet.text=""
   tweet.source=""
   tweet.country_code=""
   tweet.hashtags=""
   tweet.lang=""
   tweet.created_at=""
   tweet.timestamp=""
   tweet.retweeted_id=""
   tweet.retweeted_text=""
   tweet.retweeted_source=""
   tweet.retweeted_country_code=""
   tweet.retweeted_hashtags=""
   tweet.retweeted_lang=""
   tweet.retweeted_created_at=""
   tweet.retweeted_timestamp=""
   
   



