#gerekli kutuphanelerin cagirilmasi basladi
import pymongo
import json
import pprint
#bitti
myclient = pymongo.MongoClient("mongodb://192.168.0.24:27017/")  #mongo sunucusu ip adresinin tanımlanması
mydb = myclient["twitterdb_2018_11"] #sunucu uzerindeki veri tabanin adinin tanimlanmasi 
mycol = mydb["twitter_search"] #veritabani uzerindeki yiginin belirtilmesi
#bir  json objesi yaratıldı basladi
t='{ "created":"1"}'
y = json.loads(t)
counter = 0
countert = 0
#bitti
#mongo sorgusu


for x in mycol.find({'lang': 'tr'},{"retweeted_status.entities.hashtags.text":1,"entities.hashtags.text":1,"_id":0}) : #"entities.hashtags.text":1 ,"_id":0
 #tagların temizlik islemleri basladi
 countert = countert + 1
 pprint.pprint("Query Number : " + str(countert))
 
 if('retweeted_status' in x) :
  data = x['retweeted_status']['entities']['hashtags']
 else:
  data = x['entities']['hashtags']
 if(data):
  pprint.pprint(data)
  jdata = json.dumps(data)
  redata = jdata.replace("[","")
  redata = redata.replace("]","")
  redata = redata.replace(" ","")
  redata = redata.replace("{","")
  redata = redata.replace("}","") 
  redata = redata.replace('"','')
  redata = redata.replace("text","")
  redata = redata.replace(":","")
  redata = redata.lower()
  spdata = redata.split(",")
  #print (spdata)
#bitti 
#gelen kelime json dosyasında var mı kontrol ediliyor 
  for temp in spdata:
    counter = counter + 1
    cword=temp in y
    #eger kelime  varsa tekrarlanma sayisini alarak 1 ekliyor
    if(cword):
    
     num=int(y[temp])
     num = num + 1
    
     #yapia icerindeki guncel olmayan teklime tekrari verisi siliniyor
     del y[temp]
     #silinen veri guncel haliyse yapiya tekrar ekleniyor
     y[temp] = str(num)
    
    else:
    #kelime yapi uzerinde mevcut degilse kelimeyi tekrarlanma sayisi 1 olarak yapiya ekliyor
     y[temp] = '1'

     
     

#olusan yapi ekrana bastiriliyor
print (y)
#yapi dosya olarak kayit ediliyor
path = 'D:\python_out\\nov_lang_tr_out.json' # dosya yolu belirtildi
file = open(path,'w') #dosya yolu ve parametre gonderildi

file.write(json.dumps(y)) #yazdirma islemi yapildi

file.close() # dosya kapatildi

print("Turkey (lang : tr)")
print("Counted Hashtags = " + str(counter))
print("TT : " + str(countert))

#Notlar : 
  #kelimeler arasında gorunen u'\u7684' tarzi yazım kodları  utf-8 tarafından temizleme  sirasında  olusturulmustur  bunlar cince karakterleri  ve 
  #benzer karakterlere sahip dilleri icerir

