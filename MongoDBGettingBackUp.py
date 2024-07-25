import datetime
import time
from datetime import date, timedelta
import os
##get date from yesterday
dbdate=datetime.date.today() - timedelta(1)
print(dbdate)

#### Backup Begin
# mongodump --db twitterdb_karakoy_2019-02-25 ---out /home/pi/mongodump Dump os command
commandBackup =  "mongodump --db twitterdb_karakoy_"+str(dbdate)+" --out /home/pi/mongodump"
os.system(commandBackup)
#### Backup End
#### Zipping Begin
# zip -r myfiles.zip mydir zipping for os command
commandZipping = "zip -r twitterdb_karakoy_"+str(dbdate)+".zip /home/pi/mongodump/twitterdb_karakoy_"+str(dbdate)
os.system(commandZipping)
####Zipping End
