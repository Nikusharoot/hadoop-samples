import sys
import csv
import datetime
#import collections
import random
from random import randint
import StringIO
import socket

#names = collections.defaultdict(str) 

with open('productnames.csv', 'rb') as f:
  names = list(csv.reader(f))
with open('addresses.csv', 'rb') as f:
  addresses = list(csv.reader(f))

output = StringIO.StringIO()

#writer = csv.writer(open("test.csv", 'w'))
writer = csv.writer(output)
 
#writer.writerow( ('Purchase date', 'Product Name', 'Product Price', 'Product Category',  'User address') )
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(( '127.0.0.1', 44444 ))
#s.setblocking(False)

for i in range(200000000):
  
  pos = randint(1, len(names))
  adrPos = randint(1, len(addresses))
  day = randint(1, 7)
  price = max(0.1, random.gauss(100, 100))

  target_date_time_s = int(round(min(60*60*24, max(0, random.gauss(60*60*24/2, 60*60*24/4)))))
  base_datetime = datetime.datetime( 2017, 8, day )
  delta = datetime.timedelta( 0, target_date_time_s )
  target_date = base_datetime + delta
  
  if( pos < len(names) and adrPos < len (addresses)):
    if(len(names[pos]) > 1 and len (addresses[adrPos]) > 0):
      writer.writerow( (target_date.strftime('%Y-%m-%d %H:%M:%S'), names[pos][1], "%.2f" % price,  names[pos][0], addresses[adrPos][0]))
#      print '{0}'.format(target_date.strftime('%Y-%m-%d %H:%M:%S'))
      line = output.getvalue()
  
      try:
#        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#        s.connect(( '127.0.0.1', 44444 ))
        s.sendall(line)
        print line
#        s.shutdown(socket.SHUT_WR)
        while 0:
          data = s.recv(1024)
          if data == "":
              break
          print "Received:", repr(data)
#        s.close()
      except Exception:
        print 'Error!!!: ', sys.exc_info()
        
 
      output.truncate(0);

