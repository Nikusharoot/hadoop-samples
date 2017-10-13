import sys
import random
from random import randint

masks = [
0,
int('10000000000000000000000000000000',2),
int('11000000000000000000000000000000',2),
int('11100000000000000000000000000000',2),
int('11110000000000000000000000000000',2),
int('11111000000000000000000000000000',2),
int('11111100000000000000000000000000',2),
int('11111110000000000000000000000000',2),
int('11111111000000000000000000000000',2),
int('11111111100000000000000000000000',2),
int('11111111110000000000000000000000',2),
int('11111111111000000000000000000000',2),
int('11111111111100000000000000000000',2),
int('11111111111110000000000000000000',2),
int('11111111111111000000000000000000',2),
int('11111111111111100000000000000000',2),
int('11111111111111110000000000000000',2),
int('11111111111111111000000000000000',2),
int('11111111111111111100000000000000',2),
int('11111111111111111110000000000000',2),
int('11111111111111111111000000000000',2),
int('11111111111111111111100000000000',2),
int('11111111111111111111110000000000',2),
int('11111111111111111111111000000000',2),
int('11111111111111111111111100000000',2),
int('11111111111111111111111110000000',2),
int('11111111111111111111111111000000',2),
int('11111111111111111111111111100000',2),
int('11111111111111111111111111110000',2),
int('11111111111111111111111111111000',2),
int('11111111111111111111111111111100',2),
int('11111111111111111111111111111110',2),
int('11111111111111111111111111111111',2)
]

signRemoverMask = int('01111111111111111111111111111111',2)

def getRandomIP( netMask ):
   result = netMask.split('/')
   maskNumber = int(result[1])
   adressParts = result[0].split('.')
   address = (((((int(adressParts[0])<<8) + int(adressParts[1]))<<8) + int(adressParts[2]))<<8) + int(adressParts[3])
   address = (address & masks[maskNumber]) + randint(1, (~masks[maskNumber] & signRemoverMask))
   addressHexStr = "%08X" % address
   addressStr = '%d.%d.%d.%d' % (int(addressHexStr[0:2], 16), int(addressHexStr[2:4], 16), int(addressHexStr[4:6], 16), int(addressHexStr[6:8], 16))
#   print "Inside the function :", addressStr
   return addressStr;


#result = getRandomIP("1.0.140.0/23")
#print "Outcome of the function : ", result[0]