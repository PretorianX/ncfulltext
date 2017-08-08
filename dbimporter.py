#!/usr/bin/python3
from elasticsearch import Elasticsearch
from elasticsearch_dsl import DocType, Date, Integer, Keyword, Text
from elasticsearch_dsl.connections import connections
import time, re
from os import listdir
import base64
import sys
import postgresql
import random
import codecs
import email


mails = "/root/messages"
with open('logindata.py') as f:
    credentials = [x.strip().split(':') for x in f.readlines()]

for username,password,dbip,dbname in credentials:
    db = postgresql.open('pq://username:password@dbip:5432/dbname')

#Connection to Elastic
#connections.create_connection(hosts=['162.255.118.53:9200'])
#connection to PGSQL
#db = postgresql.open('pq://connector:iddqd@162.255.118.54:5432/spamcollection3')
#List all files in maildir
mail_dir = listdir(mails)
#mail_stdin = sys.stdin.readlines()
#print(mail_stdin)
for filename in mail_dir:
    print(filename)
    #We open all files in utf-8 encoding and ignore encoding errors
    filecontent = codecs.open(mails+"/"+filename,"r", encoding='utf-8', errors='ignore').read()
    #get filecontent as class - message
    message = email.message_from_string(filecontent)
    msgfrom = message['from']
    msgdate = message['date']
    msgtype = message['content-type']
    msgip = message['received']
    #get through all message types and get one with rfc822 (read rfc)
    for part in message.walk():
        if part.get_content_type() == 'message/rfc822':
           #message with rfc 822 has two parts. We need one that has From field not empty
           for part2 in part.walk():
                if isinstance(part2['from'], str):
                    header = ""
                    for key,value in part2.items():
                        header += key+": "+value+"\n"

                    #strip email from From field in case it has other text
                    abuseremail = re.findall(r'[A-Za-z0-9\.\+_-]+@[A-Za-z0-9\.\+_-]+[A-Za-z]+', part2['from'])
                    initdate = part2['date']
                    try:
                        amail = abuseremail[0]
                    except IndexError:
                        amail = 'null'
                    #strip ip from Received field, some servers edit received field, so if ip is removed we set null
                    if isinstance(part2['received'], str):
                        serverip = re.findall(r'[0-9]+(?:\.[0-9]+){3}', part2['received'])
                        try:
                            ip = serverip[0]
                        except IndexError:
                            ip = 'null'
                    else:
                        ip = 'null'
                    #to generate id field and be sure it will "never be the same again" we take
                    # random number curtimestamp we encode abuseremail to base64 and merge it into one line
                    messdraft_id = str(random.getrandbits(64))+str(int(time.time()))+amail
                    messageid = base64.b64encode(messdraft_id.encode('utf-8'))
                    header_base = base64.b64encode(header.encode('utf-8'))
                    body = part2.get_payload()
                    #put the data to psql base
                    ins = db.prepare("INSERT INTO main_sc3_1 (feedback_name,feddback_time,messages_timestamp,reporter_ip, abuser_from, messages_id)"
                             " VALUES ($1,$2,$3,$4,$5,$6)")
                    ins(msgfrom,msgdate,initdate,ip,amail,messageid.decode('utf-8'))
                    inslave = db.prepare("INSERT INTO slave_sc3_1 (messages_id,feed_prov_txt,header_orig,body_orig)"
                                 " VALUES ($1,$2,$3,$4)")
                    inslave(messageid.decode('utf-8'),'',str(header_base),str(body))
