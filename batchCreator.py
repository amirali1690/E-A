# -*- coding: utf-8 -*-
"""
Created on Mon Jan 31 15:30:57 2022

@author: BackFit Corp
"""

import batchFunctions
import datetime
import logging
import schedule
import time

def main():
    print(datetime.datetime.now())
    logging.basicConfig(filename="Logs\\BatchCreation.log", format='%(asctime)s %(levelname)s:%(message)s',level=logging.INFO)
    conns=batchFunctions.conns
    createdBatches=[]
    missedBatches=[]
    alreadyCreated = open("Logs\\created.log",'r')
    alreadyCreated = alreadyCreated.read().splitlines()
    createdList=open("Logs\\created.log",'a')
    for key in conns.keys():
        print(key)
        print(datetime.datetime.now())
        cursor = conns[key].cursor()
        result=0
        while result==0:
            try:
                batches = batchFunctions.get_batches(cursor,key)
                result=1
            except:
                time.sleep(60)
            
        for batch in batches:
            filename=key+'-'+str(batch[0])
            if filename in alreadyCreated:
                logging.info(filename+" has been created before")
                continue
            else:
                result=batchFunctions.create_batch(batch[0],batch[1])
                logging.info(filename+' run')
                if result==True:
                    createdBatches.append(batch)
                    logging.info(filename+" created")
                    createdList.write(filename+"\n")
                else:
                    missedBatches.append(batch)
                    logging.critical(filename+" Wasn't created")
    return createdBatches,missedBatches


if __name__=='__main__':
    schedule.clear()
    schedule.every().day.at("09:00").do(main)
    schedule.every().day.at("10:00").do(main)
    schedule.every().day.at("11:00").do(main)
    schedule.every().day.at("12:00").do(main)
    schedule.every().day.at("13:00").do(main)
    schedule.every().day.at("14:00").do(main)
    schedule.every().day.at("15:00").do(main)
    schedule.every().day.at("16:00").do(main)
    schedule.every().day.at("22:00").do(main)
    while schedule.jobs:
        schedule.run_pending()
        time.sleep(1)
#main()
