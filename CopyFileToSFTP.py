# -*- coding: utf-8 -*-
"""
Created on Fri Mar 18 13:30:56 2022

@author: BackFit Corp
"""
import pysftp
import os
import logging
import shutil
import schedule
import time
import datetime

import config
path = "C:\\Users\\BackFit Corp\\Desktop\\E&A billing"


cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

passDic = config.passDic





def copy_file_to_sftp(passDic,filepath,clinic):
    print(datetime.datetime.now())
    with pysftp.Connection('sftp.eamedbill.com', username=passDic[clinic]['username'], password=passDic[clinic]['password'],cnopts=cnopts) as sftp:
        with sftp.cd('Charges'):
            sftp.put(filepath)
            
            
def main():
    global passDic
    print(datetime.datetime.now())
    alreadyUploaded = open("Logs\\uploaded.log",'r')
    alreadyUploaded = alreadyUploaded.read().splitlines()
    uploadedList=open("Logs\\uploaded.log",'a')
    clinics = ['Chandler','Queen Creek','Desert Ridge','South Chandler']
    for clinic in clinics:
        print(clinic)
        dir_list = os.listdir(path+'\\FilesSent\\'+clinic)
        logging.basicConfig(filename="Logs\\BatchUploaded.log", format='%(asctime)s %(levelname)s:%(message)s',level=logging.INFO)
        if len(dir_list)>0:
            for file in dir_list:
                src_path = path+'\\FilesSent\\'+clinic+'\\'+file
                dst_path = path+'\\FilesUploaded\\'+clinic+'\\'+file
                uploaded_path = path+'\\FilesUploaded\\'+clinic+'\\'+file 
                if file[:-4] in alreadyUploaded:
                    logging.info(file[:-4]+" has been uploaded before")
                else:
                    try:
                        shutil.move(src_path,dst_path)
                        copy_file_to_sftp(passDic,dst_path,clinic)
                        time.sleep(10)
                        logging.info(file[:-4]+' Uploaded')
                        uploadedList.write(file[:-4]+"\n")
                    except:
                        logging.critical(file[:-4]+' has not been uploaded')
        else:
            logging.warning(clinic+' no files were ready to upload')
if __name__=='__main__':
    schedule.every().day.at("08:50").do(main)
    schedule.every().day.at("09:11").do(main)
    schedule.every().day.at("10:10").do(main)
    schedule.every().day.at("11:10").do(main)
    schedule.every().day.at("12:41").do(main)
    schedule.every().day.at("13:03").do(main)
    schedule.every().day.at("14:17").do(main)
    schedule.every().day.at("15:37").do(main)
    schedule.every().day.at("16:10").do(main)
    while schedule.jobs:
        schedule.run_pending()
        time.sleep(1)
        
        