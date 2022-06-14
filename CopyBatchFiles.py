# -*- coding: utf-8 -*-
"""
Created on Wed Mar  2 10:49:22 2022

@author: BackFit Corp
"""

import os
import logging
import shutil
import schedule
import time
import CopyFileToSFTP
import datetime
path = "C:\\Users\\BackFit Corp\\Desktop\\E&A billing"

def main():
    print(datetime.datetime.now())
    alreadyCopied = open("Logs\\copied.log",'r')
    alreadyCopied = alreadyCopied.read().splitlines()
    CopiedList=open("Logs\\copied.log",'a')
    clinics = ['Queen Creek','Desert Ridge','South Chandler','Goodyear']
    for clinic in clinics:
        print(clinic)
        dir_list = os.listdir(path+'\\FilesCreated\\'+clinic)
        logging.basicConfig(filename="Logs\\BatchCopied.log", format='%(asctime)s %(levelname)s:%(message)s',level=logging.INFO)
        if len(dir_list)>0:
            for file in dir_list:
                src_path = path+'\\FilesCreated\\'+clinic+'\\'+file
                dst_path = path+'\\FilesSent\\'+clinic+'\\'+file
                uploaded_path = path+'\\FilesUploaded\\'+clinic+'\\'+file 
                if file[:-4] in alreadyCopied:
                    logging.info(file[:-4]+" has been copied before")
                else:
                    try:
                        shutil.move(src_path,dst_path)
                        #CopyFileToSFTP.copy_file_to_sftp(CopyFileToSFTP.passDic,dst_path,clinic)
                        logging.info(file[:-4]+' copied')
                        CopiedList.write(file[:-4]+"\n")
                    except:
                        logging.critical(file[:-4]+' has not been copied')
        else:
            logging.warning(clinic+' no files were ready to move')
if __name__=='__main__':
    schedule.every().day.at("08:10").do(main)
    schedule.every().day.at("09:10").do(main)
    schedule.every().day.at("10:10").do(main)
    schedule.every().day.at("11:10").do(main)
    schedule.every().day.at("12:10").do(main)
    schedule.every().day.at("13:10").do(main)
    schedule.every().day.at("14:10").do(main)
    schedule.every().day.at("15:10").do(main)
    schedule.every().day.at("16:10").do(main)
    while schedule.jobs:
        schedule.run_pending()
        time.sleep(1)
