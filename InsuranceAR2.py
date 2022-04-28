# -*- coding: utf-8 -*-
"""
Created on Fri Apr  1 13:27:18 2022

@author: BackFit Corp
"""

import pyodbc
import datetime
import csv
import time
from decimal import *

import config


conns= config.conns

def get_billed_charges(cursor,chargeTranID,priPaid,disputeAmount):
    query = "SELECT BC.InsPolID,BC.BilledDate,BC.PaidDate,BC.AppliedAmt, "\
            "BC.ClaimLineID,I.InsuredIDNo,I.InsuredSex,I.CompanyAddress, "\
            "I.CompanyCity,I.CompanyState,I.CompanyZip,I.InsCoName,I.PolGrpFECANum "\
            "FROM BilledCharges BC "\
            "LEFT JOIN InsPolicies I ON I.ID=BC.InsPolID "\
            "WHERE BC.ChargeTranID='"+str(chargeTranID)+"' "\
            "ORDER BY BC.InsPolID DESC, BC.BilledDate ASC"
    cursor.execute(query)
    rows = cursor.fetchall()
    claimInfo = {}
    length=len(rows)
    claimLineOut='Error'
    for row in rows:
        insPolID=row[0]
        billedDate=row[1]
        paidDate=row[2] if row[2]!=None else ''
        paid=row[3] if row[3]!=None else 0
        claimLine=row[4]
        insID=row[5]
        insSex=row[6]
        insAddress=row[7]
        insCity=row[8]
        insState=row[9]
        insZip=row[10]
        insCompany=row[11]
        insGroupNo=row[12]
        if claimLine!=None:
            claimLineOut=claimLine
        if insPolID not in claimInfo.keys():
            claimInfo[insPolID]={'firstBilledDate':billedDate,'lastBilledDate':billedDate,
                                 'lastPaidDate':paidDate,'paid':paid,'insuranceID':insID,'patientSex':insSex,
                                 'insuranceAddress':insAddress,'insuranceCity':insCity,'insuranceState':insState,
                                 'insuranceZip':insZip,'insuranceCompany':insCompany,'insuranceGroupNo':insGroupNo}
        else:
            claimInfo[insPolID]['paid']=paid+claimInfo[insPolID]['paid']
            if paidDate!='':
                if claimInfo[insPolID]['lastPaidDate']!='':
                    if claimInfo[insPolID]['lastPaidDate']<paidDate:
                        claimInfo[insPolID]['lastPaidDate']=paidDate
                else:
                    claimInfo[insPolID]['lastPaidDate']=paidDate
            if claimInfo[insPolID]['lastBilledDate']<billedDate:
                claimInfo[insPolID]['lastBilledDate']=billedDate
            if claimInfo[insPolID]['firstBilledDate']>billedDate:
                claimInfo[insPolID]['firstBilledDate']=billedDate
    temp =claimInfo.copy()
    for ins in temp.keys():
        if claimInfo[ins]['paid']==priPaid and priPaid!=Decimal('0.0000') and len(temp.keys())>1:
            claimInfo.pop(ins)
    return claimLineOut,claimInfo


def get_claim_diagnosis(cursor,claimID):
    query = " SELECT * FROM ClaimDiagnoses "\
            "WHERE ClaimID="+str(claimID)+" "\
            "ORDER BY Sequence ASC"
            
    cursor.execute(query)
    rows = cursor.fetchall()
    #if len(rows)>2:
     #   print(claimID)
    diagnosis=''
    length=len(rows)
    if length>1:
        i=0
        while i<length:
            diagnosis=diagnosis+str(rows[i][2])
            if length-i>1:
                diagnosis= diagnosis+', '
            i+=1
        
    return diagnosis


def get_insurance_balance(tran,allowed,pat,pripaid,secpaid,patpaid,discount,wo,coins,disp):
    if pat==0 and pripaid==0 and secpaid==0 and patpaid==0 and discount==0 and wo==0 and coins==0 and disp==0:
        insurance_balance=allowed
    elif disp!=0:
        insurance_balance=disp
    elif coins!=0:
        insurance_balance=coins
    return insurance_balance

def get_claim(cursor,claimLineID):
    query = "SELECT CL.Modifier1,CL.Modifier2,CL.Modifier3,CL.Modifier4, "\
            "CL.DiagnosisPointer1,CL.DiagnosisPointer2,CL.DiagnosisPointer3,CL.DiagnosisPointer4,C.ID "\
            "FROM ClaimLines CL "\
            "LEFT JOIN Claims C ON C.ID=CL.ClaimID "\
            "LEFT JOIN ClaimMembers CM ON CM.ID=C.ClaimMemberID "\
            "WHERE CL.ID='"+str(claimLineID)+"'"
    cursor.execute(query)
    rows = cursor.fetchall()
    length=len(rows)
    claimInfo={}
    if length>1:
        print(length)
    for row in rows:
        claimInfo['modifier']=str(row[0])
        if row[1] is not None:
            claimInfo['modifier']=claimInfo['modifier']+'-'+str(row[1])
        if row[2] is not None:
            claimInfo['modifier']=claimInfo['modifier']+'-'+str(row[2])
        if row[3] is not None:
            claimInfo['modifier']=claimInfo['modifier']+'-'+str(row[3]) 
        claimInfo['diagnosisPointer']=str(row[4])
        if row[5] is not None:
            claimInfo['diagnosisPointer']=claimInfo['diagnosisPointer']+str(row[5])
        if row[6] is not None:
            claimInfo['diagnosisPointer']=claimInfo['diagnosisPointer']+str(row[6])
        if row[7] is not None:
            claimInfo['diagnosisPointer']=claimInfo['diagnosisPointer']+str(row[7]) 
        claimInfo['claimID']=row[8]
    return claimInfo

def get_transactions(cursor,date):
    query = "SELECT T.TranDate,T.Code,T.TranAmt,T.AllowedAmt,T.PatAmt, "\
            "T.PriPaidAmt,T.SecPaidAmt,T.PatPaidAmt,T.DiscountAmt,T.WOAmt,"\
            "T.CoInsAmt,T.DispAmt,T.PriInsPolID,T.ApptID,T.PatID,T.PatientOther, "\
            "P.FirstName,P.LastName,P.BirthDate,P.CaseType,P.Address,P.City,P.state, "\
            "P.Zip,T.ID,D.FirstName,D.LastName,D.NPI,D.FacilityNPI,D.FacilityStreet,D.FacilityStreet2, "\
            "D.FacilityCity,D.FacilityState,D.FacilityZip,P.accountNo,D.TIN "\
            "FROM Transactions T "\
            "LEFT JOIN Patients P ON P.ID=T.PatID "\
            "LEFT JOIN Appointments A ON A.ID=T.ApptID "\
            "LEFT JOIN Doctors D ON A.DoctorID=D.ID "\
            "WHERE T.TranSubType='SV' AND (((T.AllowedAmt!=0 OR T.AllowedAmt IS NULL) AND T.DispAmt>0) OR (T.PriPaidAmt=0 AND T.WOAmt=0 AND T.PatAmt=0) OR T.CoInsAmt>0) AND T.TranDate>='"+date+"' " \
            "AND T.CODE!='$RapidTest' AND T.TranDate<'2022-03-10' AND P.CaseType NOT LIKE 'PI%' "\
            "ORDER BY P.LastName DESC"
            #AND T.TranDate<'2021-06-29' 
    print(query)
    cursor.execute(query)
    rows = cursor.fetchall()
    transactions = []
    length=len(rows)
    i=0
    print(length)
    for row in rows: 
        print(i)
        i+=1
        transaction={}
        transaction['serviceDate']=row[0]
        transaction['code']=row[1]
        transaction['charge']=row[2] if row[2]!=None else 0
        transaction['allowed']=row[3] if row[3]!=None else 0
        transaction['patientPaid']=row[4] if row[4]!=None else 0
        transaction['paidAmount']=row[5] if row[5]!=None else 0
        transaction['secPaidAmount']=row[6] if row[6]!=None else 0
        transaction['patientPaidAmount']=row[7] if row[7]!=None else 0
        transaction['discountAmount']=row[8] if row[8]!=None else 0
        transaction['writeOffAmount']=row[9] if row[9]!=None else 0
        transaction['coInsAmt']=row[10] if row[10]!=None else 0
        transaction['disputeAmount']=row[11] if row[11]!=None else 0
        transaction['insurancePolicyID']=row[12]
        transaction['appointmentID']=row[13]
        transaction['patientID']=row[14]
        transaction['patientOther']=row[15]
        transaction['patientFirstName']=row[16]
        transaction['patientLastName']=row[17]
        transaction['patientBirthDate']=row[18].strftime('%m/%d/%Y')
        transaction['patientCaseType']=row[19]
        transaction['patientAddress']=row[20]
        transaction['patientCity']=row[21]
        transaction['patientState']=row[22]
        transaction['patientZip']=row[23]
        transaction['id']=row[24]
        transaction['doctorFirstName']=row[25]
        transaction['doctorLastName']=row[26]
        transaction['doctorNPI']=row[27]
        transaction['facilityNPI']=row[28]
        transaction['facilityName']=row[29]
        transaction['facilityAddress']=row[30]
        transaction['facilityCity']=row[31]
        transaction['facilityState']=row[32]
        transaction['facilityZip']=row[33]
        transaction['patientAccountNo']=row[34]
        transaction['doctorTIN']=row[35]
        transaction['insuranceBalance']=get_insurance_balance(transaction['charge'], transaction['allowed'],transaction['patientPaid'] , transaction['paidAmount'],
                                        transaction['secPaidAmount'], transaction['patientPaidAmount'], transaction['discountAmount'], transaction['writeOffAmount'],
                                        transaction['coInsAmt'], transaction['disputeAmount'])
        transaction['claimLineID'],chargeInfo=get_billed_charges(cursor,transaction['id'],transaction['paidAmount'],transaction['disputeAmount'])
        for inspolicyID in chargeInfo.keys():
            transaction['insPolicyID']=inspolicyID
            for key in chargeInfo[inspolicyID].keys():
                transaction[key]=chargeInfo[inspolicyID][key]
        
        if transaction['claimLineID']!='Error':
            claimInfo=get_claim(cursor,transaction['claimLineID'])
            transaction['diagnosis code']=get_claim_diagnosis(cursor, claimInfo['claimID'])
            for key in claimInfo.keys():
                transaction[key]=claimInfo[key]
        else:
            transaction['diagnosis code']='Unknown'
            transaction['diagnosisPointer']=''
            transaction['modifier']=''
            transaction['firstBilledDate']=''
            transaction['lastBilledDate']=''
            transaction['lastPaidDate']=''
            transaction['paid']=0
            transaction['insuranceID']=''
            transaction['insuranceAddress']=''
            transaction['insuranceCity']=''
            transaction['insuranceState']=''
            transaction['insuranceZip']=''
            transaction['insuranceCompany']=''
            transaction['insuranceGroupNo']=''
            
            
        transactions.append(transaction)    
    return transactions


clinic='Queen Creek'
conn=conns[clinic]
cursor=conn.cursor()
date = datetime.datetime(2021, 4, 1).strftime('%Y-%m-%d')
transactions=get_transactions(cursor,date)

fields=['PatientAccountNo','PatientCaseType','firstName','lastName',
       'birthdate', 'address','city','state',
       'zipcode','serviceDate','code','Diagnosis Code',
       'diagnosisPointer','modifier','ProviderFirstName','ProviderLastName','ProviderNPI',
       'ProviderTIN','FacilityName','FacilityAddress',
       'FacilityCity','FacilityState','FacilityZipCode','lastPaid',
       'chargeAmount','insBalance','ID#','InsuranceCompany','InsuranceAddress',
       'GroupNo','Paid','Primary Paid','Last Billed']

#for clinic in output.keys():
    
with open(clinic+'.csv','w',newline='\n') as csvfile:
    write = csv.writer(csvfile)
    write.writerow(fields)
    for transaction in transactions:
        print(transaction['serviceDate'],transaction['id'],transaction['patientFirstName'],transaction['patientLastName'],transaction['code'])
        row = [transaction['patientAccountNo'],transaction['patientCaseType'],transaction['patientFirstName'],transaction['patientLastName'],
               transaction['patientBirthDate'],transaction['patientAddress'],transaction['patientCity'],transaction['patientState'],
               transaction['patientZip'],transaction['serviceDate'],transaction['code'],transaction['diagnosis code'],
               transaction['diagnosisPointer'],transaction['modifier'],transaction['doctorFirstName'],transaction['doctorLastName'],transaction['doctorNPI'],
               transaction['doctorTIN'],transaction['facilityName'],transaction['facilityAddress'],
               transaction['facilityCity'],transaction['facilityState'],transaction['facilityZip'],transaction['lastPaidDate'],
               transaction['charge'],transaction['insuranceBalance'],transaction['insuranceID'],transaction['insuranceCompany'],transaction['insuranceAddress'],
               transaction['insuranceGroupNo'],transaction['paid'],transaction['paidAmount'],transaction['lastBilledDate']
               ]
        write.writerow(row)
        