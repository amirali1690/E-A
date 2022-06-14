# -*- coding: utf-8 -*-
"""
Created on Fri Jan 28 09:26:29 2022

@author: BackFit Corp
"""

import pyodbc
import datetime
import dict2xml


#clinic = 'Queen Creek'
#conn = conns[clinic]
#cursor=conn.cursor()

import config


conns= config.connsEA

facility_addresses={
                'Chandler':
                    {'LINE_1':'1949 W. Ray Rd.','LINE_2':'Suite 23','CITY':'Chandler','POSTAL_CODE':'85224','STATE':'AZ'}
                }
    
    
def create_batch(batchNo,clinic):
    conn = conns[clinic]
    cursor=conn.cursor()
    batch = get_batch(cursor,batchNo)
    facility = get_claim_BillingProvider(cursor, batchNo)
    claimMembers = get_claimIDs(cursor,facility['ID'])
    claimDetails = get_claims(cursor, claimMembers)
    batchxml = {
            'BATCH':{
                'DATE':batch['CreatedDate'].strftime('%m/%d/%Y'),
                'CLAIM_COUNT':batch['COUNT'],
                }
            }
    
    claims=[]
    for claim in claimDetails:
        claimdic={
                 'APPOINTMENT':{},
                 'FACILITY':{'ADDRESS':{}},
                 'PROVIDERS':{
                         'PROVIDER':[]
                         },
                 'PATIENT':{
                     'NAME':{},
                     'PHONE_NUMBERS':{},
                     'ADDRESSES':{
                             'ADDRESS':{}
                             },
                     'INSURANCES':{
                         'INSURANCE':{}
                         },
                     'EMPLOYMENT':{
                         'EMPLOYER':{
                                 'ADDRESS':{
                                    'LINE_1':'',
                                    'LINE_2':'',
                                    'CITY':'',
                                    'STATE':'',
                                    'POSTAL_CODE':'',
                                },
                                 'PHONE':''
                             }
                         }
                     },
                'PAYMENTS_POSTED':{
                                  'PAYMENT':[]
                                 },
                'DIAGNOSIS_CODES':{
                                    'DIAGNOSIS':[]
                                    },
                'PROCEDURE_CODES':{
                                'PROCEDURE':[]
                                  },
                'ADDITIONAL_INFO':{}
    
                }
        claimdic['ID']=claim['ID']
        claimdic['ADDITIONAL_INFO']['DATE_LASTXRAY']=claim['LastXrayDate']
        claimdic['ADDITIONAL_INFO']['DATE_ONSETCURRENTILLNESS']=claim['OnsetDate']
        claimdic['ADDITIONAL_INFO']['DATE_INITIAL_TREATMENT']=claim['TreatmentDate']
        claimdic['FACILITY']['NAME']=facility['NAME']
        claimdic['FACILITY']['ADDRESS']=facility['ADDRESS']
        claimdic['FACILITY']['NPI']=facility['NPI']
        claimdic['FACILITY']['POS']=claim['FACILITY_TYPE']
        claimLine,appointmentDate = get_claimLine(cursor,claim['DBID'])
        #print(claim['DBID'])
        claimdic['PROCEDURE_CODES']['PROCEDURE']=claimLine
        claimdic['APPOINTMENT']['DATE']=appointmentDate.date().strftime("%m/%d/%Y")
        claimdic['APPOINTMENT']['TIME']=appointmentDate.time().strftime("%H:%M")
        providers = get_claim_provider(cursor,claim['DBID'])
        claimPatient = get_claim_member(cursor, claim['ClaimMemberID'])
        #print(appointmentDate,claimPatient['InsuredName'],claimPatient['FirstName'],claimPatient['LastName'],claimPatient['patID'])
        patientInfo = get_patientInfo(cursor, appointmentDate,claimPatient['InsuredName'],claimPatient['FirstName'],claimPatient['LastName'],claimPatient['patID'])
        for procedure in claimdic['PROCEDURE_CODES']['PROCEDURE']:
            if procedure['DESCRIPTION'].find('NOC')==-1:
                procedure['DESCRIPTION']=get_description(cursor,patientInfo['patID'],appointmentDate,procedure['CODE'])
        claimdic['PATIENT']['NAME']['FIRST']=claimPatient['FirstName']
        claimdic['PATIENT']['NAME']['LAST']=claimPatient['LastName']
        claimdic['PATIENT']['NAME']['MIDDLE']=claimPatient['MiddleName']
        claimdic['PATIENT']['NAME']['SUFFIX']=claimPatient['Suffix']
        claimdic['PATIENT']['GENDER']=claimPatient['Gender']
        claimdic['PATIENT']['DATE_OF_BIRTH']=claimPatient['DateOfBirth']
        claimdic['PATIENT']['ADDRESSES']['ADDRESS']=claimPatient['ADDRESS']
        claimdic['APPOINTMENT']['ID']=patientInfo['apptID']
        claimdic['PATIENT']['MARITAL_STATUS']=patientInfo['maritalStatus']
        claimdic['PATIENT']['ID']=patientInfo['accountNo']
        claimdic['PATIENT']['EMPLOYMENT']['STATUS']=patientInfo['employmentStatus']
        claimdic['PATIENT']['EMPLOYMENT']['EMPLOYER']['NAME']=patientInfo['employerName']
        claimdic['PATIENT']['PHONE_NUMBERS']=get_contact(cursor,str(patientInfo['patID']))
        claimdic['PATIENT']['INSURANCES']=get_insurance(cursor, str(patientInfo['patID']),appointmentDate,claim['payer'])
        claimdic['DIAGNOSIS_CODES']['DIAGNOSIS']=get_claim_diagnosis(cursor, claim['DBID'])
        for provider in providers:
            if provider['TYPE']=='RENDERING':
                provider['ADDRESS']=facility['ADDRESS']
        claimdic['PROVIDERS']['PROVIDER']=providers
        claims.append(claimdic)
        
    
    batchxml['BATCH']['CLAIM']=claims
    xml = dict2xml.dict2xml(batchxml)
    if len(xml)<1:
        return False
    with open("FilesCreated\\"+clinic+'\\'+clinic+'-'+str(batchNo)+".xml","w") as f:
        f.write(xml)
    return True
def get_batches(cursor,clinic):
    today = datetime.datetime.today()
    yesterday = today- datetime.timedelta(days=6)
    today = today.strftime('%Y-%m-%d')
    yesterday = yesterday.strftime('%Y-%m-%d')
    query = "SELECT BD.ID FROM BatchDetails BD LEFT JOIN Batches B ON BD.BatchID=B.ID WHERE B.CreatedDate>='"+yesterday+"' AND B.BatchType='C'"
    cursor.execute(query)
    rows=cursor.fetchall()
    batches=[]
    for row in rows:
        print(row,clinic)
        batches.append([row[0],clinic])

    return batches
    
def get_batch(cursor,batchNo):
    query = "SELECT B.* FROM BatchDetails BD LEFT JOIN Batches B ON B.ID=BD.BatchID WHERE BD.ID="+str(batchNo)
    cursor.execute(query)
    batch = cursor.fetchall()
    batchDic={}
    batchDic['ID']=batch[0][0]
    batchDic['CreatedDate']=batch[0][2]
    batchDic['COUNT']=batch[0][3]
    batchDic['TOTAL']=round(batch[0][4],2)
    return batchDic

def get_claim_BillingProvider(cursor,batchDetailID):
    query = "SELECT * FROM ClaimBillingProviders WHERE BatchDetailID="+str(batchDetailID)
    cursor.execute(query)
    row = cursor.fetchall()
    row=row[0]
    ClaimBillingProvider={'ADDRESS':{}}
    ClaimBillingProvider['ID']=row[0]
    ClaimBillingProvider['NAME']=row[4]
    ClaimBillingProvider['NPI']=row[10]
    ClaimBillingProvider['EMPLOYERID']=row[12]
    ClaimBillingProvider['ADDRESS']['LINE_1']=row[13]
    ClaimBillingProvider['ADDRESS']['LINE_2']=(row[14] if row[14] is not None else '')
    ClaimBillingProvider['ADDRESS']['CITY']=row[15]
    ClaimBillingProvider['ADDRESS']['STATE']=row[16]
    ClaimBillingProvider['ADDRESS']['POSTAL_CODE']=row[17]
    ClaimBillingProvider['PROVIDER_ID']=row[22]
    return ClaimBillingProvider

def get_claimIDs(cursor,ID):
    query = "SELECT * FROM ClaimMembers WHERE ClaimBillingProviderID="+str(ID)
    cursor.execute(query)
    rows=cursor.fetchall()
    claimIDs=[]
    for row in rows:
        claimIDs.append(row[0])
    return claimIDs

def get_claims(cursor,claimMembers):
    query = "SELECT C.*,CP.Identifier1 FROM Claims C "\
            "LEFT JOIN ClaimPayers CP ON CP.ID=C.ClaimPayerID "\
            "WHERE ClaimMemberID IN ("
    for id in claimMembers:
        query = query + "'"+str(id)+"',"
    query = query[:-1]+')'
    cursor.execute(query)
    rows = cursor.fetchall()
    claims=[]
    for row in rows:
        claim={}
        claim['DBID']=row[0]
        claim['ID']=row[1]
        claim['CHARGED']=row[2]
        claim['FACILITY_TYPE']=row[3]
        claim['TreatmentDate']=(row[5].date().strftime("%m/%d/%Y") if row[5] is not None else '')
        claim['OnsetDate']=(row[7].date().strftime("%m/%d/%Y") if row[7] is not None else '')
        claim['LastXrayDate']=(row[11].date().strftime("%m/%d/%Y") if row[11] is not None else '')
        claim['PatientPaid']=row[18]
        claim['purchasedService']=row[19]
        claim['PayorID']=row[29]
        claim['ClaimMemberID']=row[35]
        claim['payer']=row[-1]
        claims.append(claim)
        
    return claims

def get_claim_provider(cursor,claimID):
    query = "SELECT * FROM ClaimProviders WHERE ClaimID="+str(claimID)
    cursor.execute(query)
    rows = cursor.fetchall()
    providers=[]
    for row in rows:
        if str(row[2])=='82':
            provider={'NAME':{},'ADDRESS':{},'PHONE':''}
            provider['TYPE']='RENDERING'
            provider['NAME']['FIRST']=row[5] if row[5] is not None else '' 
            provider['NAME']['LAST']=row[4] if row[4] is not None else ''
            provider['NAME']['MIDDLE']=row[6] if row[6] is not None else ''
            provider['NAME']['SUFFIX']=row[7] if row[7] is not None else ''
            provider['NPI']=row[9] if row[9] is not None else ''
            provider['ID']=row[-1]
            providers.append(provider)
        elif row[2]=='DN':
            provider={'NAME':{},'ADDRESS':{},'PHONE':''}
            provider['TYPE']='REFERRING'
            provider['NAME']['FIRST']=row[5] if row[5] is not None else '' 
            provider['NAME']['LAST']=row[4] if row[4] is not None else ''
            provider['NAME']['MIDDLE']=row[6] if row[6] is not None else ''
            provider['NAME']['SUFFIX']=row[7] if row[7] is not None else ''
            provider['NPI']=row[9] if row[9] is not None else ''
            provider['ID']=row[-1] if row[-1] is not None else ''
            providers.append(provider)
        else:
            continue
    return providers
    
ndc1 = 'N489130444401 ML2.5'
ndc2 = 'Lidocaine 2% NDC # 00409-4277-17 Xylocaine (Lidoc'
ndc3 = 'NOC Marlido Injection Kit NDC#07642-0730-01 Marcai'
ndc4 ='NOC MARLIDO INJ KIT NDC# 07642-0730-01 QUAL N4'

def parse_ndc(ndc):
    if ndc.find('NDC')==-1:
        ndc = ndc[:ndc.find(' ')]
        #print(0)
    else:
        if ndc.find('NDC # ')!=-1:
            #print(1)
            ndcPointer=ndc.find('NDC # ')
            ndc = ndc[ndcPointer+6:]
            ndc = ndc[:ndc.find(' ')]
        elif ndc.find('NDC# ')!=-1:
            #print(2)
            ndcPointer=ndc.find('NDC# ')
            ndc = ndc[ndcPointer+5:]
            ndc = ndc[:ndc.find(' ')]
        elif ndc.find('NDC #')!=-1:
            #print(3)
            ndcPointer=ndc.find('NDC #')
            ndc = ndc[ndcPointer+5:]
            ndc = ndc[:ndc.find(' ')]
        elif ndc.find('NDC#')!=-1:
            #print(4)
            ndcPointer=ndc.find('NDC#')
            ndc = ndc[ndcPointer+4:]
            ndc = ndc[:ndc.find(' ')]
    return ndc


def get_description(cursor,patientID,apptDate,Code):
    query = "SELECT Description FROM Transactions WHERE Code='"+str(Code)+"' AND PatID='"+str(patientID)+"' AND TranDate='"+apptDate.strftime("%Y-%m-%d %H:%M:%S")+"' AND TranType='C' "
    cursor.execute(query)
    rows = cursor.fetchall()
    description=''
    for row in rows:
        description = row[0]
    return description


def get_claimLine(cursor,claimID):
    query = "SELECT * FROM ClaimLines WHERE ClaimID ="+str(claimID)
    cursor.execute(query)
    rows = cursor.fetchall()
    procedure_codes=[]
    for row in rows:
        procedure={'TIME':{'START':'','STOP':'','MINUTES':'','ATTENDING':''},'UNITS':{}}
        procedure['TIME']['START']=row[11].time().strftime("%H:%M")
        procedure['DOS_BEGIN']=row[11].date().strftime("%m/%d/%Y")
        procedure['DOS_END']=row[11].date().strftime("%m/%d/%Y")
        procedure['CODE']=row[3]
        procedure['UNITS']['BASE']=row[6]
        procedure['CHARGE']=round(row[4],2)
        procedure['MODIFIERS']=(str(row[14]) if row[14] is not None else '')
        if row[15] is not None:
            procedure['MODIFIERS']=procedure['MODIFIERS']+'-'+str(row[15])
        if row[16] is not None:
            procedure['MODIFIERS']=procedure['MODIFIERS']+'-'+str(row[16])
        if row[17] is not None:
            procedure['MODIFIERS']=procedure['MODIFIERS']+'-'+str(row[17])
        procedure['DX']=(str(row[18]) if row[18] is not None else '')
        if row[19] is not None:
            procedure['DX']=procedure['DX']+str(row[19])
        if row[20] is not None:
            procedure['DX']=procedure['DX']+str(row[20])
        if row[21] is not None:
            procedure['DX']=procedure['DX']+str(row[21])
        
        procedure['DESCRIPTION']=row[-1]
        procedure['NDC']=str(parse_ndc(row[-1]) if row[-1] is not None else '')
        appointmentDate = row[11]
        procedure_codes.append(procedure)
    return procedure_codes,appointmentDate
  

def get_claim_member(cursor,claimMemberID):
    query="SELECT CM.*,IP.InsuredName,IP.PatientID "\
            "FROM ClaimMembers CM "\
            "LEFT JOIN InsPolicies IP ON CM.InsPolicyID=IP.ID "\
            "WHERE CM.ID ="+str(claimMemberID)
    cursor.execute(query)
    rows = cursor.fetchall()
    row=rows[0]
    claimMember={'ADDRESS':{}}
    claimMember['isPatient']=row[2]
    claimMember['RelationshipCode']=row[3]
    claimMember['LastName']=row[4]
    claimMember['FirstName']=row[5]
    claimMember['InsuredName']=row[-1]
    claimMember['MiddleName']=row[6] if row[6] is not None else '' 
    claimMember['Suffix']=row[7] if row[7] is not None else '' 
    claimMember['InsuredID']=row[8] if row[8] is not None else '' 
    claimMember['GroupNo']=(row[19] if (row[19] is not None and row[19].lower()!='none') else '')
    claimMember['DateOfBirth']=row[15].strftime('%m/%d/%Y') if row[15] is not None else '' 
    claimMember['Gender']= 'MALE' if row[16]=='M' else 'FEMALE' 
    claimMember['ADDRESS']=[{}]
    claimMember['ADDRESS'][0]['LINE_1']=row[9] if row[9] is not None else '' 
    claimMember['ADDRESS'][0]['LINE_2']=row[10] if row[10] is not None else '' 
    claimMember['ADDRESS'][0]['CITY']=row[11] if row[11] is not None else ''   
    claimMember['ADDRESS'][0]['STATE']=row[12] if row[12] is not None else '' 
    claimMember['ADDRESS'][0]['POSTAL_CODE']=row[13] if row[13] is not None else '' 
    claimMember['InsPolicyID']=row[23]
    claimMember['patID']=row[-1]
    return claimMember

#  2022-01-14 15:45:00

def employment_update(employment):
    if employment.lower().find('student')!=-1:
        if employment.lower().find('part')!=-1:
            status='PART TIME STUDENT'
        else:
            status='FULL TIME STUDENT'
    elif employment.lower().find('unemployed')!=-1:
        status='UNEMPLOYED'
    elif employment.lower().find('retired')!=-1:
        status = 'RETIRED'
    elif employment=='':
        status = 'UNKNOWN'
    else:
        status = 'EMPLOYED'
    return status

def marital_update(marital):
    if marital.lower().find('divorce')!=-1 or marital.lower().find('separate')!=-1:
        status='DIVORCED'
    elif marital.lower().find('single')!=-1:
        status='SINGLE'
    elif marital.lower().find('widowed')!=-1:
        status='WIDOWED'
    elif marital=='':
        status='OTHER'
    else:
        status='MARRIED'
    return status
        
'''

  get_patientInfo(cursor,datetime.datetime(2022,5,19,14,00,00),38283 ,'Susan','Wozniak',38283)
'''  
def get_patientInfo(cursor,dateTime,insName,first,last,patID):
    hour = dateTime.strftime('%H')
    dateBefore = dateTime.strftime('%Y-%m-%d ')
    dateAfter = dateTime.strftime('%Y-%m-%d ')
    hourAfter=int(hour)+6
    hourBefore=int(hour)-6
    if hourAfter==24:
        hourAfter=0
        dateAfter = dateTime+datetime.timedelta(days=1)
        dateAfter = dateAfter.strftime('%Y-%m-%d ')
    if hourAfter>24:
        hourAfter=hourAfter-24
        dateAfter = dateTime+datetime.timedelta(days=1)
        dateAfter = dateAfter.strftime('%Y-%m-%d ')
    if hourBefore<0:
        hourBefore = hourBefore+24
        dateBefore = dateTime-datetime.timedelta(days=1)
        dateBefore = dateBefore.strftime('%Y-%m-%d ')
    dateTimeBefore=dateBefore+str(hourBefore)+':00:00'
    dateTimeAfter=dateAfter+str(hourAfter)+':00:00'
    query = "SELECT A.ID,P.AccountNo,P.EmployerName,P.EmploymentStatus,P.MaritalStatus,P.ID "\
            "FROM Appointments A "\
            "LEFT JOIN Patients P ON A.PatientID=P.ID "\
            "WHERE P.ID='"+str(patID)+"' AND ((A.ScheduleDateTime>='"+str(dateTimeBefore)+"' AND A.ScheduleDateTime<='"+str(dateTimeAfter)+"') OR (A.CheckInDateTime>='"+str(dateTimeBefore)+"' AND A.CheckInDateTime<='"+str(dateTimeAfter)+"'))"
    cursor.execute(query)
    rows = cursor.fetchall()
    row = rows[0]
    patientInfo={}
    patientInfo['apptID']=row[0]
    patientInfo['accountNo']=row[1]
    patientInfo['employerName']=(row[2] if row[2] is not None else '')
    patientInfo['employmentStatus']=employment_update(row[3] if row[3] is not None else '')
    patientInfo['maritalStatus']=marital_update(row[4] if row[4] is not None else '')
    patientInfo['patID']=row[5]
    return patientInfo
            
def get_appointment(cursor,appointmentID):
    query= "SELECT DoctorID,PatientID,ScheduleDateTime FROM appointments "\
            "WHERE ID="+appointmentID
    cursor.execute(query)
    rows = cursor.fetchall()
    appointment={}
    appointment['DoctorID']=rows[0][0]
    appointment['PatientID']=rows[0][1]
    appointment['date']=rows[0][2].date().strftime("%m/%d/%Y")
    appointment['time']=rows[0][2].time().strftime("%H:%M")
    return appointment

def get_provider(cursor,providerID):
    query= "SELECT FullName,FacilityStreet,FacilityCity,FacilityState,FacilityZip, "\
            "FacilityStreet2, NPI,FacilityNPI,FirstName,LastName,MiddleName "\
            "FROM Doctors "\
            "WHERE ID="+providerID
            # 0 5
    cursor.execute(query)
    rows = cursor.fetchall()
    provider={'ADDRESS':{},'NAME':{}}
    facility={'ADDRESS':{},'NAME':''}
    facility['NAME']= rows[0][1]
    facility['NPI']=rows[0][7]
    provider['NAME']={'FIRST':rows[0][8],'MIDDLE':rows[0][10],'LAST':rows[0][9],'SUFFIX':''}
    provider['NPI']=rows[0][6]
    return provider,facility

def get_claim_diagnosis(cursor,claimID):
    query = " SELECT * FROM ClaimDiagnoses "\
            "WHERE ClaimID="+str(claimID)
    cursor.execute(query)
    rows = cursor.fetchall()
    #if len(rows)>2:
     #   print(claimID)
    diagnosises=[]
    for row in rows:
        diagnosis= {}
        diagnosis['ORDER']=row[1]
        diagnosis['ICD']=row[2]
        diagnosises.append(diagnosis)
    
    return diagnosises


def get_diagnosis(cursor,appointmentID):
    query= "SELECT Seq,Code,Description FROM Diagnoses "\
            "WHERE AppointmentID="+appointmentID
    cursor.execute(query)
    rows = cursor.fetchall()
    diagnosises=[]
    for row in rows:
        diagnosis={}
        diagnosis['ORDER']=row[0]
        diagnosis['ICD10']=row[1]
        diagnosis['DESCRIPTION']=row[2]
        diagnosises.append(diagnosis)
    return diagnosises


def get_patient(cursor,patientID):
    query= "SELECT FirstName,LastName,MiddleName,Suffix,Sex,BirthDate,Address,City,State,Zip,"\
            "MaritalStatus,BillDoctorID "\
            "FROM Patients "\
            "WHERE ID="+patientID
    cursor.execute(query)
    rows = cursor.fetchall()
    patient={}
    patient['FirstName']=rows[0][0]
    patient['LastName']=rows[0][1]
    patient['MiddleName']=rows[0][2]
    patient['Suffix']=rows[0][3]
    patient['Sex']=rows[0][4]
    patient['BirthDate']=rows[0][5].date().strftime("%m/%d/%Y")
    patient['Address']=rows[0][6]
    patient['City']=rows[0][7]
    patient['State']=rows[0][8]
    patient['Zip']=rows[0][9]
    patient['MaritalStatus']=rows[0][10]
    patient['ProviderID']=rows[0][11]
    return patient

def split_name(name):
    comma = name.find(',')
    last = name[:comma].capitalize()
    first = name[comma+2:].capitalize()
    return first,last

def get_pos(cursor,tranID):
    query = 'SELECT PlaceOfService,DaysUnits,M1,M2 FROM chargeDetails '\
        "WHERE ChargeTranID="+tranID
    cursor.execute(query)
    rows = cursor.fetchall()
    tran={}
    tran['POS']=rows[0][0]
    tran['Units']=rows[0][1]
    tran['Modifiers']= rows[0][2]+'-'+rows[0][3]
    return tran

def get_insurance(cursor,patientID,appointmentDate,payerID):
    query = "SELECT IP.Seq,IP.CompanyAddress,IP.CompanyCity,IP.CompanyState,IP.CompanyZip, " \
            "IP.insuredIDNo,IP.InsuredName,IP.InsuredAddress,IP.InsuredCity,IP.InsuredState, "\
            "IP.InsuredZip,IP.Relationship,IP.InsCoName,IP.PolGrpFECANum,IP.TerminationDate, "\
            "IP.EffectiveDate "\
        "FROM InsPolicies IP "\
        "WHERE IP.PatientID="+patientID
    # 5,10,
    cursor.execute(query)
    rows = cursor.fetchall()
    insurances={'INSURANCE':[]}
    for row in rows:
        terminationDate = row[14] if row[14] is not None else ''
        if terminationDate!='' and terminationDate<=appointmentDate:
            continue
        insurance={'ADDRESS':{},'SUBSCRIBER':{'NAME':{},'ADDRESS':{}}}
        insurance['ORDER']=row[0]
        if row[0]==1:
            insurance['PAYOR_ID']=payerID
        else:
            insurance['PAYOR_ID']=''
        insurance['NAME']=row[12]
        insurance['ADDRESS']['LINE_1']=row[1]
        insurance['ADDRESS']['LINE_2']=''
        insurance['ADDRESS']['CITY']=row[2]
        insurance['ADDRESS']['STATE']=row[3]
        insurance['ADDRESS']['POSTAL_CODE']=row[4]
        first,last = split_name(row[6])
        insurance['SUBSCRIBER']['NAME']['FIRST']=first
        insurance['SUBSCRIBER']['NAME']['LAST']=last
        insurance['SUBSCRIBER']['NAME']['MIDDLE']=''
        insurance['SUBSCRIBER']['NAME']['SUFFIX']=''
        insurance['SUBSCRIBER']['RELATIONSHIP'] = row[11].upper()
        insurance['SUBSCRIBER']['PIN'] = row[5]
        insurance['SUBSCRIBER']['GROUP'] = (row[13] if (row[13] is not None and row[13].lower()!='none') else '')
        insurance['SUBSCRIBER']['ADDRESS']['LINE_1']=row[7]
        insurance['SUBSCRIBER']['ADDRESS']['LINE_2']=''
        insurance['SUBSCRIBER']['ADDRESS']['CITY']=row[8]
        insurance['SUBSCRIBER']['ADDRESS']['STATE']=row[9]
        insurance['SUBSCRIBER']['ADDRESS']['POSTAL_CODE']=row[10]
        insurances['INSURANCE'].append(insurance)
    
    return insurances
        
def marital_status_modifier(status):
    if status=='' or status is None:
        status = 'OTHER'
    elif status.lower() in ['single','divorced','married','widowed']:
        status= status.upper()
    elif status.lower()=='separated':
        status = 'DIVORCED'
    else:
        status = 'OTHER'
    return status

def phone_modifier(phone):
    numericFilter = filter(str.isdigit,phone)
    numericString = "".join(numericFilter)
    phone = '('+numericString[0:3]+')'+numericString[3:6]+'-'+numericString[6:]
    return phone

def get_contact(cursor,patientID):
    query = "SELECT Description,Number FROM ContactInfos "\
            "WHERE PatientID="+patientID
    cursor.execute(query)
    rows = cursor.fetchall()
    contact={}
    for row in rows:
        if row[0]=='Home':
            contact['HOME']=phone_modifier(row[1])
        elif row[0]=='Cell':
            contact['CELL']=phone_modifier(row[1])
        elif row[0]=='Work':
            contact['WORK']=phone_modifier(row[1])
        else:
            contact['OTHER']=row[1]
    return contact

