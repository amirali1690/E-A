import pyodbc
import datetime
import csv
import time

import config


conns= config.conns2

def get_claim(cursor,ID):
    query = "SELECT C.*, CP.Identifier1Code,CP.Identifier1 "\
            "FROM Claims C "\
            "LEFT JOIN ClaimPayers CP ON CP.ID=C.ClaimPayerID "\
            "WHERE C.ID="+str(ID)
    cursor.execute(query)
    rows = cursor.fetchall()
    row = rows[0]
    claimInfo={}
    claimInfo['claimNo']=row[1]
    claimInfo['charge']=row[2]
    claimInfo['firstBilled']=row[-13].strftime('%m/%d/%Y')
    claimInfo['claimPayerID']=row[-12]
    claimInfo['claimMemberID']=row[-6]
    claimInfo['PayorID']=row[-1]
    claimInfo['PayorIDCode']=row[-2]
    return claimInfo

def get_claimMember(cursor,ID):
    query = "SELECT CM.*,P.AccountNo, P.CaseType, IP.PatientID "\
            "FROM ClaimMembers CM "\
            "LEFT JOIN InsPolicies IP ON IP.ID=CM.InsPolicyID "\
            "LEFT JOIN Patients P ON P.ID=IP.PatientID "\
            "WHERE CM.ID="+str(ID)
    cursor.execute(query)
    rows = cursor.fetchall()
    row = rows[0]
    claimInfo={}
    claimInfo['claimBillingProviderID']=row[1]
    claimInfo['lastName']=row[4]
    claimInfo['firstName']=row[5]
    claimInfo['subscriberID']=row[8]
    claimInfo['address1']=row[9]
    claimInfo['address2']=row[10]
    claimInfo['city']=row[11]
    claimInfo['state']=row[12]
    claimInfo['zipcode']=row[13]
    claimInfo['gender']=row[14]
    claimInfo['birthdate']=row[15].strftime('%m/%d/%Y')
    claimInfo['plan #']=row[19]
    claimInfo['insPolicyID']=row[23]
    claimInfo['payerSequenceCode']=row[24]
    claimInfo['PatientAccountNo']=row[-3]
    claimInfo['PatientCaseType']=row[-2]
    claimInfo['patientID']=row[-1]
    return claimInfo

def get_billedCharge(cursor,claimLineID):
    query = "SELECT BC.AppliedAmt,BC.paidDate,T.TranAmt,T.AllowedAmt,T.PriPaidAmt,"\
            "T.WOAmt,T.SecPaidAmt,T.ID "\
            "FROM BilledCharges BC "\
            "LEFT JOIN Transactions T ON BC.ChargeTranID=T.ID "\
            "WHERE BC.ClaimLineID="+str(claimLineID)
    cursor.execute(query)
    rows = cursor.fetchall()
    if rows:
        row = rows[0]
        #print(row)
        claimInfo={}
        claimInfo['charge']=row[2]
        claimInfo['paidAmount']=row[4]
        claimInfo['secondary']=row[6] if row[6]!=None else 0
        claimInfo['primary'] = row[4] if row[4]!=None else 0
        claimInfo['allowed'] = row[3] if row[3] !=None else 0
        claimInfo['insBalance']=claimInfo['allowed']-(claimInfo['primary']+claimInfo['secondary'])
        claimInfo['lastpaid']=row[1].strftime('%m/%d/%Y') if row[1]!=None else 0
       
    else:
        claimInfo={}
    return claimInfo


def get_claimPaymentLines(cursor,claimLineID):
    query = "SELECT PL.*,P.PostedDate "\
            "FROM PaymentLines PL "\
            "LEFT JOIN PaymentClaims PC ON PC.ID=PL.PaymentClaimID "\
            "LEFT JOIN Payments P ON P.ID=PC.PaymentID "\
            "WHERE PL.ClaimLineID="+str(claimLineID)
    cursor.execute(query)
    rows = cursor.fetchall()
    if rows:
        row = rows[-1]
        claimInfo={}  
        claimInfo['charge']=row[11]
        claimInfo['paidAmount']=row[12]
        claimInfo['insBalance']=row[27] if row[27]!=None else 0
        if claimInfo['paidAmount']==0:
            claimInfo['insBalance']=claimInfo['charge']
        claimInfo['secondary']=row[28]
        claimInfo['secondaryBalance']=row[30]
        claimInfo['lastpaid']=row[-1].strftime('%m/%d/%Y') if row[-1]!=None else 0
    else:
        claimInfo=get_billedCharge(cursor,claimLineID) 
    return claimInfo
    
def get_claimBillingProvier(cursor,claimBillingProviderID):
    query = "SELECT CBP.BatchDetailID,CBP.LastName,CBP.Identifier1Code,CBP.Identifier1,"\
            "CBP.Identifier2Code,CBP.Identifier2,CBP.Address1,CBP.Address2,CBP.City,"\
            "CBP.State,CBP.ZipCode,D.FullName,D.TIN,D.NPI,D.FacilityNPI "\
            "FROM ClaimBillingProviders CBP "\
            "LEFT JOIN Doctors D ON D.ID=CBP.DoctorID "\
            "WHERE CBP.ID="+str(claimBillingProviderID)
            #0-3 4-8 9-14
    cursor.execute(query)
    rows = cursor.fetchall()
    if rows:
        row = rows[0]
        claimInfo={}  
        claimInfo['batchDetailID']=row[0]
        claimInfo['FacilityName']=row[1]
        claimInfo['Identifier1Code']=row[2]
        claimInfo['Identifier1']=row[3]
        claimInfo['Identifier2Code']=row[4]
        claimInfo['Identifier2']=row[5]
        claimInfo['FacilityAddress1']=row[6]
        claimInfo['FacilityAddress2']=row[7]
        claimInfo['FacilityCity']=row[8]
        claimInfo['FacilityState']=row[9]
        claimInfo['FacilityZipCode']=row[10]
        claimInfo['FacilityNPI']=row[14]
    else:
        claimInfo={}  
    return claimInfo       

def get_provider (cursor,claimID):
    query = "SELECT CP.EntityTypeCode, D.FullName,D.TIN,D.NPI "\
            "FROM ClaimProviders CP "\
            "LEFT JOIN Doctors D ON D.ID=CP.DoctorID "\
            "WHERE ClaimID="+str(claimID)
    cursor.execute(query)
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            if row[0]=='1':
                providerInfo={}
                providerInfo['Provider']=row[1]
                providerInfo['ProviderTIN']=row[2]
                providerInfo['ProviderNPI']=row[3]
            
    else:
        providerInfo={}
        
    return providerInfo


def get_insurances(cursor,patientID,svcDate):
    query = "SELECT * "\
            "FROM InsPolicies "\
            "WHERE PatientID="+str(patientID)+" AND (TerminationDate>='"+svcDate+"' OR TerminationDate IS NULL)"
    cursor.execute(query)
    rows = cursor.fetchall()
    insurances={}
    for row in rows:
        insurance={}
        insurance['ID']=row[0]
        insurance['coveragetype']=row[2]
        insurance['seq']=row[3]
        insurance['ID#']=row[5]
        insurance['companyAddress']=row[29]
        insurance['companyCity']=row[30]
        insurance['companyState']=row[31]
        insurance['companyZip']=row[32]
        insurance['companyPhone']=row[33]
        if row[42]:
            insurance['EffectiveDate']=row[42].strftime('%m/%d/%Y')
        else:
            insurance['EffectiveDate'] = None
        insurance['companyName']=row[45]
        insurance['GroupNo']=row[56]
        insurances[insurance['ID']]=insurance
        
    return insurances
        
        

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


def get_claimLines(cursor,date):
    query = "SELECT CL.* "\
            "FROM ClaimLines CL "\
            "LEFT JOIN Claims C ON C.ID=CL.ClaimID "\
            "WHERE C.DocumentState<>6 AND CL.BeginDate>='"+date+"' AND BeginDate<'2022-05-01'"
    cursor.execute(query)
    rows = cursor.fetchall()
    claims = []
    length=len(rows)
    i=0
    print(length)
    for row in rows: 
        i+=1
        print(i)
        claim = {}
        claim['claimLineID']=row[0]
        claim['claimID']=row[1]
        providerInfo = get_provider(cursor,claim['claimID'])
        for key in providerInfo.keys():
            claim[key]=providerInfo[key]
        claim['sequence']=row[2]
        claim['code']=row[3]
        claim['chargeAmount']=row[4]
        claim['unit']=row[6]
        claim['serviceDateSQL']=row[11].strftime('%Y-%m-%d')
        claim['serviceDate']=row[11].strftime('%m/%d/%Y')
        claim['modifier']=str(row[14])
        if row[15] is not None:
            claim['modifier']=claim['modifier']+'-'+str(row[15])
        if row[16] is not None:
            claim['modifier']=claim['modifier']+'-'+str(row[16])
        if row[17] is not None:
            claim['modifier']=claim['modifier']+'-'+str(row[17]) 
        claim['diagnosisPointer']=str(row[18])
        if row[19] is not None:
            claim['diagnosisPointer']=claim['diagnosisPointer']+str(row[19])
        if row[20] is not None:
            claim['diagnosisPointer']=claim['diagnosisPointer']+str(row[20])
        if row[21] is not None:
            claim['diagnosisPointer']=claim['diagnosisPointer']+str(row[21]) 
        claimInfo=get_claim(cursor,claim['claimID']) 
        for key in claimInfo.keys():
            claim[key]=claimInfo[key]
        claimMember = get_claimMember(cursor, claim['claimMemberID'])
        for key in claimMember.keys():
            claim[key]=claimMember[key]
        claimPayment = get_claimPaymentLines(cursor, claim['claimLineID'])
        if claimPayment=={}:
            claim['charge']=claim['chargeAmount']
            claim['paidAmount']=0
            claim['insBalance']=claim['chargeAmount']
            claim['lastpaid']=None
        else:
            for key in claimPayment.keys():
                claim[key]=claimPayment[key]
        claimProvider=get_claimBillingProvier(cursor,claim['claimBillingProviderID'])
        for key in claimProvider.keys():
            claim[key]=claimProvider[key]
        claim['insurances']=get_insurances(cursor,claim['patientID'],claim['serviceDateSQL'])
        claim['Diagnosis Code']=get_claim_diagnosis(cursor,claim['claimID'])
        if claim['claimID']==48876:
            print(claimPayment,claim['claimLineID'])
        claims.append(claim)
    return claims
    

date = datetime.datetime(2021,4,1).strftime('%Y-%m-%d')
claims ={}
for clinic in conns:
    print(clinic)
    conn = conns[clinic]
    cursor=conn.cursor()
    if clinic=='Goodyear':
        claims[clinic]=get_claimLines(cursor,date)
        

fields=['PatientAccountNo','PatientCaseType','firstName','lastName',
       'birthdate','gender', 'address1','address2','city','state',
       'zipcode','serviceDate','code','sequence','Diagnosis Code',
       'diagnosisPointer','modifier','Provider','ProviderNPI',
       'ProviderTIN','FacilityName','FacilityAddress1','FacilityAddress2',
       'FacilityCity','FacilityState','FacilityZipCode','lastPaid',
       'chargeAmount','insBalance','ID#','InsuranceCompany','InsuranceAddress',
       'InsurancePhone','InsuranceEffective','GroupNo','Paid']
output={}
for clinic in claims.keys():
    output[clinic]=[]
    length= len(claims[clinic])
    i=0
    for claim in claims[clinic]:
        temp=[]
        #temp.append(claim['claimID'])
        #temp.append(claim['claimBillingProviderID'])
        #temp.append(claim['claimMemberID'])
        temp.append(claim['PatientAccountNo'])
        temp.append(claim['PatientCaseType'])
        temp.append(claim['firstName'])
        temp.append(claim['lastName'])
        temp.append(claim['birthdate'])
        temp.append(claim['gender'])
        temp.append(claim['address1'])
        temp.append(claim['address2'])
        temp.append(claim['city'])
        temp.append(claim['state'])
        temp.append(claim['zipcode'])
        temp.append(claim['serviceDate'])
        temp.append(claim['code'])
        temp.append(claim['sequence'])
        temp.append(claim['Diagnosis Code'])
        temp.append(claim['diagnosisPointer'])
        temp.append(claim['modifier'])
        temp.append(claim['Provider'])
        temp.append(claim['ProviderNPI'])
        temp.append(claim['ProviderTIN'])
        temp.append(claim['FacilityName'])
        temp.append(claim['FacilityAddress1'])
        temp.append(claim['FacilityAddress2'])
        temp.append(claim['FacilityCity'])
        temp.append(claim['FacilityState'])
        temp.append(claim['FacilityZipCode'])
        temp.append(claim['lastpaid'])
        temp.append(str(round(claim['chargeAmount'], 2)))
        temp.append(str(round(claim['insBalance'], 2)))
        if len(claim['insurances'])>1:
            for insuranceKey in claim['insurances'].keys():
                if claim['PatientAccountNo']==119614 and claim['code']=='97530' and claim['insurances'][insuranceKey]['companyName'].find('MUTUAL')!=-1:
                    print(temp,insuranceKey)
                if insuranceKey==claim['insPolicyID']:
                    tempx=[]
                    tempx=temp.copy()
                    tempx.append(claim['insurances'][insuranceKey]['ID#'])
                    tempx.append(claim['insurances'][insuranceKey]['companyName'])
                    tempx.append(claim['insurances'][insuranceKey]['companyAddress'])
                    tempx.append(claim['insurances'][insuranceKey]['companyPhone'])
                    tempx.append(claim['insurances'][insuranceKey]['EffectiveDate'])
                    tempx.append(claim['insurances'][insuranceKey]['GroupNo'])
                    tempx.append(claim['paidAmount'])
                    output[clinic].append(tempx)
                    i+=1
                else:
                    tempx=[]
                    tempx=temp.copy()
                    tempx.append(claim['insurances'][insuranceKey]['ID#'])
                    tempx.append(claim['insurances'][insuranceKey]['companyName'])
                    tempx.append(claim['insurances'][insuranceKey]['companyAddress'])
                    tempx.append(claim['insurances'][insuranceKey]['companyPhone'])
                    tempx.append(claim['insurances'][insuranceKey]['EffectiveDate'])
                    tempx.append(claim['insurances'][insuranceKey]['GroupNo'])
                    tempx[28]=claim['secondaryBalance'] if 'secondaryBalance' in claim.keys() else 0
                    tempx.append(claim['secondary']) if 'secondary' in claim.keys() else tempx.append(0)
                    output[clinic].append(tempx)
                    i+=1
        elif len(claim['insurances'])==0:
            temp.append('N/A')
            temp.append('N/A')
            temp.append('N/A')
            temp.append('N/A')
            temp.append('N/A')
            temp.append('N/A')
            temp.append('N/A')
        else:
            x=list(claim['insurances'].keys())
            print(claim['claimID'],claim['patientID'],claim['PatientAccountNo'],claim['serviceDate'],x)
            temp.append(claim['insurances'][x[0]]['ID#'])
            temp.append(claim['insurances'][x[0]]['companyName'])
            temp.append(claim['insurances'][x[0]]['companyAddress'])
            temp.append(claim['insurances'][x[0]]['companyPhone'])
            temp.append(claim['insurances'][x[0]]['EffectiveDate'])
            temp.append(claim['insurances'][x[0]]['GroupNo'])
            temp.append(claim['paidAmount'])
            output[clinic].append(temp)
            i+=1
    print(i,length)


for clinic in output.keys():
    with open(clinic+'.csv','w',newline='\n') as csvfile:
        write = csv.writer(csvfile)
        write.writerow(fields)
        for row in output[clinic]:
            if row[28]!='0' and row[28]!=0 and row[28]!=None and row[28]!='0.00':
                write.writerow(row)
            
        