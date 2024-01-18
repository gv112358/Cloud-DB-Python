import pandas as pd
from apiclient import discovery
from httplib2 import Http
from oauth2client import client, file, tools
from datetime import datetime
import dbclient as db
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import time
import sys


#define path variables
credentials_file_path = './credentials/credentials.json'
clientsecret_file_path = './credentials/client_secrets.json'

drive = None
cur = None

#define API scope
SCOPE = 'https://www.googleapis.com/auth/drive'

def OttieniCredenziali():
    # define store
    store = file.Storage(credentials_file_path)
    credentials = store.get()
    # get access token
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(clientsecret_file_path, SCOPE)
        credentials = tools.run_flow(flow, store)


#OttieniCredenziali() solo prima autenticazione per creare credentials.json
def SendFile(folder_id,sFile):
    global drive
    gfile = drive.CreateFile({'parents': [{'id': folder_id}]})
    # Read file and set it as the content of this instance.
    gfile.SetContentFile(sFile)
    # Upload the file.
    gfile.Upload()

#link cartella google drive inncui caricare il file
#https://drive.google.com/drive/folders/1qWVELZHzAC_qkamAszi_uo2Mh_TaJMfe?usp=drive_link
#ID cartella: 1qWVELZHzAC_qkamAszi_uo2Mh_TaJMfe?usp
#link file: 
#https://drive.google.com/file/d/1P7nxwp-k-BAJi8PPjuiBZADcawr4Ys4g/view?usp=drive_link
#https://drive.google.com/file/d/1P7nxwp-k-BAJi8PPjuiBZADcawr4Ys4g/view?usp=drive_link
#ID file: 1P7nxwp-k-BAJi8PPjuiBZADcawr4Ys4g

file_id = '1P7nxwp-k-BAJi8PPjuiBZADcawr4Ys4g'
folder_id = '1qWVELZHzAC_qkamAszi_uo2Mh_TaJMfe?usp'


def ReplaceFile(folder_id,file_id,new_file):
    gfile = drive.CreateFile({'parents': [{'id': folder_id}],'id':file_id})
    # Read file and set it as the content of this instance.
    gfile.SetContentFile(new_file)
    # Upload the file.
    gfile.Upload()

#ReplaceFile(folder_id,file_id,'img.jpg')

def EseguiQueryUpdateCSV(iStep,sFile):
    global cur
    sQuery = "select * from ordini limit " + str(iStep*5)
    #Esegui query al DB
    iNumRighe = db.read_in_db(cur,sQuery)
    if iNumRighe > 0:
        try:
            f = open(sFile, "w")
            
            for i in range(iNumRighe):
                r_vect = db.read_next_row(cur)
                if r_vect[0] == 0:
                    #Salva i record nel file sFileToUpload
                    f.write(str(r_vect[1]))
                    f.write("\n")
                    #print(r_vect[1][0])
            f.close()
            #print(r_vect[1][0])            
        except:
            #segnalare problema nel file di log
            #inviare una mail, lasciare un sms
            print("Errore gestione file")
    return



iTimeToSleepInMin = 3
sFileToUpload = "dati.csv"
sQuery = "select * from ordini where Date=current_month"

cur = db.connect()


gauth = GoogleAuth()
gauth.LoadCredentialsFile(credentials_file_path)
drive = GoogleDrive(gauth)

#SendFile('14oehx2G2xsVrhdzFv66b0u2FOyPKbm21',sFileToUpload)
#sys.exit()

iStep = 1
#https://drive.google.com/file/d/1hGdhuhw4-UPivRidp7ujbu3XxYN85f7K/view?usp=drive_link
while(True):
    EseguiQueryUpdateCSV(iStep, sFileToUpload)
    iStep = iStep + 1
    now = datetime.now()
    date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
    print(date_time, ": inizio procedura invio")
    ReplaceFile('14oehx2G2xsVrhdzFv66b0u2FOyPKbm21',"1hGdhuhw4-UPivRidp7ujbu3XxYN85f7K",sFileToUpload)
    print("File inviato")
    time.sleep(iTimeToSleepInMin*60)

"""
gfile = drive.CreateFile({'parents': [{'id': '1qWVELZHzAC_qkamAszi_uo2Mh_TaJMfe'}]})
#Read file and set it as the content of this instance.
gfile.SetContentFile('2.jpg')
#Upload the file.
gfile.Upload()

"""