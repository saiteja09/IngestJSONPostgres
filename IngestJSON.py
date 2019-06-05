import psycopg2
import os

jsondirectory = 'alljson'
jsonfiles = os.listdir(jsondirectory)
con =  psycopg2.connect(database="postgres", user="postgres", password="<password>", host="localhost", port="5432")
insertSQL = "INSERT INTO matches(cricsheetid, matchdata) VALUES (%s, %s)"
for jsonfile in jsonfiles:
    jsondata = open(jsondirectory + '/' + jsonfile, 'r')
    cricSheetId = jsonfile.replace(".json","")
    #print(cricSheetId)
    #print(jsondata.read())
    checkcur = con.cursor()
    checkcur.execute("SELECT * FROM matches WHERE cricsheetid=" + cricSheetId)
    
    if(checkcur.rowcount == 0):
        insertcur = con.cursor()
        insertcur.execute(insertSQL, (cricSheetId, jsondata.read()))
        con.commit()
        print("Inserted " + cricSheetId)
    else:
        print("Data already present")
    jsondata.close()
    
con.close()        
    
