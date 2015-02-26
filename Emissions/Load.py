import psycopg2 as DB
import csv

conn = DB.connect("dbname=Mechaman")
cur = conn.cursor()

# Import CSV Information
'''
# HHV2PUB
csvObject = csv.reader(open(r'HHV2PUB.CSV','r'), dialect = 'excel', delimiter = ',')

listHeaders= list(next(csvObject))
#indPerson = listHeaders.index('PERSONID')
indHouse = listHeaders.index('HOUSEID')
#indTdd = listHeaders.index('TDAYDATE')

cur.execute("CREATE TABLE House (HOUSEID varchar, PRIMARY KEY(HOUSEID));")

for row_Person in csvObject:
	hou = list(row_Person)[indHouse]


	cur.execute("INSERT INTO House (HOUSEID) VALUES (%s)", (hou,))
'''
print("Vehicle table...")
# VEHV2PUB
csvObject = csv.reader(open(r'VEHV2PUB.CSV','r'), dialect = 'excel', delimiter = ',')

listHeaders= list(next(csvObject))
indPerson = listHeaders.index('PERSONID')
indHouse = listHeaders.index('HOUSEID')
indTdd = listHeaders.index('TDAYDATE')
indVid = listHeaders.index('VEHID')
indEpa = listHeaders.index('EPATMPG')

cur.execute("CREATE TABLE Vehicle (PERSONID varchar, HOUSEID varchar, TDAYDATE varchar, VEHID numeric, EPATMPG numeric, PRIMARY KEY(HOUSEID, VEHID));")

for row_Person in csvObject:
	per = list(row_Person)[indPerson]
	hou = list(row_Person)[indHouse]
	tdd = list(row_Person)[indTdd]
	vid = list(row_Person)[indVid]
	epa = list(row_Person)[indEpa]

	cur.execute("INSERT INTO Vehicle (PERSONID, HOUSEID, TDAYDATE, VEHID, EPATMPG) VALUES (%s, %s, %s, %s, %s)", (per, hou, tdd, vid, epa))

#DAYV2PUB
print("Trip table...")
csvObject = csv.reader(open(r'DAYV2PUB.CSV','r'), dialect = 'excel', delimiter = ',')

listHeaders= list(next(csvObject))
indPerson = listHeaders.index('PERSONID')
indHouse = listHeaders.index('HOUSEID')
indTrip = listHeaders.index('TDTRPNUM')
indMile = listHeaders.index('TRPMILES')
indTdd = listHeaders.index('TDAYDATE')

cur.execute("CREATE TABLE Trip (PERSONID varchar, HOUSEID varchar, TRIPNUM varchar, TRPMILES integer, TDAYDATE varchar, PRIMARY KEY(PERSONID, HOUSEID, TRIPNUM));")

for row_Person in csvObject:
	per = list(row_Person)[indPerson]
	hou = list(row_Person)[indHouse]
	tri = list(row_Person)[indTrip]
	mil = float(list(row_Person)[indMile])
	if mil < 0:
		mil = 0
	tdd = list(row_Person)[indTdd]

	cur.execute("INSERT INTO Trip (PERSONID, HOUSEID, TRIPNUM, TRPMILES, TDAYDATE) VALUES (%s, %s, %s,%s, %s)", (per, hou, tri, mil, tdd))

print("EIACT table...")
# Table EIACT
csvObject = csv.reader(open(r'EIA_CO2_Transportation_2014.csv','r'), dialect = 'excel', delimiter = ',')

listHeaders= list(next(csvObject))
indMSN = listHeaders.index('MSN')
indDate = listHeaders.index('YYYYMM')
indCo2 = listHeaders.index('Value')

cur.execute("CREATE TABLE EIACT (MSN varchar, TDAYDATE numeric, CO2 numeric, PRIMARY KEY(MSN, TDAYDATE));")

for row_Person in csvObject:
	msn = list(row_Person)[indMSN]
	tdd = list(row_Person)[indDate]
	co2 = list(row_Person)[indCo2]
	cur.execute("INSERT INTO EIACT (MSN, TDAYDATE, CO2) VALUES (%s, %s, %s)", (msn, tdd, co2))

print("EIACE table...")
# TABLE EIACE --- metric tons of carbon dioxide due to electricity
csvObject = csv.reader(open(r'EIA_CO2_Electric_2014.csv','r'), dialect = 'excel', delimiter = ',')

listHeaders= list(next(csvObject))
indMSN = listHeaders.index('MSN')
indDate = listHeaders.index('YYYYMM')
indCo2 = listHeaders.index('Value')

cur.execute("CREATE TABLE EIACE (MSN varchar, TDAYDATE numeric, CO2 numeric, PRIMARY KEY(MSN, TDAYDATE));")

for row_Person in csvObject:
	msn = list(row_Person)[indMSN]
	tdd = list(row_Person)[indDate]
	co2 = list(row_Person)[indCo2]
	if co2 == 'Not Available':
		co2 = 0
	cur.execute("INSERT INTO EIACE (MSN, TDAYDATE, CO2) VALUES (%s, %s, %s)", (msn, tdd, co2))
print("EIAKWH table...")
# TABLE EIAKWH --- MkWh
csvObject = csv.reader(open(r'EIA_MkWh_2014.csv','r'), dialect = 'excel', delimiter = ',')

listHeaders= list(next(csvObject))
indMSN = listHeaders.index('MSN')
indDate = listHeaders.index('YYYYMM')
indMkwh = listHeaders.index('Value')

cur.execute("CREATE TABLE EIAKWH  (MSN varchar, TDAYDATE numeric, MKWH numeric, PRIMARY KEY(MSN, TDAYDATE));")

for row_Person in csvObject:
	msn = list(row_Person)[indMSN]
	tdd = list(row_Person)[indDate]
	mkwh = list(row_Person)[indMkwh]
	if mkwh == 'Not Available':
		mkwh = 0
	cur.execute("INSERT INTO EIAKWH (MSN, TDAYDATE, MKWH) VALUES (%s, %s, %s)", (msn, tdd, mkwh))

conn.commit()
conn.close()

print("Connection to database closed...")