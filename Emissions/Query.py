import psycopg2 as DB
import csv

conn = DB.connect("dbname=Mechaman")
cur = conn.cursor()

###################### 
########################
print("Querying results for problem 3a...")
blah2 = "SELECT COUNT(*) FROM (SELECT PERSONID, HOUSEID FROM Trip GROUP BY PERSONID, HOUSEID) as table1"
cur.execute(blah2)
numPeople = list(cur.fetchall())
numPeople = float(numPeople[0][0])

for i in xrange(5,105,5):
	# Trip miles counted for each person/date combination. A person may be counted more than once. 
	blah = "SELECT PERSONID, HOUSEID FROM (SELECT PERSONID, HOUSEID, TDAYDATE, SUM(TRPMILES) as MILES FROM Trip GROUP BY TDAYDATE, PERSONID, HOUSEID) table1 WHERE MILES <  %s" % (i) 
	cur.execute(blah)
	row = cur.fetchall()
	print("Less than %s : " %(i))
	print(len(row)/numPeople)


######################  
######################
print("Querying results for problem 3b...")
for i in xrange(5,105,5):
	distancesQ = "SELECT SUM(TRPMILES) FROM ((SELECT * FROM Trip WHERE TRPMILES < %s)table1 NATURAL JOIN (SELECT * FROM Vehicle WHERE VEHID > 1)table3)table0" % (i) # Sum of distances traveled for a certain trip
	fuelQ = "SELECT SUM(Fuel) FROM (SELECT TRIPNUM, TRPMILES/EPATMPG AS Fuel  FROM ((SELECT * FROM Trip WHERE TRPMILES < %s)table1 NATURAL JOIN (SELECT * FROM Vehicle WHERE VEHID > 1)table3)table0)table2" % (i)

	cur.execute(distancesQ)
	totalDist = list(cur.fetchall())[0][0]

	cur.execute(fuelQ)
	totalFuel= list(cur.fetchall())[0][0]
	print("Less than %s : " %(i))
	print(totalDist/totalFuel)


######################
########################

print("Querying results for problem 3c...")
# Average gallons per date
fuelQ = "SELECT TDAYDATE, AVG(Fuel) FROM(SELECT TDAYDATE, HOUSEID, SUM((TRPMILES/EPATMPG)*30) as Fuel FROM (SELECT HOUSEID, TDAYDATE, TRPMILES, EPATMPG FROM (Trip NATURAL JOIN Vehicle) t0 WHERE VEHID > 1) t1 GROUP BY HOUSEID, TDAYDATE)t2 GROUP BY TDAYDATE ORDER BY TDAYDATE"

cur.execute(fuelQ)

co2 = [float(x[1])*(8.8887*(10**-3))*117538000 for x in list(cur.fetchall())] 

i =0
totalFuel = "SELECT co2 FROM EIACT WHERE MSN = 'TEACEUS' AND (TDAYDATE >= 200803 AND TDAYDATE <= 200904 AND TDAYDATE <> 200813) ORDER BY TDAYDATE "
cur.execute(totalFuel)
totalFuel = cur.fetchall();
for row in totalFuel:
	percFuel = co2[i]/(float(row[0])*(10**6))
	print(percFuel)
	i = i+1

total = [float(x[0])for x in list(totalFuel)] 

######################
######################
print("Querying results for problem 3d...")
# Gather average #s
countQAll = "SELECT TDAYDATE, COUNT(Fuel) FROM(SELECT TDAYDATE, HOUSEID, SUM(gallons*30) as Fuel FROM (SELECT HOUSEID, TDAYDATE, TRPMILES/EPATMPG AS gallons FROM (Trip NATURAL JOIN Vehicle) t0 WHERE VEHID > 1) t1 GROUP BY HOUSEID, TDAYDATE)t2 GROUP BY TDAYDATE ORDER BY TDAYDATE"
cur.execute(countQAll)
count = [x[1] for x in list(cur.fetchall())]

for j in xrange(20,80,20):

	print("With %s Electric" % (j))

	carbonE = "SELECT TDAYDATE, co2 FROM EIACE WHERE MSN = 'TXEIEUS' AND (TDAYDATE >= 200803 AND TDAYDATE <= 200904 AND TDAYDATE <> 200813) "
	mkwhE = "SELECT TDAYDATE, MKWH FROM EIAKWH WHERE MSN = 'ELETPUS' AND (TDAYDATE >= 200803 AND TDAYDATE <= 200904 AND TDAYDATE <> 200813) "
	#C02/kwh
	carbkwh = "SELECT  TDAYDATE ,(co2/MKWH)/(10^6) as co2kwh  FROM ((SELECT TDAYDATE, co2 FROM EIACE WHERE MSN = 'TXEIEUS' AND (TDAYDATE >= 200803 AND TDAYDATE <= 200904 AND TDAYDATE <> 200813))t0 NATURAL JOIN (SELECT TDAYDATE, MKWH FROM EIAKWH WHERE MSN = 'ELETPUS' AND (TDAYDATE >= 200803 AND TDAYDATE <= 200904 AND TDAYDATE <> 200813))t1);"
	cur.execute(carbkwh)
	co2KWH = [x[1] for x in list(cur.fetchall())] #C02/kwh

	# Make sure trip miles is greater than electric ... assuming each trip is on its own day ... SUM fuel per household during month that have greater than 20 miles per trip
	
	fuelQAll = "SELECT TDAYDATE, SUM(Fuel) FROM(SELECT TDAYDATE, HOUSEID, SUM(gallons*30) as Fuel FROM (SELECT HOUSEID, TDAYDATE, TRPMILES/EPATMPG AS gallons FROM (Trip NATURAL JOIN Vehicle) t0 WHERE VEHID > 1 AND TRPMILES > %s) t1 GROUP BY HOUSEID, TDAYDATE)t2 GROUP BY TDAYDATE ORDER BY TDAYDATE" % (j)

	fuelQRemove = "SELECT TDAYDATE, SUM(Fuel) FROM(SELECT TDAYDATE, HOUSEID, SUM(gallons*30) as Fuel FROM (SELECT HOUSEID, TDAYDATE, %s/EPATMPG AS gallons FROM (Trip NATURAL JOIN Vehicle) t0 WHERE VEHID > 1 AND TRPMILES > %s) t1 GROUP BY HOUSEID, TDAYDATE)t2 GROUP BY TDAYDATE ORDER BY TDAYDATE" % (j,j)
	
	cur.execute(fuelQRemove)
	co2ComR = [float(x[1])*(8.8887*(10**-3)) for x in list(cur.fetchall())] 
	
	cur.execute(fuelQAll)
	co2ComAll = [float(x[1])*(8.8887*(10**-3)) for x in list(cur.fetchall())] 

	co2Com =[x-y for x,y in zip(co2ComAll,co2ComR)]



	# for every trip that is greater than electric X, multiply by equivalent constant and MPG : 20 miles /(miles/kwh) =  kwh ... average kwh spent by each household
	ePartQ1 = "SELECT TDAYDATE, SUM(equiv) FROM(SELECT TDAYDATE, SUM(30*%s/(0.090634441*EPATMPG)) AS equiv FROM (SELECT HOUSEID, TDAYDATE, TRPMILES, EPATMPG FROM (Trip NATURAL JOIN Vehicle) t0 WHERE VEHID > 1 AND TRPMILES > %s)t1 GROUP BY HOUSEID, TDAYDATE)t2 GROUP BY TDAYDATE ORDER BY TDAYDATE" % (j,j)
	# All trips less than X ... produce kwh
	ePartQ2 = "SELECT TDAYDATE, SUM(equiv) FROM(SELECT TDAYDATE, SUM(30*TRPMILES/(0.090634441*EPATMPG)) AS equiv FROM (SELECT HOUSEID, TDAYDATE, TRPMILES, EPATMPG FROM (Trip NATURAL JOIN Vehicle) t0 WHERE VEHID > 1 AND TRPMILES < %s)t1 GROUP BY HOUSEID, TDAYDATE)t2 GROUP BY TDAYDATE ORDER BY TDAYDATE" % (j)

	cur.execute(ePartQ1)
	ePart1 = [float(x[1]) for x in list(cur.fetchall())] 

	cur.execute(ePartQ2)
	ePart2 = [float(x[1]) for x in list(cur.fetchall())] 

	kwh = [x+y for x, y in zip(ePart1, ePart2)]


	# Take kwh * C02/kwh = C02 due to electric
	co2Elect = [x*y for x, y in zip(kwh, co2KWH)]


	# Total C02
	# sum(C02Electric + C02Gasoline)
	co2EG =  [x+y for x, y in zip(co2Com, co2Elect)]

	# Average C02 per household for a particular month
	co2Hybrid = [(x/y)*117538000 for x,y in zip(co2EG,count)]


	#print(co2Avg)
	#print(co2)
	# Change from just Fuel
	co2Delta = [(((x*(10**6)-y))/(x*(10**6))) for x, y in zip(total, co2Hybrid)]

	print(co2Delta)



