from utilities import *
import pandas as pd
import requests
import datetime
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from datetime import date 

logging.info("Creating connection")
db_url_mysqldb = { "drivername":driver_name,
                  "username":user,
                  "password":password,
                  "host": host,
                  "port":"3306"
                 }

conn = create_engine(URL(**db_url_mysqldb))


today=date.today() ############# datetime
d1 = today.strftime("%Y-%m-%d") ############# date
current_datetime = datetime.datetime.now()

logging.info("Truncating temporary schedule tables")
conn.execute(f"truncate table {temp_db_name}.{nac_schedule_temp};")
conn.execute(f"truncate table {temp_db_name}.{simrik_schedule_temp};")
conn.execute(f"truncate table {temp_db_name}.{sita_schedule_temp};")
conn.execute(f"truncate table {temp_db_name}.{tara_schedule_temp};")
conn.execute(f"truncate table {temp_db_name}.{yeti_schedule_temp};")

############## yeti airlines schedule
def yeti_schedule():
    try:
        source_yeti=['KTM','KTM','KTM','KTM','KTM','KTM','KTM','KTM','KTM','KTM','KTM','KTM','BDP','BDP','BDP','BDP','BDP','BDP','BDP','BDP','BDP','BDP','BDP','BDP','BWA','BWA','BWA','BWA','BWA','BWA','BWA','BWA','BWA','BWA','BWA','BWA','BHR','BHR','BHR','BHR','BHR','BHR','BHR','BHR','BHR','BHR','BHR','BHR','BIR','BIR','BIR','BIR','BIR','BIR','BIR','BIR','BIR','BIR','BIR','BIR','JKR','JKR','JKR','JKR','JKR','JKR','JKR','JKR','JKR','JKR','JKR','JKR','JOM','JOM','JOM','JOM','JOM','JOM','JOM','JOM','JOM','JOM','JOM','JOM','LUA','LUA','LUA','LUA','LUA','LUA','LUA','LUA','LUA','LUA','LUA','LUA','KEP','KEP','KEP','KEP','KEP','KEP','KEP','KEP','KEP','KEP','KEP','KEP','PKR','PKR','PKR','PKR','PKR','PKR','PKR','PKR','PKR','PKR','PKR','PKR','TMI','TMI','TMI','TMI','TMI','TMI','TMI','TMI','TMI','TMI','TMI','TMI','MTN','MTN','MTN','MTN','MTN','MTN','MTN','MTN','MTN','MTN','MTN','MTN','RCH','RCH','RCH','RCH','RCH','RCH','RCH','RCH','RCH','RCH','RCH','RCH']
        destination_yeti=['RCH','MTN','TMI','PKR','KEP','LUA','JOM','JKR','BIR','BHR','BWA','BDP','RCH','MTN','TMI','PKR','KEP','LUA','JOM','JKR','BIR','BHR','BWA','KTM','RCH','MTN','TMI','PKR','KEP','LUA','JOM','JKR','BIR','BHR','KTM','BDP','RCH','MTN','TMI','PKR','KEP','LUA','JOM','JKR','BIR','KTM','BWA','BDP','RCH','MTN','TMI','PKR','KEP','LUA','JOM','JKR','KTM','BHR','BWA','BDP','RCH','MTN','TMI','PKR','KEP','LUA','JOM','KTM','BIR','BHR','BWA','BDP','RCH','MTN','TMI','PKR','KEP','LUA','KTM','JKR','BIR','BHR','BWA','BDP','RCH','MTN','TMI','PKR','KEP','KTM','JOM','JKR','BIR','BHR','BWA','BDP','RCH','MTN','TMI','PKR','KTM','LUA','JOM','JKR','BIR','BHR','BWA','BDP','RCH','MTN','TMI','KTM','KEP','LUA','JOM','JKR','BIR','BHR','BWA','BDP','RCH','MTN','KTM','PKR','KEP','LUA','JOM','JKR','BIR','BHR','BWA','BDP','RCH','KTM','TMI','PKR','KEP','LUA','JOM','JKR','BIR','BHR','BWA','BDP','KTM','MTN','TMI','PKR','KEP','LUA','JOM','JKR','BIR','BHR','BWA','BDP']
        for yeti_source,yeti_destination in zip(source_yeti,destination_yeti):
            url="https://www.yetiairlines.com/flight-schedule?date="+d1+"&from="+yeti_source+"&to="+yeti_destination
            r=requests.get(url)
            soup=BeautifulSoup(r.content,'html5lib')
            x=soup.find("table")
            m2=[[x for x in row.find_all('td')] for row in x.find_all('tr')]
            n2 = [x for x in m2 if x]
            for o in n2:
                cols=[ele.text.strip() for ele in o[2:]]
                cols.insert(0,current_datetime.strftime("%Y-%m-%d %H:%M:%S"))
                cols.insert(1,'YETI AIRLINES')
                x=tuple(cols)
                if '-' not in x:
                    conn.execute(f"insert into {temp_db_name}.{yeti_schedule_temp}(date, airline, flight_number, sector, departure_time, arrival_time) values(%s,%s,%s,%s,%s,%s)", x)
    except Exception as e:
        raise Exception("Error in yeti airlines schedule {}".format(str(e)))
		


################## tara airlines schedule
def tara_schedule():
    try:
        url2="http://www.taraair.com/page/flight-schedule"
        r=requests.get(url2, headers={"User-Agent": "XY"})
        soup=BeautifulSoup(r.content,'html5lib')
        a1=soup.find_all("table")
        a2=soup.find_all("tbody")
        m=[[x for x in row.findAll('td')] for row in soup.findAll('tr')]
        for j in m[1:]:
            cols2=[ele.text.strip() for ele in j]
            cols2.insert(0,current_datetime.strftime("%Y-%m-%d %H:%M:%S"))
            cols2.insert(1,'TARA AIRLINES')
            x=tuple(cols2)
            if len(x)==8:
                conn.execute(f"insert into {temp_db_name}.{tara_schedule_temp}(date, airline, origin, destination, flight_number, days_of_operation, departure_time, arrival_time) values(%s,%s,%s,%s,%s,%s,%s,%s)", x)

    except Exception as e:
        raise Exception("Error in tara airlines schedule {}".format(str(e)))
		



######## simrik airlines schedule
def simrik_schedule():
    try:
        url2="https://simrikairlines.com/flight-schedule/"
        r=requests.get(url2)
        soup=BeautifulSoup(r.content,'html5lib')
        m=[[x for x in row.findAll('td')] for row in soup.findAll('tr')]
        n = [x for x in m if x]
        for o in n:
            cols=[ele.text.strip() for ele in o]
            cols.insert(0,current_datetime.strftime("%Y-%m-%d %H:%M:%S"))
            cols.insert(1,'SIMRIK AIRLINES')
            x=tuple(cols)
            conn.execute(f"insert into {temp_db_name}.{simrik_schedule_temp}(date, airline, sector, flight_number, days_of_operation, departure_time, arrival_time) values(%s,%s,%s,%s,%s,%s,%s)", x)
    except Exception as e:
        raise Exception("Error in simrik airlines schedule {}".format(str(e)))



############### sita airlines schedule
def sita_schedule():
    try:
        url="https://www.sitaair.com.np/flight-schedule/"
        r=requests.get(url)
        soup=BeautifulSoup(r.content,'html5lib')
        x=soup.find("table",attrs={'class':'table table-striped'})
        m2=[[x for x in row.findAll('td')] for row in x.findAll('tr')]
        n2 = [x for x in m2 if x]
        for o in n2:
            cols=[ele.text.strip() for ele in o]
            cols.insert(0,current_datetime.strftime("%Y-%m-%d %H:%M:%S"))
            cols.insert(1,'SITA AIRLINES')
            x=tuple(cols)
            conn.execute(f"insert into {temp_db_name}.{sita_schedule_temp}(date, airline, sector, flight_number, days_of_operation, departure_time, arrival_time) values(%s,%s,%s,%s,%s,%s,%s)", x)
    except Exception as e:
        raise Exception("Error in sita airlines schedule {}".format(str(e)))
 



############# nac airlines schedule
def nac_schedule():
    try:
        url="https://www.nepalairlines.com.np/home/schedule/domestic"
        r=requests.get(url)
        soup=BeautifulSoup(r.content,'html5lib')
        x=soup.find("table")
        m2=[[x for x in row.find_all('td',attrs={'style':'text-align:center'})] for row in x.findAll('tr')]
        n2 = [x for x in m2 if x]
        for o in n2:
            cols=[ele.text.strip() for ele in o]
            cols.insert(0,current_datetime.strftime("%Y-%m-%d %H:%M:%S"))
            cols.insert(1,'NAC AIRLINES')
            x=tuple(cols)
            if len(x)==6 and 'SECTOR' not in x:
                conn.execute(f"insert into {temp_db_name}.{nac_schedule_temp}(date, airline, flight_number, sector, days_of_operation, departure_arrival_time) values(%s,%s,%s,%s,%s,%s)", x)
    except Exception as e:
        raise Exception("Error in NAC airline schedule {}".format(str(e)))



################## yeti status
def yeti_status():
    try:
        url2="https://yetiairlines.com/flight-status"
        r=requests.get(url2)
        soup=BeautifulSoup(r.content,'html5lib')
        a1=soup.find_all("table",attrs={'id':'flightInfo'})
        for i in a1:
            a2=soup.find_all("tbody")
            m=[[x for x in row.findAll('td')] for row in soup.findAll('tr')]
            n2 = [x for x in m if x]
            for j in n2:
                cols=[ele.text.strip() for ele in j]
                cols2=cols[1:]
                cols2.insert(0,current_datetime.strftime("%Y-%m-%d %H:%M:%S"))
                cols2.insert(1,'YETI AIRLINES')
                x=tuple(cols2)
                if any("can" in s.lower() for s in x):
                    conn.execute(f"insert into {temp_db_name}.{yeti_status_temp}(date, airline, origin, destination, flight_number, status, ETD, revised_time) values(%s,%s,%s,%s,%s,%s,%s,%s)", x)

    except Exception as e:
        raise Exception("Error in yeti airlines status {}".format(str(e)))
        
        
############### buddha status
def buddha_status():
    try:
        source_buddha=['KTM','KTM','KTM','KTM','KTM','KTM','KTM','KTM','KTM','KTM','KTM','KTM','KTM','BDP','BDP','BDP','BDP','BDP','BDP','BDP','BDP','BDP','BDP','BDP','BDP','BDP','BWA','BWA','BWA','BWA','BWA','BWA','BWA','BWA','BWA','BWA','BWA','BWA','BWA','BHR','BHR','BHR','BHR','BHR','BHR','BHR','BHR','BHR','BHR','BHR','BHR','BHR','BIR','BIR','BIR','BIR','BIR','BIR','BIR','BIR','BIR','BIR','BIR','BIR','BIR','DHI','DHI','DHI','DHI','DHI','DHI','DHI','DHI','DHI','DHI','DHI','DHI','DHI','JKR','JKR','JKR','JKR','JKR','JKR','JKR','JKR','JKR','JKR','JKR','JKR','JKR','KEP','KEP','KEP','KEP','KEP','KEP','KEP','KEP','KEP','KEP','KEP','KEP','KEP','PKR','PKR','PKR','PKR','PKR','PKR','PKR','PKR','PKR','PKR','PKR','PKR','PKR','RJB','RJB','RJB','RJB','RJB','RJB','RJB','RJB','RJB','RJB','RJB','RJB','RJB','SIF','SIF','SIF','SIF','SIF','SIF','SIF','SIF','SIF','SIF','SIF','SIF','SIF','SKH','SKH','SKH','SKH','SKH','SKH','SKH','SKH','SKH','SKH','SKH','SKH','SKH','TMI','TMI','TMI','TMI','TMI','TMI','TMI','TMI','TMI','TMI','TMI','TMI','TMI','VNS','VNS','VNS','VNS','VNS','VNS','VNS','VNS','VNS','VNS','VNS','VNS','VNS','PKR','KTM']
        destination_buddha=['BDP','BWA','BHR','BIR','DHI','JKR','KEP','PKR','RJB','SIF','SKH','TMI','VNS','KTM','BWA','BHR','BIR','DHI','JKR','KEP','PKR','RJB','SIF','SKH','TMI','VNS','BDP','KTM','BHR','BIR','DHI','JKR','KEP','PKR','RJB','SIF','SKH','TMI','VNS','BDP','BWA','KTM','BIR','DHI','JKR','KEP','PKR','RJB','SIF','SKH','TMI','VNS','BDP','BWA','BHR','KTM','DHI','JKR','KEP','PKR','RJB','SIF','SKH','TMI','VNS','BDP','BWA','BHR','BIR','KTM','JKR','KEP','PKR','RJB','SIF','SKH','TMI','VNS','BDP','BWA','BHR','BIR','DHI','KTM','KEP','PKR','RJB','SIF','SKH','TMI','VNS','BDP','BWA','BHR','BIR','DHI','JKR','KTM','PKR','RJB','SIF','SKH','TMI','VNS','BDP','BWA','BHR','BIR','DHI','JKR','KEP','KTM','RJB','SIF','SKH','TMI','VNS','BDP','BWA','BHR','BIR','DHI','JKR','KEP','PKR','KTM','SIF','SKH','TMI','VNS','BDP','BWA','BHR','BIR','DHI','JKR','KEP','PKR','RJB','KTM','SKH','TMI','VNS','BDP','BWA','BHR','BIR','DHI','JKR','KEP','PKR','RJB','SIF','KTM','TMI','VNS','BDP','BWA','BHR','BIR','DHI','JKR','KEP','PKR','RJB','SIF','SKH','KTM','VNS','BDP','BWA','BHR','BIR','DHI','JKR','KEP','PKR','RJB','SIF','SKH','TMI','KTM','MTN','MTN']
        for buddha_source,buddha_destination in zip(source_buddha,destination_buddha):
            print(buddha_source,buddha_destination)
            url2="https://admin.buddhaair.com/api/flight-status/"+buddha_source+"/"+buddha_destination
            r=requests.get(url2)
            soup=BeautifulSoup(r.content,'html5lib')
            a2=soup.find_all("flight")
            for j in a2:
                flight_no=j.find("flightno")
                departure=j.find("departure")
                arrival=j.find("arrival")
                flighttime=j.find("flighttime")
                revisedtime=j.find("revisedtime")
                flightstatus=j.find("flightstatus")
                flightremarks=j.find("flightremarks")
                cols=[current_datetime.strftime("%Y-%m-%d %H:%M:%S"),'BUDHHA AIRLINES',flight_no.text,departure.text,arrival.text,flighttime.text,revisedtime.text,flightstatus.text,flightremarks.text]
                x=tuple(cols)
                if any("can" in s.lower() for s in x):  
                    conn.execute(f"insert into {temp_db_name}.{buddha_status_temp}( date, airline, flight_number, origin, destination, flight_time,revised_time, flight_status, flight_remarks) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)", x)
    except Exception as e:
        raise Exception("Error in buddha airlines status {}".format(str(e)))



def main():
    try:
        logging.info("Running yeti_schedule function")
        yeti_schedule()
        logging.info("Running tara_schedule function")
        tara_schedule()
        logging.info("Running simrik_schedule function")
        simrik_schedule()  
        logging.info("Running sita_schedule function")
        sita_schedule() 
        logging.info("Running nac_schedule function")
        nac_schedule() 
        logging.info("Running yeti_status function")
        yeti_status()
        logging.info("Running buddha_status function")
        buddha_status()
        logging.info("Truncating main status table")
        trunc=f"""truncate table {db_name}.{status}"""
        conn.execute(trunc)
        logging.info("Inserting data in main status table")
        stmt=f"""insert into {db_name}.{status} SELECT date, airline, flight_number, origin, destination,replace(flight_status,"  "," ") as flight_status, flight_time as ETD, revised_time, flight_remarks FROM airlines_schedule_status.buddha_status  union SELECT date, airline, flight_number,origin, destination,  replace(status,'  ',' ') as flight_status, ETD, revised_time,' ' FROM airlines_schedule_status.yeti_status"""
        conn.execute(stmt)
    except Exception as e:
        logging.error("Error : {}".format(str(e)))

if __name__ == "__main__":
    main()




