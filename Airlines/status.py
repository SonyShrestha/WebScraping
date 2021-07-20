from utilities import *
import pandas as pd
import requests
import datetime
import json
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
d2= today.strftime("%d-%b-%Y")
print(d2)

 

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


def tia_status():
    try:
        url="https://www.tiairport.com.np/flight_details_2"
        r=requests.get(url)
        cont=json.loads(r.content.decode())
        for item in cont['data']['arrivals']:
            if item['IntDom']=='0'and item['FlightStatus']=='Cancelled':
                list1=[current_datetime.strftime("%Y-%m-%d %H:%M:%S"),item['Airline'],item['FlightNumber'],item['OrigDest'],'Kathmandu',item['FlightStatus'],item['STASTD_DATE'],item['ETAETD_date']]
                x=tuple(list1)
                conn.execute(f"insert into {temp_db_name}.{tia_status_temp} ( date, airline, flight_number, origin, destination, flight_status, STASTD_Date, ETAETD_Date) values(%s,%s,%s,%s,%s,%s,%s,%s)",x)
        for item in cont['data']['departure']:
            if item['IntDom']=='0'and item['FlightStatus']=='Cancelled':
                list2=[current_datetime.strftime("%Y-%m-%d %H:%M:%S"),item['Airline'],item['FlightNumber'],'Kathmandu',item['OrigDest'],item['FlightStatus'],item['STASTD_DATE'],item['ETAETD_date']]
                x=tuple(list2)
                conn.execute(f"insert into {temp_db_name}.{tia_status_temp} ( date, airline, flight_number, origin, destination, flight_status, STASTD_Date, ETAETD_Date) values(%s,%s,%s,%s,%s,%s,%s,%s)",x)

    except Exception as e:
        raise Exception("Error in buddha airlines status {}".format(str(e)))



def main():
    try:
        logging.info("Running yeti_status function")
        yeti_status()
        logging.info("Running buddha_status function")
        buddha_status()
        logging.info("Running tia_status function")
        tia_status()
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




