
import datetime
from datetime import date
today=date.today() ############# datetime
d1 = today.strftime("%Y-%m-%d") ############# date
d2= today.strftime("%d-%b-%Y")
print(d2)
current_datetime = datetime.datetime.now()

############ tia
import requests
import json
url="https://www.tiairport.com.np/flight_details_2"
r=requests.get(url)
cont=json.loads(r.content.decode())
for item in cont['data']['arrivals']:
    if item['IntDom']=='0':
        list1=[d1,item['Airline'],item['FlightNumber'],item['OrigDest'],'Kathmandu',item['FlightStatus'],item['STASTD_DATE'],item['ETAETD_date']]
        print(list1)
for item in cont['data']['departure']:
    if item['IntDom']=='0':
        list2=[d1,item['Airline'],item['FlightNumber'],'Kathmandu',item['OrigDest'],item['FlightStatus'],item['STASTD_DATE'],item['ETAETD_date']]
        print(list2)
        
        

######### buddha schedule
sess = requests.Session()
home_page = sess.get('https://www.buddhaair.com/soap/FlightAvailability/')
soup = BeautifulSoup(home_page.content, "html.parser")
headers = {'content-type': 'application/json'}
data={
'strSectorFrom': "KTM",
'strSectorTo': "PKR",
'strFlightDate': "8-Mar-2020",
'strReturnDate': "null",
'strNationality': "NP",
'strTripType': "O",
'intAdult': 1,
'intChild': 0
}
requestpost = requests.post("https://www.buddhaair.com/soap/FlightAvailability/", json=data)
response_data = requestpost.json()
print("flightid","flight_date","flight_number","classcode","departure_city","departure_time","arrival_city","arrival_time","sector_pair","air_fare_currency","fare","child_fare","sur_charge","commision_amount","child_commission_amount","tax_name","tax_amount","discount","child_discount","cash_back","child_cash_back","net_fare","free_baggage","refundable","cancellation_charge_before_24_hours","cancellation_charge_after_24_hours")
for item in response_data["data"]["outbound"]["flightsector"]["flightdetail"]:
    #print(item)
    list1=[item["flightid"],item["flightdate"],item["flightno"],item["classcode"],item["departurecity"],item["departuretime"],item["arrivalcity"],item["arrivaltime"],item["sectorpair"],item["airfare"]["faredetail"]["currency"],item["airfare"]["faredetail"]["fare"],item["airfare"]["faredetail"]["childfare"],item["airfare"]["faredetail"]["surcharge"],item["airfare"]["faredetail"]["commissionamt"],item["airfare"]["faredetail"]["childcommissionamt"],item["airfare"]["faredetail"]["taxbreakup"]["taxdetail"]["taxname"],item["airfare"]["faredetail"]["taxbreakup"]["taxdetail"]["taxamount"],item["airfare"]["faredetail"]["discount"],item["airfare"]["faredetail"]["childdiscount"],item["airfare"]["faredetail"]["cashback"],item["airfare"]["faredetail"]["childcassback"],item["airfare"]["faredetail"]["netfare"],item["freebaggage"],item["refundable"],item["cancellationcharge"]["before24hours"],item["cancellationcharge"]["after24hours"]]
    print(list1)
    
    
    
    
