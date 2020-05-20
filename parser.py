#Data Pipeline
import WorkQueue as wq
import DataWorker as dw
#File Reader Libraries
import json
from xml.etree import ElementTree as ET
import pandas as pd

#Call the workqueue and dataworker classes
WQ = wq.workQueue()
DW = dw.DataWorker(WQ)

#**************************************************************************
#XML PARSER CLASS 
class Broker:
    def __init__(self, code, phone, name):
        self.code = str(code)
        self.phone = phone
        self.name = name


class Agent:
    def __init__(self, code, phone, name):
        self.code = str(code)
        self.phone = phone
        self.name = name

class Listing:

    def __init__(self, xmlItem):
    	#Obtain the name, code, phone attributes in xml, that are in the agent and broker nodes.
        agentName = xmlItem.find('agent').find('name').text
        agentCode = xmlItem.find('agent').find('code').text
        agentPhone = xmlItem.find('agent').find('phone').text
        self.agent = Agent(agentCode, agentPhone, agentName)       
        brokerName = xmlItem.find('broker').find('name').text
        brokerCode = xmlItem.find('broker').find('code').text
        brokerPhone = xmlItem.find('broker').find('phone').text
        self.broker = Broker(brokerCode, brokerPhone, brokerName)

        self.price = xmlItem.find('price').text
        self.description = xmlItem.find('description').text
        self.status = xmlItem.find('status').text

        self.mls_number = xmlItem.find('mls_number').text
        self.type = xmlItem.find('type').text

        # The address column in the database refers to the street attribute in the xml file:
        self.city = xmlItem.find('address').find('city').text
        self.address = xmlItem.find('address').find('street').text
        self.zip = xmlItem.find('address').find('zip').text
        self.state = xmlItem.find('address').find('state').text
        
        #Ensure agent and office id's are strings
        self.agent_id = str(agentCode)
        self.office_id = str(brokerCode)

    #List of dictionaries dependent on their end sqlite table destination ('dest')
    def getAgentAsDict(self):
        return [{'dest': 'agent', 'name': self.agent.name, 'agent_code': self.agent.code, 'phone': self.agent.phone, 'city': self.city, 'state': self.state, 'zip': self.zip}]
    
    def getOfficeAsDict(self):
        return [{'dest': 'offices', 'name': self.broker.name, 'office_code': self.broker.code, 'phone': self.broker.phone, 'city': self.city, 'state': self.state, 'zip': self.zip}]

    def getListingAsDict(self):
        return [{'dest': 'listings', 'address': self.address, 'city': self.city, 'state': self.state, 'zip': self.zip, 'mls_number': self.mls_number, 'price': self.price,
         'status': self.status, 'type': self.type, 'description': self.description, 'agent_id': self.agent_id, 'office_id': self.office_id}]
#***********************************************************************************************************************************************

def json_handler_agents(file, destDB):
	data = []
	df = pd.read_json(file)
	for index, row in df.iterrows(): #include all columns needed for final agents sql table
		#Format string to be passed so it is ready to be converted to list, then dictionary for passing to the sqlite db
		#Only include rows/columns needed for agent table in SQLLite DB
		st = row['agent_name'] + ', '
		st += row['agent_code'] + ', '
		st += row['agent_phone'] + ', '         #This creates essentially a lot of overhead for data, and the string object takes up more memory and more space and memory 
		st += row['city'] + ', '				#Unnecessary memory and space overhead.
		st += row['state'] + ', '
		st += row['zip']
		#Created string into list for easy dictionary creation
		st = st.split(', ')
		#Data that will become our 'records' for database placement
		data.append({'dest': destDB, 'name': st[0], 'agent_code': st[1], 'phone': st[2], 'city': st[3], 'state': st[4], 'zip': st[5]})
	return data

def json_handler_offices(file, destDB):
	data = []
	df = pd.read_json(file)
	#Just in case office name includes LLC we don't split incorrectly
	df['office_name'] = df['office_name'].replace(',', '', regex = True)
	for index, row in df.iterrows():
		st = row['office_name'] + ', '
		st += row['office_code'] + ', '
		st += row['office_phone'] + ', '	#If i just placed it in asa row['office_name'] then I wouldnt need to replace comma
		st += row['city'] + ', '
		st += row['state'] + ', '
		st += row['zip']

		st = st.split(', ')
		data.append({'dest': destDB, 'name': st[0], 'office_code': st[1], 'phone': st[2], 'city': st[3], 'state': st[4], 'zip': st[5]})
	return data

def json_handler_listings(file, destDB):
	data = []
	df = pd.read_json(file)
	for index, row in df.iterrows():  
		#final DB stores all numbers as strings because data is inconsistent
		st = str(row['mls_number']) + ', '
		st += row['street_address'] + ', '
		st += row['city'] + ', '         
		st += row['state'] + ', '
		st += row['zip'] + ', '
		st += str(row['price']) + ', '		#Look up changing dtype in sql
		st += row['status'] + ', '
		st += row['type'] + ', '
		st += row['agent_code'] + ', '
		st += row['office_code'] + ', '
		st += row['description']
		
		st = st.split(', ')
		
		data.append({'dest': destDB, 'address': st[1], 'city': st[2], 'state': st[3], 'zip': st[4], 'mls_number': st[0], 'price': st[5], 'status': st[6], 'type': st[7],
			'description': st[10], 'agent_id': st[8], 'office_id': st[9]})
		
	return data


def csv_handler_agents(file, destDB):
	data = []
	df = pd.read_csv(file)
	for index, row in df.iterrows():    
		
		st = row['NAME'] + ', '
		st += row['AGENT_CODE'] + ', '
		st += row['PHONE'] + ', '        
		st += row['CITY'] + ', '
		st += row['STATE'] + ', '
		st += row['ZIP']
		
		st = st.split(', ')
		
		data.append({'dest': destDB, 'name': st[0], 'agent_code': st[1], 'phone': st[2], 'city': st[3], 'state': st[4], 'zip': st[5]})
		
	return data

def csv_handler_offices(file, destDB):
	data = []
	df = pd.read_csv(file)
	#Remove comma from LLC office name for splitting into list purposes
	df['NAME'] = df['NAME'].replace(',', '', regex = True)
	for index, row in df.iterrows():    
	
		st = row['NAME'] + ', '
		st += row['OFFICE_CODE'] + ', '
		st += row['PHONE'] + ', '        
		st += row['CITY'] + ', '
		st += row['STATE'] + ', '
		st += row['ZIP']
		
		st = st.split(', ')
		
		data.append({'dest': destDB, 'name': st[0], 'office_code': st[1], 'phone': st[2], 'city': st[3], 'state': st[4], 'zip': st[5]})
		
	return data

def csv_handler_listings(file, destDB):
	data = []
	df = pd.read_csv(file)
	for index, row in df.iterrows():   
		
		st = str(row['MLS_NUMBER']) + ', '
		st += row['ADDRESS'] + ', '
		st += row['CITY'] + ', '        
		st += row['STATE'] + ', '
		st += row['ZIP'] + ', '
		st += str(row['PRICE']) + ', '
		st += row['STATUS'] + ', '
		st += row['TYPE'] + ', '
		st += row['AGENT_CODE'] + ', '
		st += row['OFFICE_CODE'] + ', '
		st += row['DESC']
		
		st = st.split(', ')
		
		data.append({'dest': destDB, 'address': st[1], 'city': st[2], 'state': st[3], 'zip': st[4], 'mls_number': st[0], 'price': st[5], 'status': st[6], 'type': st[7],
			'description': st[10], 'agent_id': st[8], 'office_id': st[9]})
		
	return data

#Pass in our data files for parsing and data collection for our WorkQueue and finally load data in with data worker 
#**** AFTER RUNNING THIS FILE 9 (3 FOR EACH FILE TYPE) SEPARATE QUERIES SHOULD HAPPEN THAT CONNECT AND INSERT DATA TO THE HOMES.DB 
#CSV FILES ****************************************************
file = 'agents.csv'
if file == 'agents.csv':
	data = csv_handler_agents(file, "agent")
	for parcel in data:
		WQ.enQ(parcel)		#The main purpose is that you want data to rest in a data buffer because if data is coming in faster than it is being loaded
							#if the data worker goes down, you want your data to be saved in the queue so not data loss (blanace the input and processing speed, protext data from being lost)
	#print(data)
	DW.load_data()

file = 'offices.csv'
if file == 'offices.csv':
	data = csv_handler_offices(file, 'offices')
	for parcel in data:
		WQ.enQ(parcel)
	DW.load_data()

file = 'listings.csv'
if file == 'listings.csv':
	data = csv_handler_listings(file, 'listings')
	for parcel in data:
		WQ.enQ(parcel)
	DW.load_data()

#JSON FILES **************************************************
file = 'feed.json'
data = json_handler_agents(file, 'agent')
for parcel in data:
	WQ.enQ(parcel)
DW.load_data()

file = 'feed.json'
data = json_handler_offices(file, 'offices')
for parcel in data:
	WQ.enQ(parcel)
DW.load_data()

file = 'feed.json'
data = json_handler_listings(file, 'listings')
for parcel in data:
	WQ.enQ(parcel)
DW.load_data()

#XML FILE ****************************************************
file = 'data.xml'
tree = ET.parse(file)
root = tree.getroot()
#Data to keep the lists of listings in
listings = []

#Get all data from xml roots for data entry
for item in root.findall('listing'):
	listings.append(Listing(item))

#Agents table
data = []
for listing in listings:
	data = listing.getAgentAsDict()
	for parcel in data:
		WQ.enQ(parcel)
DW.load_data()

#Offices table
data = []
for listing in listings:
	data = listing.getOfficeAsDict()
	for parcel in data:
		WQ.enQ(parcel)
DW.load_data()

#Listings table
data = []
for listing in listings:
	data = listing.getListingAsDict()
	for parcel in data:
		WQ.enQ(parcel)
DW.load_data()




