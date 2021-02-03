# Extracts 1 page from Endpoint 2 Returns a sql db of MOT Details.
# I Made This
# Import Libraries
import requests
import csv
import json
import sqlite3
from sqlite3 import Error
from datetime import datetime

# Initialise Timer
start = datetime.now()
count = 0
vld = 0

# Connect or create a sqlite3 DB
conn = sqlite3.connect('mot3.db')

# Sort the tables
c = conn.cursor()

# Create a datbase with 3 tables if they don't exist: Vehicles, MOT Tests and RFRs
c.execute('''CREATE TABLE IF NOT EXISTS vehicles
(VID INTEGER PRIMARY KEY AUTOINCREMENT,
vehicleid TEXT,
vrm TEXT,
valid TEXT,
make TEXT,
model TEXT,
colour TEXT,
fuel TEXT,
enginesize TEXT,
dateregistered TEXT)''')

c.execute('''CREATE TABLE IF NOT EXISTS mottests
(TID INTEGER PRIMARY KEY,
testnumber TEXT,
result TEXT,
date TEXT,
mileage TEXT,
expiry TEXT,
VID INTEGER,
FOREIGN KEY (VID) REFERENCES vehicles (VID))''')

c.execute('''CREATE TABLE IF NOT EXISTS rfrs
(RID INTEGER PRIMARY KEY,
advisory TEXT,
type TEXT,
danger TEXT(4),
TID INTEGER,
FOREIGN KEY (TID) REFERENCES mottests(TID))''')

# Commit to DB
conn.commit()

# Your API Key Goes Here
headers = {
	'Accept': 'application/json+v6',
	'x-api-key': '***** zey belong ere Mozambique ******',
}

while count <= 1:

    page = count
        
# Populate the API Parameters        
    params = (
                ('page', page),
        )

# Interrogate the API
    response = requests.get('https://beta.check-mot.service.gov.uk/trade/vehicles/mot-tests/', headers=headers, params=params)
                    
# Check for valid VRM
    if response.status_code == 404:
            
        valid = 'No'
        ouch = ({'VRM':vrm, 'Valid':valid})
        
    else:   
        car_string = response.text

        valid = 'Yes'

        vehicle_dict = json.loads(car_string)

# Get the Vehicle Details from the API's JSON
        for vehicle in vehicle_dict: 
            vld = vld +1
            reg = vehicle['registration']
            make = vehicle['make']
            model = vehicle['model']
            colour = vehicle['primaryColour']
            fuel = vehicle['fuelType']
            vid = vehicle['vehicleId']
                
# Catch missing Engine Size              
            try:
                engsize =  vehicle['engineSize']
            except KeyError:
                engsize = 'Not Available'

# Catch missing Registration Date
            try:
                registered = vehicle['registrationDate']
            except KeyError:
                registered = 'Not Available'
                
# Get next available key
            vuid = c.execute("""SELECT seq FROM sqlite_sequence WHERE name = 'vehicles'""")
            vuid = (c.fetchone()[0])
                                
# write vehicle details to db
            steam = [(vid, reg, valid, make, model, colour, fuel, engsize, registered)]
            param = """INSERT INTO vehicles (vehicleid, vrm, valid, make, model, colour, fuel, enginesize, dateregistered) VALUES (?,?,?,?,?,?,?,?,?);"""
            c.executemany(param, steam)
            conn.commit()

# Echo something to look at
            print(reg,' processed.')
                
# Extract MOT Test details             
            try:
                mots = vehicle['motTests']

# There could be a lot - Iterate               
                for test_data in mots:
                    testNumber = test_data['motTestNumber']
                    result = test_data['testResult']
                    testDate = test_data['completedDate']
                    odovalue = test_data['odometerValue']
                        
# If it failed, there's no expiry date, we need to catch that
                    try:
                        expiry = test_data['expiryDate']
                    except:
                        expiry = "N/A"
                        
# Write test details to db
                    test_deets = [(testNumber, result, testDate, odovalue, expiry, vuid)]
                    
                    #print(vuid, type(vuid))

                    param = """INSERT INTO mottests (testnumber, result, date, mileage, expiry, VID) VALUES (?,?,?,?,?,?);"""
                    c.executemany(param, test_deets)
                    conn.commit()

# Extract Reasons for Refusal and Advisories
                    comments = test_data['rfrAndComments']

                    for rfr in comments:
                        text = rfr['text']
                        ftype = rfr['type']
                        dang = rfr['dangerous']

# Get current TID

                        tuid = c.execute("""SELECT TID FROM mottests ORDER BY TID DESC LIMIT 1""")
                        tuid = (c.fetchone()[0])
                        
# write RFR details to db
                        rf_deets = [(text, ftype, dang,tuid)]
                        param = """INSERT INTO rfrs (advisory, type, danger, TID) VALUES (?,?,?,?);"""
                        c.executemany(param, rf_deets)
                        conn.commit()
                              
# If there's are no MOT Tests recorded for the vehicle, move along home
            except KeyError:
                pass
            
    count = count +1    
# Report back on how much time it all took, how many VRMS were processed and how long, on average, each one took                  

# Close DB Conexxion
conn.close()
end = datetime.now()
time_taken = end - start
print('Time: ',time_taken)
print('Rows: ',count)
print('Valid VRMs: ',vld)
print('Rate: ',(time_taken/count))
print('End Of Line')
