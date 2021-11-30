# Extracts  MOTs from Endpoint 2 Returns a sql db of MOT Details.
# I Made This
# Import Libraries
from time import sleep
import requests
import json
import sqlite3
from datetime import datetime, timedelta
 
# Initialise Timer
start = datetime.now()
# Count = Start Page
count = 0
# TCount = Last Page 
tcount = 1000
vld = 0
vcnt = 0
ctm = 0
giu = 0
yeti = 0
maxrow = 0
maxcnt = 0
intnow = datetime.now()

# Connect or create a sqlite3 DB
conn = sqlite3.connect('full_extract_mac.db', isolation_level='DEFERRED')

# Sort the tables
c = conn.cursor()
c.execute('''PRAGMA synchronous = OFF''')
c.execute('''PRAGMA journal_mode = OFF''')
c.execute('''PRAGMA auto_vacuum = 1''')


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
TVID INTEGER,
FOREIGN KEY (TVID) REFERENCES vehicles (VID))''')

c.execute('''CREATE TABLE IF NOT EXISTS rfrs
(RID INTEGER PRIMARY KEY,
advisory TEXT,
type TEXT,
danger TEXT(4),
RTID INTEGER,
FOREIGN KEY (RTID) REFERENCES mottests(TID))''')

# Commit to DB
conn.commit()

# Your API Key Goes Here
headers = {
	'Accept': 'application/json+v6',
	'x-api-key': 'API-KEY',
}

while count <= tcount:
    interim = datetime.now()
    page = count

    # Populate the API Parameters
    params = (
        ('page', page),
    )
# Interrogate the API
    try:
        print('Accessing page ', page)
        inttime = datetime.now()
        response = requests.get('https://beta.check-mot.service.gov.uk/trade/vehicles/mot-tests/', headers=headers, params=params)
        intnow = datetime.now()
        intdelta = intnow - inttime
        vcnt = 0
        print('API Access Time ', intdelta)
                
    # Check for tests on a page
        if response.status_code == 404:
            print('*** ERROR ***')
            sleep(0.25)
            valid = 'No'
        
            
        else:  
            car_string = response.text
            valid = 'Yes'
            vehicle_dict = json.loads(car_string)
                      
    # Get the Vehicle Details from the API's JSON
            for vehicle in vehicle_dict: 
                vld = vld + 1
                vcnt = vcnt + 1
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
                try:
                    vuid = (c.fetchone()[0])
                except:
                    vuid=1
    # write vehicle details to db
                steam = [(vid, reg, valid, make, model, colour, fuel, engsize, registered)]
                param = """INSERT INTO vehicles (vehicleid, vrm, valid, make, model, colour, fuel, enginesize, dateregistered) VALUES (?,?,?,?,?,?,?,?,?);"""
                c.executemany(param, steam)
                conn.commit()

    # Echo something to look at
                if make == 'CATERHAM':
                    ctm = ctm + 1
                    print(reg, make, model,' processed.')

                if model == 'GIULIA':
                    giu = giu + 1
                    print(reg, make, model,' processed.')

                if model == 'YETI':
                    yeti = yeti + 1
                    print(reg, make, model,' processed.')
                    
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
                        test_deets = [(testNumber, result, testDate, odovalue, expiry, vid)]
                        
                        #print(vuid, type(vuid))

                        param = """INSERT INTO mottests (testnumber, result, date, mileage, expiry, TVID) VALUES (?,?,?,?,?,?);"""
                        c.executemany(param, test_deets)
                        conn.commit()

    # Extract Reasons for Refusal and Advisories
                        comments = test_data['rfrAndComments']

                        for rfr in comments:
                            text = rfr['text']
                            ftype = rfr['type']
                            dang = rfr['dangerous']

# write RFR details to db
                            rf_deets = [(text, ftype, dang,testNumber)]
                            param = """INSERT INTO rfrs (advisory, type, danger, RTID) VALUES (?,?,?,?);"""
                            c.executemany(param, rf_deets)
                            conn.commit()
                                  
    # If there are no MOT Tests recorded for the vehicle, move along home
                except KeyError:
                    pass
# Bottom of the try page loop
    except:
        print('404 on page :', page)
        
        sleep(1)
        pass

    
    count = count +1
    now = datetime.now()
    latency = now - intnow
    sleep(1)
    print(vcnt,' vehicles processed in ', (latency)) 
    print(vld,' processed so far')
    print(' ')
    print('**** END OF PAGE ****')
    print(' ')
    
# Close DB Connexion
conn.close()

# Report back on how much time it all took, how many VRMS were processed and how long, on average, each one took                  

end = datetime.now()
time_taken = end - start
print('Time: ',time_taken)
print('Valid VRMs: ',vld)
print(ctm,' Caterhams, ',giu,' Giulias & ', yeti,' Yetis' )
print(' ')
print('*** End Of Line ***')
