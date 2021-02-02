# Creates random but correctly formatted VRMs, Looks them up on DVSA API, Returns a CSV of MOT Details.
# I Made This
# Import Libraries
import requests
import csv
import json
from datetime import datetime
from random import randrange

# Set up VRM Elements
aa = ['A','B','C','D','E','F','G','H','J','K','L','M','N','O','P','R','S','T','U','V','W','X','Y']
nn = ['51','02','52','03','53','04','54','05','55','06','56','07','57','08','58','09','59','10','60','11','61','12','62','13','63','14','64','15','65','16','66','17','67']

# Initialise Timer
start = datetime.now()
count = 0
vld = 0

# Your API Key Goes Here
headers = {
	'Accept': 'application/json+v6',
	'x-api-key': '***** yours here *****',
}

# Set up output csv
with open('TestOutput.csv', 'w', newline='') as f:
    fieldnames = ['VRM','Valid','Make','Model','Colour','Fuel','Engine Size','Date Registered','MOT Test Number','Test Date','Test Result','Recorded Milage','Expiry Date','Advisory Note','Failure Type','Dangerous (T/F)']
    thewriter = csv.DictWriter(f, fieldnames = fieldnames)
    thewriter.writeheader()

while count <= 4999:
    l1 = randrange(len(aa))
    l2 = randrange(len(aa))
    l3 = randrange(len(nn))
    l4 = randrange(len(aa))
    l5 = randrange(len(aa))
    l6 = randrange(len(aa))

    vrm = (aa[l1]+aa[l2]+nn[l3]+aa[l4]+aa[l5]+aa[l6])
        
# Populate the API Parameters        
    params = (
                ('registration', vrm),
        )

# Interrogate the API
    response = requests.get('https://beta.check-mot.service.gov.uk/trade/vehicles/mot-tests/', headers=headers, params=params)

# Catch non-existant VRM and record to CSV
    if response.status_code == 404:
            
        valid = 'No'
        ouch = ({'VRM':vrm, 'Valid':valid})
            
        with open('TestOutput.csv', 'a', newline='') as f:
                thewriter = csv.DictWriter(f, fieldnames = fieldnames)
                thewriter.writerow(ouch)
                    
# Start Parsing to CSV
    else:   
        car_string = response.text

        valid = 'Yes'

        vehicle_dict = json.loads(car_string)

        vld = vld +1

# Get the Vehicle Details from the API's JSON
        for vehicle in vehicle_dict: 

            reg = vehicle['registration']
            make = vehicle['make']
            model = vehicle['model']
            colour = vehicle['primaryColour']
            fuel = vehicle['fuelType']
                
# Catch missing Engine Size              
            try:
                engsize =  vehicle['engineSize']
            except KeyError:
                engsize = 'Not Available'
                    
            registered = vehicle['registrationDate']

# Make a Vehicle Details Dictionary                
            deets_dict = ({'VRM':reg, 'Valid':valid, 'Make':make, 'Model':model, 'Colour':colour,'Fuel':fuel, 'Engine Size':engsize, 'Date Registered':registered})
                
# write vehicle details to CSV
            with open('TestOutput.csv', 'a', newline='') as f:
                thewriter = csv.DictWriter(f, fieldnames = fieldnames)
                thewriter.writerow(deets_dict)

                
            print(count,' - VRM:',reg, 'Valid:',valid, 'Make:',make, 'Model:', model, 'Colour:', colour,'Fuel:', fuel, 'Engine Size:', engsize, 'Date Registered:', registered)

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

# Make a MOT Test Dictionary                         
                    test_deets = ({'MOT Test Number':testNumber, 'Test Date':testDate, 'Test Result':result, 'Recorded Milage':odovalue, 'Expiry Date':expiry})
                        
# Write test details to CSV
                    with open('TestOutput.csv', 'a', newline='') as f:
                        thewriter = csv.DictWriter(f, fieldnames = fieldnames)
                        thewriter.writerow(test_deets)

# Extract Reasons for Refusal and Advisories
                    comments = test_data['rfrAndComments']

                    for rfr in comments:
                        text = rfr['text']
                        ftype = rfr['type']
                        dang = rfr['dangerous']

# Make an RFR Dictionary
                        rf_deets = ({'Advisory Note':text, 'Failure Type':ftype, 'Dangerous (T/F)':dang})

# write RFR details to CSV
                        with open('TestOutput.csv', 'a', newline='') as f:
                            thewriter = csv.DictWriter(f, fieldnames = fieldnames)
                            thewriter.writerow(rf_deets)
                                
# If there's are no MOT Tests recorded for the vehicle, move along home
            except KeyError:
                pass
    count = count +1    
# Report back on how much time it all took, how many VRMS were processed and how long, on average, each one took                  
end = datetime.now()
time_taken = end - start
print('Time: ',time_taken)
print('Rows: ',count)
print('Valid VRMs: ',vld)
print('Rate: ',(time_taken/count))
