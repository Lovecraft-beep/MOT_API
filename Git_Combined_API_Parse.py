# Takes a CSV of VRMs, Looks them up on DVSA API, Returns a CSV of MOT Details.
# I Made This
# Import Libraries
import requests
import csv
import json
from datetime import datetime

# Initialise Timer
start = datetime.now()
count = 0

# Your API Key Goes Here
headers = {
	'Accept': 'application/json+v6',
	'x-api-key': 'ihadtotakeitoutordavejohnsonwouldhavekilledme',
}

# Set up output csv
with open('TestOutput.csv', 'w', newline='') as f:
    fieldnames = ['VRM','Valid','Make','Model','Colour','Fuel','Engine Size','Date Registered','MOT Test Number','Test Date','Test Result','Recorded Milage','Expiry Date','Advisory Note','Failure Type','Dangerous (T/F)']
    thewriter = csv.DictWriter(f, fieldnames = fieldnames)
    thewriter.writeheader()

# Open the source CSV file    
with open('vrms.csv', newline='') as csvfile:
    vrmreader = csv.reader(csvfile, delimiter=' ', quotechar='|')

    for row in vrmreader:
        vrm = (', '.join(row))

        count = count + 1
        
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
                        
# write test details to CSV
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
                    
# Report back on how much time it all took, how many VRMS were processed and how long, on average, each one took                  
end = datetime.now()
time_taken = end - start
print('Time: ',time_taken)
print('Rows: ',count)
print('Rate: ',(time_taken/count))
