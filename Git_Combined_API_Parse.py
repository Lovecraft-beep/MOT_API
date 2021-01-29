import requests
import csv
import json
from datetime import datetime

start = datetime.now()

headers = {
	'Accept': 'application/json+v6',
	'x-api-key': 'api-key-here',
}

with open('vrms.csv', newline='') as csvfile:
    vrmreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
    for row in vrmreader:
        vrm = (', '.join(row))

        params = (
                ('registration', vrm),
        )

        response = requests.get('https://beta.check-mot.service.gov.uk/trade/vehicles/mot-tests/', headers=headers, params=params)

        if response.status_code == 404:
            
            ouch = "VRM " + vrm +" is not valid"
            print(ouch)
            car_string = json.dumps(ouch)

        else:   
            car_string = response.textcar_string = response.text

            car_dict = json.loads(car_string)

            with open('temp.json', 'w') as json_file:
                json.dump(car_dict, json_file, skipkeys = False, sort_keys = False, separators = (',', ':'), indent = 3)

            with open('temp.json', 'r') as f: 
                vehicle_dict = json.load(f)
     
            for vehicle in vehicle_dict: 

                reg = vehicle['registration']
                make = vehicle['make']
                model = vehicle['model']
                colour = vehicle['primaryColour']
                fuel = vehicle['fuelType']
                
                try:
                    engsize =  vehicle['engineSize']
                except KeyError:
                    engsize = 'Not Available'
                    
                registered = vehicle['registrationDate']

                print(reg)
                print(make)
                print(model)
                print(colour)
                print(fuel)
                print(engsize)
                print(registered)
            
                try:
                    mots = vehicle['motTests']
                
                    for test_data in mots:
                        result = test_data['testResult']
                        testDate = test_data['completedDate']
                        odovalue = test_data['odometerValue']
                        try:
                            expiry = test_data['expiryDate']
                        except:
                            expiry = "N/A"
                            
                        print(testDate, result, odovalue, expiry)

                        comments = test_data['rfrAndComments']

                        for rfr in comments:
                            text = rfr['text']
                            ftype = rfr['type']
                            dang = rfr['dangerous']

                            print(text,ftype,dang)
         
                except KeyError:
                    print('No Test Data Available')
                    
end = datetime.now()
time_taken = end - start
print('Time: ',time_taken)
