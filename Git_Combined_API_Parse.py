import requests
import csv
import json

headers = {
	'Accept': 'application/json+v6',
	'x-api-key': 'xxx-xxxx-xxx-xxxx-xxx',
}

with open('vrms.csv', newline='') as csvfile:
    vrmreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
    for row in vrmreader:
        vrm = (', '.join(row))

        params = (
                ('registration', vrm),
        )

        response = requests.get('https://beta.check-mot.service.gov.uk/trade/vehicles/mot-tests/', headers=headers, params=params)

        car_string = response.text

        car_dict = json.loads(car_string)

        with open('s872chu.json', 'w') as json_file:
            json.dump(car_dict, json_file, skipkeys = False, sort_keys = False, separators = (',', ':'), indent = 3)

        with open('s872chu.json', 'r') as f: 
            vehicle_dict = json.load(f)
 
        for vehicle in vehicle_dict: 

            vrm = vehicle['registration']
            make = vehicle['make']
            model = vehicle['model']
            colour = vehicle['primaryColour']
            fuel = vehicle['fuelType']
            engsize =  vehicle['engineSize']
            registered = vehicle['registrationDate']

            print(vrm)
            print(make)
            print(model)
            print(colour)
            print(fuel)
            print(engsize)
            print(registered)

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
 
