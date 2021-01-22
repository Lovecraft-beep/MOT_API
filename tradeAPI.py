import json
import requests

headers = {
	'Accept': 'application/json+v6',
	'x-api-key': '###',
}

params = (
	('registration', 's872chu'),
)

response = requests.get('https://beta.check-mot.service.gov.uk/trade/vehicles/mot-tests/', headers=headers, params=params)

car_string = response.text

car_dict = json.loads(car_string)

print(json.dumps(car_dict, indent = 4, sort_keys = True))

with open('cat.txt', 'a') as json_file:
	json.dump(car_dict, json_file, indent = 2)
#print(response.json())

#NB. Original query string below. It seems impossible to parse and
#reproduce query strings 100% accurately so the one below is given
#in case the reproduced version is not "correct".
# response = requests.get('http://\https://beta.check-mot.service.gov.uk/trade/vehicles/mot-tests\?registration=S872CHU', headers=headers)

