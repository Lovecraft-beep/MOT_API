# Basic Python Script for TAPI query (Endpoint 1)
import requests
response = requests.get("https://beta.check-mot.gov.uk/trade/vehicles/mot-tests?registration={S872CHU}")
print(response.status_code)
