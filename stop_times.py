import requests
import json


response = requests.get('https://data.edmonton.ca/resource/ctwr-tvrd.json')
y=json.loads(response.content)

file_path = "trips.txt"

with open(file_path, 'w') as file:
    json.dump(y, file, indent=4)

