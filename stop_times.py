import json

import requests

response = requests.get('https://data.edmonton.ca/resource/ctwr-tvrd.json')
y = json.loads(response.content)

file_path = "data/trips.json"

with open(file_path, 'w') as file:
    json.dump(y, file, indent=4)
