import requests
import jsonschema

r = requests.get("https://raw.githubusercontent.com/SuLab/outbreak.info-resources/master/yaml/outbreak.json")
print("r")

class OutbreakParser:
    schema = r.json()

    def __init__(self):
        self.required_fields = ["_id", "@type", "name", "url", "curatedBy"]

    def validate(self, data):
        self.result = jsonschema.validate(data, self.schema)
