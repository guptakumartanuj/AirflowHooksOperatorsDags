# importing the requests library
import requests
import json 


task=json.dumps({"sql":"SELECT a.shape_name, a.num_sides, b.color_name, b.red_value, b.green_value, b.blue_value FROM shapes_production ;"})

header = {"Content-Type" : "application/json", "Authorization" : "token"}

resp = requests.post('http://prod.xyz.com:1234/query/12345@XyOrg/test',
                     data=task,
                     headers=header)
                     
# extracting response text 
Final_Response = resp.text
print("The pastebin URL is:%s"%Final_Response)                     