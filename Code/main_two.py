
import requests
import ssl
import json
import datetime
import time
import data_import
import Nested_to_csv
from datetime import datetime

#Some text
#Sets SSL certificate to false.  
ssl._create_default_https_context = ssl._create_unverified_context
started = datetime.now().strftime("%H:%M:%S")
print(f'Process started at {started}')
#Function that calls the API and returns the response
def get_time():

    get_time.start_time = time.time()
    
    
def get_year():
    year = input("please enter the year you wish to gather data for: ")
    get_time()
    return year

def api_call():
    url = f"https://api.tfl.gov.uk/AccidentStats/{get_year()}"
    
    #Sends get request to URL
    response = requests.get(url, headers={'Cache-Control': 'no-cache'})
    #parses into a readable json dictionary
    api_response = json.loads(response.text)
    return api_response
# some text
# def response_writer():
#     api_response = api_call()
#     timestr = datetime.datetime.now().strftime("%Y-%m-%d")
#     filename = f'/Users/samappleton/Documents/Python/Project/Outputs/api_response_{timestr}.json'
#     with open(filename, 'w', encoding='utf-8') as output_data:
#         json.dump(api_response, output_data)
#         print("--------------------------------------------\n", "API RESPONSE SAVED:", filename, "\n--------------------------------------------")

if __name__ =="__main__":
    try:
        
        Nested_to_csv.new(api_call())
        # data_import.main()

    except Exception as e:
        print(e)


print("Process finished --- %s seconds ---" % (time.time() - get_time.start_time))
    