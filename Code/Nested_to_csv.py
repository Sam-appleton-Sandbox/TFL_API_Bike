import json
import pandas as pd
import numpy as np
import datetime
import arrow
import time





api_path = "/Users/samappleton/Documents/TFL_API_Bike/Code/Outputs/API.csv"
api_vehicle_path ="/Users/samappleton/Documents/TFL_API_Bike/Code/Outputs/API_Vehicle.csv"
api_casualty_path = "/Users/samappleton/Documents/TFL_API_Bike/Code/Outputs/API_Casualty.csv"

timestr = datetime.datetime.now().strftime("%Y-%m-%d")


def new(output_data):
    print("PROCESS 'NESTED_TO_CSV.PY' HAS STARTED RUNNING----------")
    start_time1 = time.time()   


    def write_file(file,output):
        file.to_csv(output)
        print(f'File has been output to {output}')

    # load data using Python JSON module
    
    data = output_data
        
    # Normalizing data
    try:

        api = pd.json_normalize(data,
        meta =[
            'id',
            'lat',
            'lon',
            'location',
            'date',
            'severity',
            'borough'
        ])
        df_vehicle = pd.json_normalize(data,record_path=['vehicles'],
        meta =[
            'id',
        ]
        )
        df_cas = pd.json_normalize(data, record_path =['casualties'],
        meta =[
            'id', ])
    
        # for row in api['date']:
        #     row = datetime.datetime.fromisoformat(row)

        #     new_row = datetime.datetime.strftime(row,"%Y-%m-%d")
        #     date_row = (datetime.datetime.strptime(new_row,"%Y-%m-%d"))
            



        #changes the datetime and stores it in an attribute
        for row in api['date']:
            new_row = arrow.get(row).datetime
        #Maps the above value over the old date time value
        for row in api.index: 
            api.loc[row,'date'] = new_row
      





            
        
        write_file(api,"/Users/samappleton/Documents/TFL_API_Bike/Code/Outputs/API.csv")
        write_file(df_vehicle,"/Users/samappleton/Documents/TFL_API_Bike/Code/Outputs/API_Vehicle.csv")
        write_file(df_cas,"/Users/samappleton/Documents/TFL_API_Bike/Code/Outputs/API_Casualty.csv")


        


        print("Process NESTED_to_CSV finished in --- %s seconds ---" % (time.time() - start_time1))
    except Exception as e:
        print(e)

# if __name__ =="__main__":
#     try: new()

    # except Exception as e:
    #     print(e)

