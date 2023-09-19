#!mamba install pandas==1.3.3 -y
#!mamba install requests==2.26.0 -y

import glob
import pandas as pd
from datetime import datetime

!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_1.json
!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_2.json
!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Final%20Assignment/exchange_rates.csv


#Extract Function

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe
	
columns=['Name','Market Cap (US$ Billion)']
market_cap_dollar_file = 'bank_market_cap_1.json'
exchange_rate_file = 'exchange_rates.csv'
load_to_file = 'market_cap_gbp_file.csv'

def extract(file_name):
    # Write your code here
    extracted_data = extract_from_json(file_name)
    df = pd.DataFrame(extracted_data, columns=columns)
    return df

#Exchange Rates

rates = pd.read_csv(exchange_rate_file, index_col=0)
exchange_rate = rates.at['GBP','Rates']
exchange_rate

# Transform function

def transform(extracted_df, exchange_rate):
    # Write your code here
    transformed_df = extracted_df.rename(columns={'Market Cap (US$ Billion)':'Dollar'})
    transformed_df['Dollar'] = round(transformed_df.Dollar*exchange_rate)
    transformed_df = transformed_df.rename(columns={'Dollar':'Market Cap (GBP£ Billion)'})
    
    return transformed_df
	
#Load

def load(df_to_load, filename):
    # Write your code here
    df_to_load.to_csv(filename)
	
#Logging function

def log(message):
    # Write your code here
    timestamp_format = '%d-%h-%Y-%H:%M:%S'
    time = datetime.now()
    timestamp = time.strftime(timestamp_format)
    with open("logmessages.txt","a") as f:
        f.write(f'{timestamp}, {message}\n')
		
#Run ETL

log("ETL Job Started")
log("Extract phase Started")

#Call the extraction
extracted_data = extract(market_cap_dollar_file)
extracted_data.head()

log("Extract phase Ended")

log("Transform phase Started")

#Call the transformation

transformed_data = transform(extracted_data,exchange_rate)
transformed_data.head()

log("Transform phase Ended")

log("Load phase Started")

#Call the loading

load(transformed_data, load_to_file)

log("Load Phase Ended")
