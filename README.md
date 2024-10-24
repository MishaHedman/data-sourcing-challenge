# data-sourcing-challenge
# Dependencies
import requests
import time
from dotenv import load_dotenv
import os
import pandas as pd
import json
import os
from datetime import datetime
## Load the NASA_API_KEY from the env file
load_dotenv()
NASA_API_KEY = os.getenv('NASA_API_KEY')

# Set the base URL to NASA's DONKI API:
base_url = "https://api.nasa.gov/DONKI/"

# Set the specifier for CMEs:
CME = "CME"

# Search for CMEs published between a begin and end date
startDate = "2013-05-01"
endDate   = "2024-05-01"

# Build URL for CME
query_url =(f'{base_url}{CME}?startDate={startDate}&endDate={endDate}&api_key={NASA_API_KEY}')

# Make a "GET" request for the CME URL and store it in a variable named cme_response
cme_response = requests.get(query_url)
#cme_response

# Convert the response variable to json and store it as a variable named cme_json
cme_json = cme_response.json()
cme_json

# Preview the first result in JSON format
# Use json.dumps with argument indent=4 to format data
print(json.dumps(cme_json, indent=4))

# Convert cme_json to a Pandas DataFrame 
cme_df = pd.json_normalize(cme_json)
cme_df

# Keep only the columns: activityID, startTime, linkedEvents
cme_sorted = cme_df[['activityID', 'startTime', 'linkedEvents']]
cme_sorted

# Notice that the linkedEvents column allows us to identify the corresponding GST
# Remove rows with missing 'linkedEvents' since we won't be able to assign these to GSTs
cme_new = cme_sorted[cme_sorted['linkedEvents'].notna()]
#cme_cleaned = cme_sorted.dropna
cme_new.head()

# Initialize an empty list to store the expanded rows
expanded_rows = []

# Notice that the linkedEvents sometimes contains multiple events per row
# Write a nested for loop that iterates first over each row in the cme DataFrame (using the index)
for i in cme_new.index:
    activityID       = cme_new.loc[i, 'activityID']    # Get the corresponding value from 'activityID'
    startTime        = cme_new.loc[i, 'startTime']     # Get the corresponding value from 'startTime'    
    linkedEvents     = cme_new.loc[i, 'linkedEvents']  # Get the list of dictionaries in 'linkedEvents'
    try:
    # Iterate over each dictionary in the list
            # Append a new dictionary to the expanded_rows list for each dictionary item and corresponding 'activityID' and 'startTime' value
        for item in linkedEvents:
            expanded_rows.append({
            'activityID': activityID,
            'startTime': startTime,
            'linkedEvent': item,
})
    except:
        print('missing activity')

# Create a new DataFrame from the expanded rows
expanded_df = pd.DataFrame(expanded_rows)
expanded_df.head()  

# Create a function called extract_activityID_from_dict that takes a dict as input such as in linkedEvents
# and verify below that it works as expected using one row from linkedEvents as an example
# Be sure to use a try and except block to handle errors
def extract_activityID_from_dict(input_dict):
    try:
        activityID = input_dict.get('activityID', None)
        return activityID
    except (ValueError, TypeError) as e:
        # Log the error or print it for debugging
        print(f"Error processing input dictionary: {input_dict}. Error: {e}")
        return None
extract_activityID_from_dict(expanded_df.loc[0, 'linkedEvent'])
#x = expanded_df['linkedEvent'].apply(extract_activityID_from_dict)
#print(x)

# Apply this function to each row in the 'linkedEvents' column (you can use apply() and a lambda function)
# and create a new column called 'GST_ActivityID' using loc indexer:
expanded_df.loc[:, 'GST_ActivityID'] = expanded_df['linkedEvent'].apply(lambda x: extract_activityID_from_dict(x))
 
expanded_df.head()

# Remove rows with missing GST_ActivityID, since we can't assign them to GSTs:
new_df = expanded_df[expanded_df['GST_ActivityID'].notna()]
new_df.head()

# print out the datatype of each column in this DataFrame:
new_df.dtypes
new_df.info()

# Convert the 'GST_ActivityID' column to string format 
new_df['GST_ActivityID'] = new_df['GST_ActivityID'].astype(str)

# Convert startTime to datetime format  
new_df['startTime'] = pd.to_datetime(new_df['startTime'])
#new_df['startTime'] = pd.to_datetime(new_df['startTime'])

# Rename startTime to startTime_CME and activityID to cmeID
new_df.rename(columns = {'startTime': 'startTime_CME', 'activityID' : 'cmeID'}, inplace=True)
# Drop linkedEvents
dropped_df = new_df.drop(columns=['linkedEvent'])
# Verify that all steps were executed correctly
#dropped_df.dtypes
dropped_df.info()

# We are only interested in CMEs related to GSTs so keep only rows where the GST_ActivityID column contains 'GST'
# use the method 'contains()' from the str library.  
new_df = new_df[new_df['GST_ActivityID'].str.contains('GST')]
new_df.head()

# Set the base URL to NASA's DONKI API:
base_url = "https://api.nasa.gov/DONKI/"

# Set the specifier for Geomagnetic Storms (GST):
GST = "GST"

# Search for GSTs between a begin and end date
startDate = "2013-05-01"
endDate   = "2024-05-01"

# Build URL for GST
gst_url =(f'{base_url}{GST}?startDate={startDate}&endDate={endDate}&api_key={NASA_API_KEY}')

# Make a "GET" request for the GST URL and store it in a variable named gst_response
gst_response = requests.get(gst_url)

# Convert the response variable to json and store it as a variable named gst_json
gst_json = gst_response.json()
# Preview the first result in JSON format
# Use json.dumps with argument indent=4 to format data
print(json.dumps(gst_json, indent=4))

# Convert gst_json to a Pandas DataFrame  
df = pd.DataFrame(gst_json)
df.head()
# Keep only the columns: activityID, startTime, linkedEvents
df = df[['gstID', 'startTime', 'linkedEvents']]
df.head()

# Notice that the linkedEvents column allows us to identify the corresponding CME
# Remove rows with missing 'linkedEvents' since we won't be able to assign these to CME
df = df[df['linkedEvents'].notna()]
df.head() 

# Notice that the linkedEvents sometimes contains multiple events per row
# Use the explode method to ensure that each row is one element. Ensure to reset the index and drop missing values.
df_explode = df.explode('linkedEvents', ignore_index=True).dropna()
df_explode.head()

# Apply the extract_activityID_from_dict function to each row in the 'linkedEvents' column (you can use apply() and a lambda function)
# and create a new column called 'CME_ActivityID' using loc indexer:
df_explode.loc[:, 'CME_ActivityID'] = df_explode['linkedEvents'].apply(lambda x: extract_activityID_from_dict(x))

# Remove rows with missing CME_ActivityID, since we can't assign them to CMEs:
#df_explode.dropna(subset=['CME_ActivityID'], inplace=True)
df_explode = df_explode[df_explode['CME_ActivityID'].notna()]
#df_explode = df_explode[df_explode['CME_ActivityID'].str.contains('CME')]
df_explode.head()

df_explode.columns

# Convert the 'CME_ActivityID' column to string format 
df_explode['CME_ActivityID'] = df_explode['CME_ActivityID'].astype(str)

# Convert the 'gstID' column to string format
df_explode['gstID'] = df_explode['gstID'].astype(str)   

# Convert startTime to datetime format
df_explode['startTime'] = pd.to_datetime(df_explode['startTime'])

# Rename startTime to startTime_GST
df_explode.rename(columns = {'startTime': 'startTime_GST'}, inplace=True)

# Drop linkedEvents
df_explode = df_explode.drop(columns=['linkedEvents'])

# Verify that all steps were executed correctly
df_explode.info()

# We are only interested in GSTs related to CMEs so keep only rows where the CME_ActivityID column contains 'CME'
# use the method 'contains()' from the str library.  
df_explode = df_explode[df_explode['CME_ActivityID'].str.contains('CME', na=False)]
df_explode.reset_index(drop=True, inplace=True)
df_explode.head()

# Now merge both datasets using 'gstID' and 'CME_ActivityID' for gst and 'GST_ActivityID' and 'cmeID' for cme. Use the 'left_on' and 'right_on' specifiers.
merged_df = pd.merge(df_explode, dropped_df, left_on=['gstID', 'CME_ActivityID'], 
                     right_on=['GST_ActivityID', 'cmeID'])
merged_df.head()

# Verify that the new DataFrame has the same number of rows as cme and gst
merged_df.info()

# Compute the time diff between startTime_GST and startTime_CME by creating a new column called `timeDiff`.
merged_df['timeDiff'] = merged_df['startTime_GST'] - merged_df['startTime_CME']
merged_df.head()

# Use describe() to compute the mean and median time 
# that it takes for a CME to cause a GST. 
summary = merged_df['timeDiff'].describe()
summary_df = pd.DataFrame(summary)
summary_df  

# Export data to CSV without the index
merged_df.to_csv('merged_df.csv', index=False)
