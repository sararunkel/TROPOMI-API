import requests
import pandas as pd
import shutil
import os
import tempfile
import json
import zipfile

##Copernicus user and password
user='sara.runkel@gwu.edu'
pwrd = ''
##output directory to save the files
output_dir = './test_out/'

from datetime import datetime,timedelta
today = datetime.today()
# Create a timedelta object for one day
one_day = timedelta(days=1)

# Create a list to store dates
date_list = []

# Append today's date to the list
date_list.append(today.strftime('%Y-%m-%d'))

# Loop through the previous 7 days
for i in range(1, 7):
    # Subtract i days from today
    previous_date = today - i * one_day
    # Append the previous date to the list
    date_list.append(previous_date.strftime('%Y-%m-%d'))

data_collection = "SENTINEL-5P"
aoi = "POLYGON((-124.791110603 18.91619,-112.449 43.3577635769,-66.96466 49.4,-76.63 25.02,-124.791110603 18.91619))'" ##simple US polygon

def get_access_token(username: str, password: str) -> str:
    data = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password",
        }
    try:
        r = requests.post("https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
        data=data,
        )
        r.raise_for_status()
    except Exception as e:
        raise Exception(
            f"Access token creation failed. Reponse from the server was: {r.json()}"
            )
    return r.json()["access_token"]


def save_files(result,dayofint):
    for pos in range(len(result)):
        product_identifier = result.iloc[pos,1]
        product_name = result.iloc[pos,2]
        if os.path.exists(output_dir+product_name):
            print("File exists!")
            continue
        # Establish session
        session = requests.Session()
        access_token = get_access_token(user, pwrd)
        headers = {"Authorization": f"Bearer {access_token}"}
        url = f"https://zipper.dataspace.copernicus.eu/odata/v1/Products({product_identifier})/$value"
        response = session.get(url, headers=headers, stream=True)
        # Check for successful response
        if response.status_code == 200:
            print(f"Starting download of {product_name}")
        else:
            print(f"Error: {response.status_code}. Moving to next file.")
            continue
        
        with tempfile.TemporaryDirectory() as temp_dir:
            with tempfile.NamedTemporaryFile(suffix=".zip") as temp_file:
                # Write the downloaded content to the temporary file
                for chunk in response.iter_content(1024):
                    temp_file.write(chunk)
                #Unzip file into temp directory 
                with zipfile.ZipFile(temp_file, 'r') as zip_ref:
                    zip_ref.extractall(path=temp_dir)
            #Walk through directory to find netcdf files
            for root, dirs, files in os.walk(temp_dir):
                for filename in files:
                    if filename.endswith(".nc"):
                        #move the nc to output directory
                        shutil.move(os.path.join(root, filename),output_dir)
            print(f"Downloaded successful!")
    print(f'Extracted all files for {dayofint}')

#start_date = today.strftime('%Y-%m-%d')

for sensing_date in date_list:
    json = requests.get(
        f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=OData.CSC.Intersects(area=geography'SRID=4326;{aoi}) and contains(Name,'S5P_NRTI_L2__NO2') and ContentDate/Start ge {sensing_date}T00:00:00.000Z and ContentDate/Start le {sensing_date}T23:59:00.000Z"
    ).json()
    result = pd.DataFrame.from_dict(json["value"])
    save_files(result, sensing_date)