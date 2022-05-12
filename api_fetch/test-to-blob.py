import datetime
import logging
import time
import requests
import os
import json
import yaml
import pandas as pd
import azure.functions as func

from azure.storage.blob import ContainerClient
from azure.keyvault.secrets import SecretClient

from azure.identity import ClientSecretCredential
from azure.identity import DefaultAzureCredential

#get secrets from azure key vault
def key_vault():
    
    # Initialize the Credentials.
    default_credential = DefaultAzureCredential()
    
    # Create a Secret Client, so we can grab our Connection String.
    secret_client = SecretClient(
        vault_url='https://bgksolutions-key-vault.vault.azure.net/',
        credential=default_credential
    )
    return secret_client

# get rapid-api-key from azure key vault
def rapid_api_key():
    rapid_api_key = key_vault().get_secret(
        name='zillow-rapid-api-key'
    )
    return rapid_api_key.value

#get zpids to get property details
def rapid_api():
    # search location
    yaml_file =open(os.path.join(os.path.dirname(__file__),'config_files/API_Search_Criteria.yml'))
    yaml_content = yaml.full_load(yaml_file)
    # print(yaml_content)
    # print("Key: Value")
    # for city in yaml_content['City']:
    #loop through all cities in config file 
    zpids=[]
    for city in yaml_content['Cities']:
        search_str = str(city) + ', ' + str(yaml_content['State'])
        # path = "https://api.covid19api.com/live/country/usa/status/confirmed/date/"
        url = 'https://zillow-com1.p.rapidapi.com/propertyExtendedSearch'
        querystring = {'location': search_str,
                    'home_type': str(yaml_content['Home_Type']),
                    'minPrice': str(yaml_content['minPrice']),
                    'maxPrice': str(yaml_content['maxPrice']),
                    'bedsMin': str(yaml_content['bedsMin']),
                    'bathsMin': str(yaml_content['bathsMin']),
                    'sqftMin': str(yaml_content['sqftMin'])

                    }
        headers = {
            'X-RapidAPI-Host': 'zillow-com1.p.rapidapi.com',
            'X-RapidAPI-Key': rapid_api_key()
        }

        zillow_sale_resp = requests.get(
            url, headers=headers, params=querystring)

        # transform to json
        zillow_sale_resp_json = zillow_sale_resp.json()

        # normalize data
        df_zillow_sale = pd.json_normalize(zillow_sale_resp_json['props'])

        #Make a list from all ove the zpids
        zpids_list = df_zillow_sale['zpid'].tolist()
        
        #append to zpids on each iteration
        zpids.append(zpids_list)

        #wait 1 second before next call. API limitation
        time.sleep(2)

    # flatten list of lists
    zpids_flat = sum(zpids,[])
    return zpids_flat

def prop_details():
        
    #loop through each property and get details from api
    prop_detail_list = []

    for zpid in rapid_api():
        querystring = {"zpid": zpid}

        # end point
        url = "https://zillow-com1.p.rapidapi.com/property"

        # header
        headers = {
            "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com",
            "X-RapidAPI-Key":  rapid_api_key()
        }

        # get property details
        z_property_dtl = requests.request(
            "GET", url, headers=headers, params=querystring)
        z_property_dtl_json = z_property_dtl.json()

        # wait 0.5 sec. API provider limitation limitation
        time.sleep(0.5)

        prop_detail_list.append(z_property_dtl_json)

    # prop_detail_flat = sum(prop_detail_list,[])
    json_string = json.dumps(prop_detail_list)
  
    return json_string


# Grab the Blob Connection String, from our Azure Key Vault.
blob_conn_string = key_vault().get_secret(
    name='blob-storage-connection-string'
)
#get config details from yaml file
yaml_file =open(os.path.join(os.path.dirname(__file__),'config_files/API_Search_Criteria.yml'))
yaml_content = yaml.full_load(yaml_file)
# Connect to the Container.
container_client = ContainerClient.from_connection_string(
    conn_str=blob_conn_string.value,
    container_name=str(yaml_content['Container'])
)

# Create a dynamic filename.
filename = str(yaml_content['Profile']) + "_on_sale_{ts}.json".format(
    ts=datetime.datetime.now().timestamp()
)
# Create a new Blob.
container_client.upload_blob(
    name=filename,
    data=prop_details(),
    blob_type="BlockBlob"
)


