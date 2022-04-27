import datetime
import logging
import time
import requests
import json
import pytz
import pandas as pd
import azure.functions as func

from azure.storage.blob import ContainerClient
from azure.keyvault.secrets import SecretClient

from azure.identity import ClientSecretCredential
from azure.identity import DefaultAzureCredential

def rapid_api():
    # search location
    city = 'westerville'
    state = 'oh'
    search_str = city + ', ' + state
    
    # path = "https://api.covid19api.com/live/country/usa/status/confirmed/date/"
    url = 'https://zillow-com1.p.rapidapi.com/propertyExtendedSearch'
    querystring = {'location': search_str,
                'home_type': 'Houses',
                'minPrice': '350000',
                'maxPrice': '550000',
                'bedsMin': '3',
                'bathsMin': '2',
                'sqftMin': '1500'

                }
    headers = {
        'X-RapidAPI-Host': 'zillow-com1.p.rapidapi.com',
        'X-RapidAPI-Key': 'a57ec0d22bmsh88bfeb4844bfbf5p198dc8jsn98e7860c60e8'
    }

    zillow_sale_resp = requests.get(
        url, headers=headers, params=querystring)

    # transform to json
    zillow_sale_resp_json = zillow_sale_resp.json()

    # normalize data
    df_zillow_sale = pd.json_normalize(zillow_sale_resp_json['props'])
    num_of_rows = len(df_zillow_sale)
    num_of_columns = len(df_zillow_sale.columns)

    # print('number of rows :', num_of_rows)
    # print('number of columns ', num_of_columns)
    # print(df_zillow_sale.head())

    zpids = df_zillow_sale['zpid'].tolist()

    prop_detail_list = []

    for zpid in zpids:
        querystring = {"zpid": zpid}

        # end point
        url = "https://zillow-com1.p.rapidapi.com/property"

        # header
        headers = {
            "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com",
            "X-RapidAPI-Key": "a57ec0d22bmsh88bfeb4844bfbf5p198dc8jsn98e7860c60e8"
        }

        # get property details
        z_property_dtl = requests.request(
            "GET", url, headers=headers, params=querystring)
        z_property_dtl_json = z_property_dtl.json()

        # wait 10 sec. free edition limitation
        time.sleep(0.5)

        prop_detail_list.append(z_property_dtl_json)

        # print(prop_detail_list)

    # product detail info to data fram
    df_zillow_prop_detail = pd.json_normalize(prop_detail_list)


    # columns of interest
    detail_cols = {
        'streetAddress',
        'city',
        'county',
        'zipcode',
        'state',
        'price',
        'homeType',
        'timeOnZillow',
        'zestimate',
        'rentZestimate',
        'livingArea',
        'bedrooms',
        'bathrooms',
        'yearBuilt',
        'description',
        'priceHistory',
        'taxHistory',
        'zpid'
    }
    # file_path = dwnld_dir + 'westerville.csv'
    rawdata=df_zillow_prop_detail[detail_cols].to_csv()
    return rawdata



def main(mytimer: func.TimerRequest) -> None:
    uutc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s')


def main(mytimer: func.TimerRequest) -> None:

    # Initialize the Credentials.
    default_credential = DefaultAzureCredential()
    
    # Create a Secret Client, so we can grab our Connection String.
    secret_client = SecretClient(
        vault_url='https://bgksolutions-key-vault.vault.azure.net/',
        credential=default_credential
    )

    # Grab the Blob Connection String, from our Azure Key Vault.
    blob_conn_string = secret_client.get_secret(
        name='blob-storage-connection-string'
    )

    # Connect to the Container.
    container_client = ContainerClient.from_connection_string(
        conn_str=blob_conn_string.value,
        container_name='microsoft-azure-articles'
    )

   

    print('connected')
    # Create a dynamic filename.
    filename = "Microsoft RSS Feeds/articles_{ts}.json".format(
        ts=datetime.datetime.now().timestamp()
    )

    # Create a new Blob.
    container_client.upload_blob(
        name=filename,
        data=rapid_api(),
        blob_type="BlockBlob"
    )

    logging.info('File loaded to Azure Successfully...')

    # Grab the UTC Timestamp.
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc
    ).isoformat()

    # Send message if Past Due.
    if mytimer.past_due:
        logging.info('The timer is past due!')

    # Otherwise let the user know it ran.
    logging.info('Python timer trigger function ran at %s', utc_timestamp)

print('raw_data')