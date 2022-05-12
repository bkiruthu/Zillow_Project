import datetime
import logging
import time
import requests
import os
import json

import snowflake.connector as snowflake

from azure.storage.blob import ContainerClient
from azure.keyvault.secrets import SecretClient

from azure.identity import ClientSecretCredential
from azure.identity import DefaultAzureCredential

#get secrets from azure key vault

    
# Initialize the Credentials.
default_credential = DefaultAzureCredential()

# Create a Secret Client, so we can grab our Connection String.
secret_client = SecretClient(
    vault_url='https://bgksolutions-key-vault.vault.azure.net/',
    credential=default_credential
)


# get snowflake credentials from azure key vault

SFUser = secret_client.get_secret(name='SFUser').value
SFPassword = secret_client.get_secret(name='SFPassword').value

   # Grab the Blob Connection String, from our Azure Key Vault.
blob_conn_string = secret_client.get_secret(name='blob-storage-connection-string')

# Connect to the Container.
container_client = ContainerClient.from_connection_string(
    conn_str=blob_conn_string.value,
    container_name='zillow-columbus'
)
# from azure.storage.blob import BlobServiceClient,generate_blob_sas,BlobSasPermissions


# from datetime import datetime,timedelta
# import snowflake.connector

# def generate_sas_token(file_name):

#     sas = generate_blob_sas(account_name="xxxx",
# account_key="p5V2GELxxxxQ4tVgLdj9inKwwYWlAnYpKtGHAg==", container_name="airflow-dif",blob_name=file_name,permission=BlobSasPermissions(read=True),
# expiry=datetime.utcnow() + timedelta(hours=2))
#     print (sas)
#     return sas

# sas = generate_sas_token("raw-area/moviesDB.csv")

# # Connectio string

# conn = snowflake.connector.connect(user='xx',password='xx@123',account='xx.southeast-asia.azure',database='xx')

# # Create cursor

# cur = conn.cursor()
# cur.execute(
#             f"copy into FIRST_LEVEL.MOVIES FROM  'azure://xxx.blob.core.windows.net/airflow-dif/raw-area/moviesDB.csv'   credentials=(azure_sas_token='{sas}')  file_format = (TYPE = CSV) ;")
# cur.execute(f" Commit  ;")
# # Execute SQL statement
# cur.close()
# conn.close()





# Read Private ssh key from .p8 file
# with open("rsa_key.p8", "rb") as key:
#     p_key= serialization.load_pem_private_key(
#         key.read(),
#         password= None,
#         backend=default_backend()
#     )



# Connect to snowflake using the Provaiet key created above
ctx = snowflake.connect(
    user=SFUser,
    account='il77660.west-us-2.azure',
    password=SFPassword,
    warehouse='etl_dev_wh',
    database='ZILLOWDB',
    schema='RAW'
    )

try:
    # using the snowflake connector object execute a SQL statement against TPCH Demo Database
    resultset = ctx.cursor().execute(
                """copy into North_COLUMBUS_SALES_RAW 
                    FROM (select 
                        $1:zpid,
                        $1:datePosted,
                        $1:zipcode,
                        $1:state, 
                        $1:city,
                        $1:bedrooms,
                        $1:bathrooms,
                        $1:yearBuilt,
                        $1:price,
                        $1:zestimate,
                        $1:livingArea,
                        $1:mortgageRates:thirtyYearFixedRate,
                        'BGK Python ETL Ingestion',
                        current_timestamp()
                        from @my_azure_stage/{dt}/)
                    FILE_FORMAT = (FORMAT_NAME = ZILLOW_FORMAT)
                    PATTERN = '.*[.]json'""".format(
                            dt= datetime.datetime.now().strftime("%Y/%m/%d")
                        )
                    )
    
    df = resultset.fetchall()
    
    for cur in df:
        print(cur)

except Exception as e:
    print(e)