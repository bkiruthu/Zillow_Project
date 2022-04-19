import pandas as pd
import requests
import json
import time

#
file_dir = 'C:/Users/Benki/OneDrive/Documents/Ben_Academy/Zillow_Project/api_fetch/'
dwnld_dir = 'C:/Users/Benki/OneDrive/Documents/Ben_Academy/Zillow_Project/raw_data/'

# search location
city = 'westerville'
state = 'oh'
search_str = city + ', ' + state

# read in api key file
# df_api_key = pd.read_csv(file_dir + 'api_keys.csv')
# rapid_api_key = df_api_key.iloc[[0], [1]]
# print(rapid_api_key)

# get data

url = 'https://zillow-com1.p.rapidapi.com/propertyExtendedSearch'
querystring = {'location': search_str,
               'home_type': 'Houses',
               'minPrice': '350000',
               'maxPrice': '550000',
               'bedsMin': '3',
               'bathsMin': '2',
               'sqftMin': '1900'

               }
headers = {
    'X-RapidAPI-Host': 'zillow-com1.p.rapidapi.com',
    'X-RapidAPI-Key': 'a57ec0d22bmsh88bfeb4844bfbf5p198dc8jsn98e7860c60e8'
}

zillow_sale_resp = requests.request(
    'GET', url, headers=headers, params=querystring)

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
df_zillow_prop_detail[detail_cols].to_csv(dwnld_dir + 'westerville.csv')
print(df_zillow_prop_detail[detail_cols].head())
