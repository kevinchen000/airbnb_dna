import requests
import pandas

def get_token(tokenpath):
    with open(tokenpath, 'r') as file:
        token = file.read().rstrip('\n')
    if type(token) != str:
        str(token)
    return token

def get_ids(paramdict, headers):
    #URL for call
    endpoint = '/market/search'
    host = 'https://api.airdna.co/client/v1'
    url = host+endpoint

    response = requests.get(url, params = paramdict, headers = headers).json()
    return response

def call_ids(name,token):
    id_list = []
    for item in name:
        ####Getting all region IDs######  

        #dict of all search terms needed. terms for states only return major cities. 
        #terms for cities return regions within cities
        paramdict = {'access_token':token, 
                    'term':f'{item}',
                    }
        headers={}
        market_ids_json = get_ids( paramdict, headers)

        #dict to temp hold json entries
        temp_id_dict = {}

        #Loop through JSON. Add new entries to dict
        #052021 you wrote this before finding out about json_normalize. 
        #you don't have to loop through the json if you use that function. cleaner
        for index in range(market_ids_json['num_items']):
            if market_ids_json['items'][index]['type']!='city':
                if market_ids_json['items'][index]['region']['name'] not in temp_id_dict:
                    temp_id_dict[market_ids_json['items'][index]['region']['name']] = {'country':market_ids_json['items'][index]['country']['name'],
                                                            'state': market_ids_json['items'][index]['state']['name'],
                                                            'city': market_ids_json['items'][index]['city']['name'],
                                                            'city_id': market_ids_json['items'][index]['city']['id'],
                                                            'region_id': market_ids_json['items'][index]['region']['id']}
            elif market_ids_json['items'][index]['type']=='city':
                if market_ids_json['items'][index]['city']['name'] not in temp_id_dict:
                    temp_id_dict[market_ids_json['items'][index]['city']['name']] = {'country': market_ids_json['items'][index]['country']['name'],
                                                            'state': market_ids_json['items'][index]['state']['name'],
                                                            'city': market_ids_json['items'][index]['city']['name'],
                                                            'city_id': market_ids_json['items'][index]['city']['id'],
                                                            'region_id': 'none'}

        #add dict entries to df
        ids_df=pandas.DataFrame.from_dict(temp_id_dict).transpose()

        #grabs first entry in city_id column. not using iloc bc noticed 
        #there are differently formatted results depending on city
        id_list.append(ids_df['city_id'][0])

        #this line was used to create a csv for each city in case we wanted to delve into regions per city
        #ids_df.to_csv(f'/Users/Kevin/Desktop/airdna_env/City_IDs/{item}_ID.csv', mode='a', header = 'True')
    return id_list


############################### User Input ##############################################

#List of cities to get IDs. Will print out individual csvs
#for each city
name = ['Scottsdale','Mammoth Lakes','Santa Rosa Beach',
        'Kihei','Fort Walton Beach','Flagstaff','Destin','Lahaina',
        'Charleston','Big Bear Lake','Las Vegas','Miramar Beach',
        'Sedona','Savannah','Yucca Valley','Ridgedale','Fairplay',
        'Orderville','Luray','Morganton','Satellite Beach',
        'Joshua Tree','Mineral Bluff','Broken Bow','Three Rivers','Depoe Bay',
        'Bullhead City',' Sturgeon Bay','Tobyhanna','Nashville','Springdale',
        'Bella Vista','Crystal River', 'Julian','Captiva','Grand Marais','Williamsburg',
        'Munds Park']

save_path = '/Users/Kevin/Desktop/airdna_env/City_IDs/newest_IDs.csv'
#Get and read access token. Will need to change to scale
tokenpath = '/Users/Kevin/Desktop/airdna_env/token.txt'

########################################################################################

#df for holding cities and ids
list_df = pandas.DataFrame(columns = ['Cities','ids'])

#get api token and call function to grab ids for all cities
toke=get_token(tokenpath)
id_list = call_ids(name, toke)

#assign columns to the id df
list_df['Cities'] = name
list_df['ids'] = id_list

#save df to csv
list_df.to_csv(save_path)


