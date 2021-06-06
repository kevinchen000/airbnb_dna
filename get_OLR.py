
import pandas
import requests
def get_latest_date(old_file_path):
    old_df = pandas.read_csv(old_file_path)
    #need to sort year and month columns individually. Grab the top item
    #no 'by' keyword bc when you grab col it becomes a series not a df
    #old_df = old_df.sort_values(by = ['year'], inplace = True, ascending = False)

    #new method. Assume that input csv is always sorted, which it should if passed directly from API
    df_size = old_df.shape

    latest_year = old_df.at[df_size[0]-1, 'year']
    latest_month = old_df.at[df_size[0]-1,'month']
    return latest_year, latest_month
#Get and read access token. Will need to change to scale
def get_token(tokenpath):
    with open(tokenpath, 'r') as file:
        token = file.read().rstrip('\n')
    if type(token) != str:
        str(token)
    return token

#flatten JSON data in list and convert to DF
def convert_occ_json(json_file_list, old_file_path = ''):

    #grab Json files and flatten data. set errors to ignore to skip over NANs
    for index in range(len(json_file_list)):
        target_df = pandas.json_normalize(json_file_list[index], record_path = [['data','occupancy','calendar_months']],
                                    meta=[['request_info','city_id'],['request_info','room_types'],
                                    ['request_info','bedrooms'], 
                                    ['area_info', 'geom','code','city'],
                                    ['area_info', 'geom','code','state'],
                                    ['area_info', 'geom','code','country']],errors='ignore')

        #if string path is 0, then we're starting a new file
        # if len(old_file_path) ==0:
        #     agg_df.to_csv(f'/Users/Kevin/Desktop/airdna_env/Remaining/_occ.csv')
        # elif len(old_file_path)>0:#if old file exists. concat the new data below the old data
        #     #open old csv
        #     old_df = pandas.read_csv(old_file_path)
        #     #old_df.reset_index(drop=True)
        #     agg_df = pandas.concat([old_df, agg_df], axis = 0, ignore_index = True)
        #     agg_df = agg_df.drop(['Unnamed: 0'], axis=1)
        #     agg_df.to_csv(old_file_path)
        return target_df

def convert_listings_json(json_file_list, old_file_path = ''):

    #grab Json files and flatten data. set errors to ignore to skip over NANs
    for index in range(len(json_file_list)):
        target_df = pandas.json_normalize(json_file_list[index], record_path = [['data','active_listings','calendar_months']],
                                    meta=[['request_info','city_id'],['request_info','room_types'],
                                    ['request_info','bedrooms'],
                                    ['area_info', 'geom','code','city'],
                                    ['area_info', 'geom','code','state'],
                                    ['area_info', 'geom','code','country']],errors='ignore')

    return target_df

def convert_revenue_json(json_file_list, old_file_path = ''):

    #grab Json files and flatten data. set errors to ignore to skip over NANs
    for index in range(len(json_file_list)):
        target_df = pandas.json_normalize(json_file_list[index], record_path = [['data','revenue','calendar_months']],
                                    meta=[['request_info','city_id'],['request_info','room_types'],
                                    ['request_info','bedrooms'],
                                    ['area_info', 'geom','code','city'],
                                    ['area_info', 'geom','code','state'],
                                    ['area_info', 'geom','code','country']],errors='ignore')

    return target_df

#call API to get JSON data.
def get_monthly_occupancy(param_dict_list, headers):
    #URL for call
    endpoint = '/market/occupancy/monthly'
    host = 'https://api.airdna.co/client/v1'
    url = host+endpoint
    
    response = requests.get(url, params = param_dict_list[index], headers = headers).json()
    return response

def get_monthly_listings(param_dict_list, headers):
    #URL for call
    endpoint = '/market/supply/active'
    host = 'https://api.airdna.co/client/v1'
    url = host+endpoint

    response = requests.get(url, params = param_dict_list[index], headers = headers).json()
        
    return response

def get_monthly_revenue(param_dict_list, headers):
    #URL for call
    endpoint = '/market/revenue/monthly'
    host = 'https://api.airdna.co/client/v1'
    url = host+endpoint

    response = requests.get(url, params = param_dict_list[index], headers = headers).json()
   
    return response

#returns list of dicts of all search terms needed 
#length of list is num bed x num cities. Total calls to be made
def create_param_dict(access_token, start_year,start_month,room_types,
                     number_of_months, bedrooms,c_or_r, id):
    idvar = ''
    if c_or_r == 'r':
        idvar = 'region_id'
    elif c_or_r =='c':
        idvar = 'city_id'

    param_dict = {'access_token':access_token, 
                'start_year':start_year,
                'start_month':start_month,
                'room_types':room_types, 
                'bedrooms':bedrooms,
                'number_of_months':number_of_months, #default 1
                idvar : id,
                #'accommodates':'9',
                #'percentiles': '0.75' #Value between 0 and 1. Defaults to .25, .50, .75, .90 }
                }
    return param_dict

def create_update(access_token, city_names, id, c_or_r,bedrooms,
                 room_types, data_name, old_file_path, headers):
    occupancy_json_list = []
    list_json_list = []
    revenue_json_list = []
 
    if new_file:
        #start year
        start_year = '2016'
        #start month
        start_month = '1'
        #how many months of data after start year/month to grab
        number_of_months = '63'

        #create params dict. output is a param dicts 
        paramdict = create_param_dict(access_token, start_year,start_month,room_types,
                            number_of_months, bedrooms, c_or_r, id)

        #get monthly data. returns list of json. size [4bds x #cities]
        occupancy_json_list = get_monthly_occupancy(paramdict, headers)
        listing_json_list = get_monthly_listings(paramdict, headers)
        revenue_json_list = get_monthly_revenue(paramdict, headers)

        #convert monthly data to csv
        occupancy_df = convert_occ_json(occupancy_json_list)
        listing_df = convert_listings_json(listing_json_list)
        revenue_df = convert_revenue_json(revenue_json_list)

    elif not new_file:
        #grabbing the latest dates in file to check for last dates
        date1 = [get_latest_date(old_file_path[0])[0], 
                get_latest_date(old_file_path[0])[1] + 1]
        if date1[1]==13: date1[1] = 1
        date2 = [get_latest_date(old_file_path[1])[0], 
                get_latest_date(old_file_path[1])[1] + 1]
        if date2[1]==13: date2[1] = 1
        date3 = [get_latest_date(old_file_path[2])[0], 
                get_latest_date(old_file_path[2])[1] + 1]
        if date3[1]==13: date3[1] = 1
        
        #if the years and the months match then call the updates
        if date1[0] == date2[0] and date1[0] == date3[0]:
            if date1[1] == date2[1] and date1[1] == date3[1]:
                #start year
                start_year = date1[0]
                #start month
                start_month = date1[1]
                #how many months of data after start year/month to grab
                #1 for now assuming monthly updates
                number_of_months = '1'
                #create params dict. output is a list of param dicts depending on how many bedroom types
                # and cities there are
                paramdict = create_param_dict(access_token, start_year,start_month,room_types,
                                    number_of_months, bedrooms,c_or_r, id)
                #get updated monthly data. returns list of json.
                occupancy_json_list = get_monthly_occupancy(paramdict, headers)
                listing_json_list = get_monthly_listings(paramdict, headers)
                revenue_json_list = get_monthly_revenue(paramdict, headers)

                #convert updated monthly data to csv
                occupancy_df = convert_occ_json(occupancy_json_list, old_file_path[0])
                listing_df = convert_listings_json(listing_json_list, old_file_path[1])
                revenue_df = convert_revenue_json(revenue_json_list, old_file_path[2])
            else:
                print('latest months between files don\'t match')
        else:
            print('latest years between files don\'t match')
    return occupancy_df, listing_df, revenue_df

############################### User Input ##############################################
#get token
tokenpath = '/Users/Kevin/Desktop/airdna_env/token.txt'
access_token = get_token(tokenpath)
#City Name list
city_names = ['Palm Springs','La Quinta','Sevierville','Gatlinburg',
              'Pigeon Forge','Saint Augustine','South Lake Tahoe',
              'Gulf Shores', 'Bradenton Beach', 'Panama City Beach']
#city or region id list. Need to pull separately
id = [58874, 58655, 79356, 59107, 79116, 
      79304, 60327, 56779, 60003,60292] 
#city or region. 'c' or 'r'
c_or_r= 'c'
#list of bedroom #s to grab
bedrooms = [1,2,3,4]
#room_types. (entire_place, private_room, shared_room)
room_types = 'entire_place'
#data names
data_name = 'OLR' 
#True to create new file, false to update old file
new_file = True
#old file path. 
old_file_path = ['/Users/Kevin/Desktop/airdna_env/occ_trial.csv']
#not entirely sure the purpose of this. don't change for now
headers={'Accept': 'application/json'}

#########################################################################################

agg_occ__df = pandas.DataFrame()
agg_list__df = pandas.DataFrame()
agg_rev__df = pandas.DataFrame()
final_df = pandas.DataFrame()

#call each occ, list, rev method for each city for all #bds specified
if len(city_names)==len(id):
    for index in range(len(city_names)):
        for ind in range(len(bedrooms)):
            occ_df, list_df, rev_df = create_update(access_token, city_names[index], id[index], 
                                                    c_or_r,bedrooms[ind],room_types, data_name, 
                                                    old_file_path, headers)
            agg_occ_df = pandas.concat([agg_occ__df, occ_df], axis = 0, ignore_index = True)
            agg_list_df = pandas.concat([agg_list__df, list_df], axis = 0, ignore_index = True)
            agg_rev_df = pandas.concat([agg_rev_df, rev_df], axis = 0, ignore_index = True)
else:
    print('# cities don\'t match # city IDs')   

#check all 3 are same size and join the 3 dfs into 1. Then save to csv
if agg_occ_df.size == agg_list_df.size and agg_occ_df.size == agg_rev_df.size:

    final_df = pandas.concat([agg_occ_df, 
                              agg_list_df['active_listings'], 
                              agg_rev_df[['total_revenue','percentiles.25',
                                         'percentiles.50','percentiles.75',
                                         'percentiles.90']]],
                              axis = 1, ignore_index = True)
    final_df.to_csv('/Users/Kevin/Desktop/airdna_env/OLR_2016-2021_1-4bd.csv')
else:
    print('list ' + str(agg_list_df.size))
    print('occ ' + str(agg_occ_df.size))
    print('rev ' + str(agg_rev_df.size))
    print('rows aren\'t equal')
    