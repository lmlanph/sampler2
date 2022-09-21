import requests
import xml.etree.ElementTree as ET
import json
import re
import csv
import time
from datetime import timedelta, datetime
import psycopg2
import psycopg2.extras
import os
import random
import boto3

'''

TO DO:

test when get_memberships() fails--current try/except doesn't cover--expand? okay if membership(s) missing for tup_list build?

*** can't rely on ID's to be unique if using one checkins table for all MMS--make sure that any updates to a checkin are limited to the unit/company

if no mobile, check home/work;

'''


def get_param(param):  # get param store
    ssm_client = boto3.client("ssm", region_name="us-east-1")
    get_response = ssm_client.get_parameter(Name=param, WithDecryption=True)
    return get_response['Parameter']['Value']


# param store vs env vars
Prod = True

if Prod:
    PSQL_PASS = get_param('PSQL_PASS')
    MB_API_KEY = get_param('MB_API_KEY')
    MB_SRC_USERNAME = get_param('MB_SRC_USERNAME')
    MB_SRC_PW = get_param('MB_SRC_PW')
else:
    PSQL_PASS = os.environ.get('PSQL_PASS')
    MB_API_KEY = os.environ.get('MB_API_KEY')
    MB_SRC_USERNAME = os.environ.get('MB_SRC_USERNAME')
    MB_SRC_PW = os.environ.get('MB_SRC_PW')


# True will prevent writing records to DB--or is this better controlled by checkins.py now...?
test_mode = False


CHUNK_SIZE = 19


def get_token(site_id):

    headers = {
        'Content-Type': "application/json",
        'Api-Key': MB_API_KEY,
        'SiteId': site_id
    }

    payload = '{\r\n\t\"Username\": \"' + MB_SRC_USERNAME + '\", \r\n\t\"Password\": \"' + MB_SRC_PW + '\"\r\n}'

    response = requests.post('https://api.mindbodyonline.com/public/v6/usertoken/issue', data=payload, headers=headers)

    data = response.content

    json_data = json.loads(data.decode('utf-8'))

    return json_data['AccessToken']


# bulk call gets data for all units under a site ID
def MB_bulk_call(date, site_id):

    url = 'https://api.mindbodyonline.com/0_5/DataService.asmx'

    start_date = date
    end_date = date
    mod_date = '01/01/1999'

    headers = {'SOAPAction': 'http://clients.mindbodyonline.com/api/0_5/FunctionDataXml',
               'API-key': MB_API_KEY,
               'SiteID': site_id,
               'content-type': 'text/xml'
               }

    data = f'''<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
       <soap:Body>
          <FunctionDataXml xmlns="http://clients.mindbodyonline.com/api/0_5">
             <Request>
                <XMLDetail>Full</XMLDetail>
                <PageSize>5</PageSize>
                <CurrentPageIndex>0</CurrentPageIndex>
                <FunctionName>UtilityFunction_VisitsV4</FunctionName>
                <FunctionParams>
                   <FunctionParam>
                      <ParamName>@StartDate</ParamName>
                      <ParamValue>{start_date}</ParamValue>
                      <ParamDataType>datetime</ParamDataType>
                   </FunctionParam>
                   <FunctionParam>
                      <ParamName>@EndDate</ParamName>
                      <ParamValue>{end_date}</ParamValue>
                      <ParamDataType>datetime</ParamDataType>
                   </FunctionParam>
                   <FunctionParam>
                      <ParamName>@ModifiedDate</ParamName>
                      <ParamValue>{mod_date}</ParamValue>
                      <ParamDataType>datetime</ParamDataType>
                   </FunctionParam>
                </FunctionParams>
             </Request>
          </FunctionDataXml>
       </soap:Body>
    </soap:Envelope>'''

    response = requests.post(url, data=data, headers=headers)
    # print(response.text)

    root = ET.fromstring(response.content)

    data = []
    visit = {}
    odd_row = True

    # parse bulk visits call XML
    for i in root.findall(".//"):
        # print(type(i.tag)) - print(i.tag) - print(type(i.text)) - print(i.text)

        try:

            res = re.findall(r'}(\b\w*)', i.tag)
            # print(res[0])

            if 'Row' in i.tag and 'VisitRow' not in i.tag and odd_row:
                odd_row = True
                continue

            elif 'Row' in i.tag and 'VisitRow' not in i.tag and not odd_row:
                data.append(visit)
                visit = {}
                # odd_row = False

            else:
                visit[res[0]] = i.text
                odd_row = False

        except Exception as e:
            print(e)

    # remove first item, metadata not needed
    data = data[1:]

    # sort list by ID
    sorted_data_bulk_call = sorted(data, key=lambda d: d['ID'])

    # create new list without duplicates (by ID)
    sorted_data_bulk_call_unique = []
    # add to new list
    last = '0000'
    for index, record in enumerate(sorted_data_bulk_call):
        if record['ID'] != last:
            sorted_data_bulk_call_unique.append(record)
        last = record['ID']

    print(f'len sorted_data_bulk_call: {len(sorted_data_bulk_call)}')
    print(f'len sorted_data_bulk_call_unique: {len(sorted_data_bulk_call_unique)}')
    # print(sorted_data_bulk_call_unique)

    ### RETURNS LIST of DICT, SORTED BY ID, WITH NO DUPS ###
    return sorted_data_bulk_call_unique


def get_membership_types(headers):

    try:
        print('getting membership types...')

        response = requests.get(f'https://api.mindbodyonline.com/public/v6/site/memberships', headers=headers)

        membership_data = json.loads(response.text)

        membership_lookup = {}

        # put in dict, assumes unique membership id's...
        for i in membership_data['Memberships']:
            membership_lookup[i['MembershipId']] = i['MembershipName']

    except Exception as e:
        print(f'get membership types error: {e}')

    return membership_lookup


def get_memberships(headers, ids, num_chunks):

    types = get_membership_types(headers)
    # print(f'membership types: {types}')

    all_memberships = {}

    for i in range(num_chunks):
        print('chunking membership types from client/activeclientsmemberships')

        ids_range_start = i * CHUNK_SIZE
        ids_range_stop = ids_range_start + CHUNK_SIZE

        client_string = 'ClientIds='

        # build param string
        for id in ids[ids_range_start:ids_range_stop]:
            client_string += (id + '&ClientIds=')

        response = requests.get(f'https://api.mindbodyonline.com/public/v6/client/activeclientsmemberships?' + client_string, headers=headers)

        data = json.loads(response.text)

        try:
            for i in data['ClientMemberships']:

                all_memberships[i['ClientId']] = types[i['Memberships'][0]['MembershipId']]

        except Exception as e:
            print(e)

    # print(all_memberships)
    return all_memberships


def chunk(headers, ids, num_chunks, bulk_call, id_map):

    main_index = 0

    ###
    # run get memberships function (returns a dict lookup by member ID)
    memberships = get_memberships(headers, ids, num_chunks)
    ###

    tup_list = []

    # for each group of 20, grab chunk of id's and make call to client/clients
    # okay to start at 0 index here, as we're looking at a list of ids only, not the response
    for i in range(num_chunks):

        try:

            ids_range_start = i * CHUNK_SIZE
            ids_range_stop = ids_range_start + CHUNK_SIZE

            print(f'range_start: {ids_range_start}')
            print(f'range_stop: {ids_range_stop}')

            client_string = 'ClientIds='

            for id in ids[ids_range_start:ids_range_stop]:
                client_string += (id + '&ClientIds=')

            response = requests.get(f'https://api.mindbodyonline.com/public/v6/client/clients?' + client_string, headers=headers)

            # 20 Clients
            data_client_clients = json.loads(response.text)
            # print(data_client_clients)
            client_list = data_client_clients['Clients']
            # print(len(client_list))

            # list of dict
            sorted_client_list = sorted(client_list, key=lambda d: d['Id'])

            # pretty = json.dumps(sorted_client_list, indent=4)
            # print(pretty)

        except Exception as e:
            print(f'for i in range num chunks error: {e}')

        for i in range(CHUNK_SIZE):

            try:

                print(f'building tup_list item {main_index}')
                print(f'i is {i}')

                # birthdate, source example
                # from '1987-09-14T00:00:00'
                birth_date_dt = datetime.strptime(sorted_client_list[i]['BirthDate'], '%Y-%m-%dT%H:%M:%S')
                birth_date_string = datetime.strftime(birth_date_dt, '%Y-%m-%d %H:%M:%S-07')

                # taking the hour/minutes from startTime, but the date from VisitDate
                # this is how it's set up in W, confirm with other companies it's the same
                visit_date_dt = datetime.strptime(bulk_call[main_index]['VisitDate'], '%m/%d/%Y %I:%M:%S %p')
                checkin_time_dt = datetime.strptime(bulk_call[main_index]['StartTime'], '%m/%d/%Y %I:%M:%S %p')
                visit_date_string = datetime.strftime(visit_date_dt, '%Y-%m-%d')
                checkin_time_str = datetime.strftime(checkin_time_dt, ' %H:%M:%S-07')
                visit_datetime_string = visit_date_string + checkin_time_str

                m_id = id_map[bulk_call[main_index]['LocationID']]

                tup = (
                    bulk_call[main_index]['LastName'],
                    bulk_call[main_index]['FirstName'],
                    birth_date_string,
                    sorted_client_list[i]['Gender'],
                    bulk_call[main_index]['ID'],
                    sorted_client_list[i]['MobilePhone'],
                    bulk_call[main_index]['EmailName'],
                    sorted_client_list[i]['Status'],
                    m_id,
                    '1800-01-01 00:00:00',
                    visit_datetime_string,
                    memberships[bulk_call[main_index]['ID']]
                )

                tup_list.append(tup)

            # added this because of partial last chunks, cause list index out of range error...
            # figure out a better solution?
            except Exception as e:
                print(f'for i in range chunk size (inner loop) error: {e}')

            finally:
                main_index += 1

    print(f'tup_list for all: {tup_list}')

    return tup_list


def get_checkins_join():

    connect = psycopg2.connect(
        database='sampler',
        user='postgres',
        password=PSQL_PASS
    )
    cursor = connect.cursor()

    #####
    #####
    # NEED TO REMOVE THESE HARD-CODED ID'S, check lower down for example of ANY
    #####
    #####
    cursor.execute("SELECT member_id, join_date FROM checkins WHERE location_id IN ('ABC-6838', 'ABC-6839')")
    checkins_data = cursor.fetchall()
    cursor.close()
    connect.close()

    checkins_data_no_date = [x for x in checkins_data if x[1] < datetime(1900, 1, 1)]
    checkins_data_with_date = [x for x in checkins_data if x[1] >= datetime(1900, 1, 1)]

    return checkins_data_no_date, checkins_data_with_date


def get_join_date(cnd, cwd, headers, m_id_list):

    connect = psycopg2.connect(
        database='sampler',
        user='postgres',
        password=PSQL_PASS
    )
    cursor = connect.cursor()

    checkins_no_date = cnd
    checkins_with_date = cwd
    c_set = {x[0] for x in checkins_with_date}  # maybe just a list or dict? need to get date, if id present...

    # print(c_set)

    print(f'len of checkins_no_date: {len(checkins_no_date)}')

    for i in range(10):

        try:

            record = random.sample(checkins_no_date, 1)
            # print(record)
            print(record[0][0])
            id = record[0][0]

            # see if the id is in the list that already has join date
            if id in c_set:
                print('join_date ALREADY HERE, goin copy it')
                # loop back through records to find date that matches ID (this could be made more efficient?)
                # maybe don't need set above, just use list? or maybe a dict, and just check if in keys(), then can access key/value at same time?
                for i in checkins_with_date:
                    if id == i[0]:
                        join_date_formatted = i[1]
                        break

            # if not in list, make API call to get join date
            else:

                print('found new checkin')

                response = requests.get(f'https://api.mindbodyonline.com/public/v6/client/clientcompleteinfo?ClientId={id}', headers=headers)

                data = json.loads(response.text)

                # check if the member has a contract with an agreement date...
                if data['ClientContracts']:

                    contract_date_oldest = data['ClientContracts'][0]['AgreementDate']

                    date_dt = datetime.strptime(contract_date_oldest, '%Y-%m-%dT%H:%M:%S')
                    join_date_formatted = datetime.strftime(date_dt, '%Y-%m-%d %H:%M:%S')

                # if no contracts (agreement dates) then just assign these as 1900 so loop will disregard next time
                else:
                    print('no contract agreement dates, writing as 1900')
                    join_date_formatted = '1900-01-01 00:00:00'

            ####
            ####
            # IMPORTANT, CONFIRM THAT IT CAN ONLY UPDATE FOR THE CURRENT UNIT(S) AND NOT ACCIDENTALLY UPDATE RECORDS THAT ARE DUPLICATES of member_id
            ####
            # THIS AND STATEMENT SEEMS TO WORK, BUT NEED TO TEST FURTHER AFTER REMOVING HARD CODED M IDENTIFIERS....
            ####
            # updates ALL join dates for member

            cursor.execute("UPDATE checkins SET join_date = %s WHERE (member_id = %s AND location_id = ANY (%s))", (join_date_formatted, id, m_id_list))

        except Exception as e:
            print(f'error occurred: {e}')

    connect.commit()
    cursor.close()
    connect.close()


def main(date, site_id, id_map, m_id_list):

    headers = {
        'Api-Key': MB_API_KEY,
        'SiteId': site_id,
        'Authorization': get_token(site_id)
    }

    ids = []  # could set this up in chunk() instead?
    bulk_call = MB_bulk_call(date, site_id)
    for i in bulk_call:
        ids.append(i['ID'])

    # print(f'\nids:\n{ids}')

    num_chunks = (len(ids) // CHUNK_SIZE) + 1

    if not test_mode:
        connect = psycopg2.connect(
            database='sampler',
            user='postgres',
            password=PSQL_PASS
        )
        cursor = connect.cursor()

        sql = """
                    INSERT INTO checkins (last, first, birth_date, gender, member_id, phone, email, member_status, location_id, join_date, visit_date, membership_type)
                    VALUES %s
                    """

        psycopg2.extras.execute_values(cursor, sql, chunk(headers, ids, num_chunks, bulk_call, id_map))

        connect.commit()
        cursor.close()
        connect.close()

    ####
    # get join dates (will update join dates for ALL days in checkins table--not just current day)
    ####

    print('getting join dates...')

    last = 0
    current = 1
    delta_loop = current - last

    for i in range(1000):
        # idea is to refresh the list often, speeds up the process...
        # but this may not work since only running one day at a time--might work if several days missing join dates?
        cnd = get_checkins_join()[0]
        cwd = get_checkins_join()[1]

        current = len(cnd)
        delta_loop = current - last

        print(f'current: {current}')
        # print(f'last: {last}')
        print(f'delta_loop: {delta_loop}')

        # break out of loop if no change in len of list cnd
        if delta_loop == 0:
            print(f'no change, break')
            break
        else:
            last = len(cnd)
            # get more join dates (has it's own loop)
            get_join_date(cnd, cwd, headers, m_id_list)

    print(f'--\n --\nfinished company: xyz --\n --\n')
    print(datetime.now())


if __name__ == "__main__":

    main(date, site_id, id_map, m_id_list)
