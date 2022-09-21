import os
from datetime import timedelta, datetime
import MB_checkins as mb
import psycopg2
import psycopg2.extras
import boto3

'''
TO DO:
backfill process won't work with multiple companies (if active, will backfill all companies), need to specify unit(s) and/or company;

should checkins DB write move up to this file, rather than in MB file?

'''


def get_param(param):  # get param store
    ssm_client = boto3.client("ssm", region_name="us-east-1")
    get_response = ssm_client.get_parameter(Name=param, WithDecryption=True)
    return get_response['Parameter']['Value']


# param store vs env vars
Prod = True


if Prod:
    PSQL_PASS = get_param('PSQL_PASS')
else:
    PSQL_PASS = os.environ.get('PSQL_PASS')


checkin_dates = []
# date_str_today = datetime.strftime(datetime.now(), '%m/%d/%Y')
# if running after midnight, get yesterday's checkins instead of todays
date_str_yesterday = datetime.strftime(datetime.now() - timedelta(days=1), '%m/%d/%Y')

# backfill in dev--Could be based on unit status in units table...?
# if backfill, don't run all clubs, instead run specific clubs? backfill_locations = ['ABC-6839'] use a list if identifiers?
backfill = False
backfill_date = '09/15/2022'  # date to END (most recent date)
backfill_days = 5  # number of days to go back, including final "backfill_date" e.g. 1 would just run the backfill_date only

if backfill:

    backfill_end_date = datetime.strptime(backfill_date, '%m/%d/%Y')

    for i in range(backfill_days):
        i_date = backfill_end_date - timedelta(days=i)
        date_str = datetime.strftime(i_date, '%m/%d/%Y')
        checkin_dates.append(date_str)

else:  # just run current day (yesterday, if run after midnight)
    checkin_dates.append(date_str_yesterday)

print(checkin_dates)


def get_units():

    connect = psycopg2.connect(
        database='sampler',
        user='postgres',
        password=PSQL_PASS
    )
    cursor = connect.cursor()
    cursor.execute("SELECT m_id, location_name, company, brand, mms, site_cred_1, site_cred_2, status, member_type_exclusions, max_invites FROM units")
    units_data = cursor.fetchall()
    cursor.close()
    connect.close()

    return units_data


def units_dict():

    d = {}

    for i in get_units():
        if i[7] == 'Active':
            if i[2] in d.keys():
                d[i[2]][i[0]] = {'location_name': i[1], 'brand': i[3], 'mms': i[4], 'site_cred_1': i[5], 'site_cred_2': i[6], 'status': i[7], 'member_type_exclusions': i[8], 'max_invites': i[9]}
            else:
                d[i[2]] = {i[0]: {'location_name': i[1], 'brand': i[3], 'mms': i[4], 'site_cred_1': i[5], 'site_cred_2': i[6], 'status': i[7], 'member_type_exclusions': i[8], 'max_invites': i[9]}}

    return d


unit_dict = units_dict()

print(f'\nunit_dict is: {unit_dict}\n')

for company in unit_dict:

    id_map = {}

    for unit in unit_dict[company]:
        # unit is the _id in this case...
        id_map[unit_dict[company][unit]['site_cred_2']] = unit

    m_id_list = [unit for unit in unit_dict[company]]
    print(f'm_id_list: {m_id_list}')

    # MAY WANT TO PUT CHECK HERE, TO ALERT IF EACH UNIT ISN'T THE SAME SITE ID? THIS CURRENTLY WILL JUST GRAB THE LAST SITE ID IN THE UNIT LOOP (should all be the same)...
    site_id = unit_dict[company][unit]['site_cred_1']
    print(f'site_id: {site_id}')
    print(f'id_map is: {id_map}\n')

    # NOTE THAT site_id IS SPECIFIC FOR MB, NEED TO MAKE MORE FLEXIBLE...
    for date in checkin_dates:
        mb.main(date, site_id, id_map, m_id_list)
