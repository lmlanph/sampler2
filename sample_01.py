import os
import datetime
from datetime import datetime, timedelta
import csv
import random
import smtplib
import psycopg2
import boto3

'''
TO DO:

quar lookups should be limited to the current unit (in config) otherwise could slow down with lots of locations

If test mode, have files write out to a different folder called test_out or something, then can see output but it won't get picked up by the SFTP send function accidentally 

Test mode should prevent writing to quarantine, in all cases

'''


def get_param(param):  # get param store
    ssm_client = boto3.client("ssm", region_name="us-east-1")
    get_response = ssm_client.get_parameter(Name=param, WithDecryption=True)
    return get_response['Parameter']['Value']


# param store vs env vars
Prod = True

base_path = os.environ.get('BASE_PATH')

if Prod:
    PSQL_PASS = get_param('PSQL_PASS')
else:
    PSQL_PASS = os.environ.get('PSQL_PASS')

mail_me = False

# test mode will not write feed files, or write to quar
test_mode = False


def get_units():

    connect = psycopg2.connect(
        database='sampler',
        user='postgres',
        password=PSQL_PASS
    )
    cursor = connect.cursor()
    cursor.execute("SELECT m_id, location_name, company, brand, mms, site_cred_1, site_cred_2, status, member_type_exclusions, max_invites, quar_days, survey_types FROM units")
    locations_data = cursor.fetchall()
    cursor.close()
    connect.close()

    # print(locations_data)

    return locations_data


def mailMe(content):
    '''main smtp mail function'''
    msg = EmailMessage()
    msg['Subject'] = 'Manual Processing TEST Reports/Errors'
    msg['From'] = email_add
    msg['To'] = email_to
    msg.set_content(content)

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(email_add, email_pw)

        smtp.send_message(msg)


def ageCalc(date):
    now = datetime.now()
    then = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')  # strptime gives string > datetime
    diff = now - then
    diffYears = diff.days // 365.25
    return diffYears


def TF30D(date):
    # returns True/False if date 30 days or older
    now = datetime.now()
    visit = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')  # strptime gives string > datetime
    diff = now - visit
    if int(diff.days) < 31:
        return True
    else:
        return False


def last_30_visits_count(id):

    now = datetime.now()
    then = now - timedelta(days=30)
    then_str = datetime.strftime(then, "%Y-%m-%d")

    connect = psycopg2.connect(
        database='sampler',
        user='postgres',
        password=PSQL_PASS
    )
    cursor = connect.cursor()
    cursor.execute("SELECT COUNT(member_id) FROM checkins WHERE (member_id = %s AND visit_date > %s);", (id, then_str))
    visit_count = cursor.fetchall()
    cursor.close()
    connect.close()

    # returns int
    return visit_count[0][0]


def last_visit_text(dt_object):

    dt_9 = dt_object.replace(hour=9, minute=0, second=0, microsecond=0)
    dt_12 = dt_object.replace(hour=12, minute=0, second=0, microsecond=0)
    dt_14 = dt_object.replace(hour=14, minute=0, second=0, microsecond=0)
    dt_17 = dt_object.replace(hour=17, minute=0, second=0, microsecond=0)
    dt_20 = dt_object.replace(hour=20, minute=0, second=0, microsecond=0)
    dt_24 = dt_object.replace(hour=23, minute=59, second=59, microsecond=0)

    if dt_object.weekday() in [5, 6]:
        return 'Weekend'
    elif dt_object < dt_9:
        return 'Before Work (Open-9am)'
    elif dt_9 <= dt_object < dt_12:
        return 'Morning (9am-12pm)'
    elif dt_12 <= dt_object < dt_14:
        return 'Lunch (12pm-2pm)'
    elif dt_14 <= dt_object < dt_17:
        return 'Afternoon (2pm-5pm)'
    elif dt_17 <= dt_object < dt_20:
        return 'After Work (5pm-8pm)'
    else:
        return 'Evening (8pm-Close)'


def join_date_checker(begin_day, end_day, dt_object):
    # end_day of 0 would be today
    now = datetime.now()
    begin = now - timedelta(days=begin_day)
    end = now - timedelta(days=end_day)

    if begin <= dt_object <= end:
        return True


def write_rows(id, data, survey_type):
    '''
    see test mode

    '''

    now = datetime.now()
    quar_insert_s = now.strftime('%Y-%m-%d %H:%M:%S')  # use for inserting new quar records

    file_suff = ''

    if survey_type == 0:
        file_suff = '_core_'
    elif survey_type == 1:
        file_suff = '_join_'
    elif survey_type == 2:
        file_suff = '_early_'

    file_str = base_path + 'sample/out/' + id + file_suff + 'ML.csv'

    with open(file_str, 'w') as f:
        writer = csv.writer(f)
        headers = [
            'Last name',
            'First name',
            'Age',
            'Gender',
            'Address',
            'City',
            'State/Province',
            'Zip/Postal code',
            'Country',
            'Member Id',
            'Phone',
            'Email',
            'Member Status',
            'Frequent Club Id',
            'Home Club Id',
            'Date Joined',
            'Last visit date',
            'Membership Type',
            'E Last Visit',
            'E L30 Visits',
            'Last Visit Text'
        ]

        writer.writerow(headers)

        # write quar to postgres
        # opens cursor here, executed lower down...
        connect = psycopg2.connect(
            database='sampler',
            user='postgres',
            password=PSQL_PASS
        )
        cursor = connect.cursor()

        for i in data:

            bday_str = datetime.strftime(i[2], '%Y-%m-%d %H:%M:%S')

            gender = '-'
            if i[3].lower().startswith('f'):
                gender = 'Female'
            elif i[3].lower().startswith('m'):
                gender = 'Male'

            phone = i[5]
            if phone and len(phone) == 10:
                phone_formatted = '(' + phone[:3] + ') ' + phone[3:6] + '-' + phone[6:]
            else:
                phone_formatted = phone

            last_visit_formatted = datetime.strftime(i[10], '%-m/%-d/%Y')
            last_visit_datetime_formatted = datetime.strftime(i[10], '%-m/%-d/%Y %-I:%M %p')
            join_date_formatted = datetime.strftime(i[9], '%-m/%-d/%Y')

            # PUT HERE SO THAT I COULD COLLECT THE INSERTS, BEFORE REMOVING EMAILS FROM RANDSEND
            # THEN COMMIT LATER... CONFIRM ADDING TO QUAR PROPERLY
            cursor.execute('INSERT INTO quar (timestamp, email, survey, location_id) VALUES (%s, %s, %s, %s)', (quar_insert_s, i[6], survey_type, id))

            row = [
                i[0],
                i[1],
                int(ageCalc(bday_str)),
                gender,
                '',
                '',
                '',
                '',
                '',
                i[4],
                phone_formatted,
                i[6],
                i[7],
                i[8],  # frequent club
                i[8],  # home club
                join_date_formatted,  # date joined
                last_visit_formatted,  # last visit date
                i[11],
                last_visit_datetime_formatted,
                last_30_visits_count(i[4]),
                last_visit_text(i[10])
            ]
            writer.writerow(row)

        connect.commit()
        cursor.close()
        connect.close()


def sampler(company, config):

    print(f'\nworking on company: {company}\n')

    for id in config:

        print(id)
        print(config[id]['mms'])
        print('\n')

        max_invites = config[id]['max_invites']

        quar_days = config[id]['quar_days']

        piped_types = config[id]['member_type_exclusions']
        membership_types_excluded = [x.lower() for x in piped_types.split('|')]

        survey_types = config[id]['survey_types'].split('|')

        print(f'survey types: {survey_types}')

        # in some cases, set() may not be necessary as data is now dup free, with DISTINCT ON query
        emailsQuarSet = set()
        emailsPoolSet = set()
        emailsEligSet = set()

        emailsQuarSetJoin = set()
        emailsQuarSetEarly = set()

        try:

            connect = psycopg2.connect(
                database='sampler',
                user='postgres',
                password=PSQL_PASS
            )

            # DISTINCT ON removes duplicates by email address
            # not sure why email required in ORDER BY clause...
            cursor = connect.cursor()
            cursor.execute("SELECT DISTINCT ON (email) last, first, birth_date, gender, member_id, phone, email, member_status, location_id, join_date, visit_date, membership_type FROM checkins WHERE location_id = %s ORDER BY email, visit_date DESC", (id,))
            data = cursor.fetchall()
            cursor.close()
            connect.close()
            # returns list of tup, ex: ('Smith', 'Jan', datetime.datetime(1996, 1, 21, 0, 0), 'Female', '00121', '1231231234', 'test.test@gmail.com', 'Active', '2', datetime.datetime(1901, 1, 1, 0, 0), datetime.datetime(2022, 7, 20, 12, 0), 'still need membership type')
            # printing that date by itself looks like this? 1996-01-21 00:00:00 (but is type datetime.datetime...?)

            print(f'len data: {len(data)}')
            # print(data)

            data_core = []
            data_join = []
            data_early = []

            # build data lists/sets for each survey
            for i in data:
                bday_str = datetime.strftime(i[2], '%Y-%m-%d %H:%M:%S')
                visit_date_str = datetime.strftime(i[10], '%Y-%m-%d %H:%M:%S')
                join_date_str = datetime.strftime(i[9], '%Y-%m-%d %H:%M:%S')

                rules_base = [
                    i[6] and ageCalc(bday_str) >= 18,
                    i[7] == 'Active',
                    i[11].lower() not in membership_types_excluded,
                    not i[11].lower().startswith('emp'),
                ]

                rules_core = [
                    not TF30D(join_date_str),
                    TF30D(visit_date_str)
                ]

                rules_join = [
                    # new rule with join_date_str stating join within last 7 days
                    join_date_checker(7, 0, i[9])
                ]

                rules_early = [
                    # new rule with join_date_str stating join 21-28 days ago
                    join_date_checker(28, 21, i[9])
                ]

                if all(rules_base) and all(rules_core):
                    emailsPoolSet.add(i[6])

                if '1' in survey_types and all(rules_base) and all(rules_join):
                    data_join.append(i)

                if '2' in survey_types and all(rules_base) and all(rules_early):
                    data_early.append(i)

            now = datetime.now()
            quar_date = now - timedelta(quar_days)
            quar_date_join_early = now - timedelta(days=180)  # can't get one of these surveys more than once per 180 days? confirm this number...
            quar_date_s = quar_date.strftime('%Y-%m-%d')  # use for calculating elig email pool
            quar_date_join_early_s = quar_date_join_early.strftime('%Y-%m-%d')

            #####
            #####
            # get quar for each survey type
            connect = psycopg2.connect(
                database='sampler',
                user='postgres',
                password=PSQL_PASS
            )
            cursor = connect.cursor()
            cursor.execute("SELECT * FROM quar WHERE (timestamp > %s AND survey = %s)", (quar_date_s, '0'))
            data_quar_core = cursor.fetchall()
            cursor.execute("SELECT * FROM quar WHERE (timestamp > %s AND survey = %s)", (quar_date_join_early_s, '1'))
            data_quar_join = cursor.fetchall()
            cursor.execute("SELECT * FROM quar WHERE (timestamp > %s AND survey = %s)", (quar_date_join_early_s, '2'))
            data_quar_early = cursor.fetchall()

            loc_id_count = 0

            # get list of quar emails, as well as count of location id in quar
            for i in data_quar_core:
                emailsQuarSet.add(i[2])

                if i[4] == id:
                    loc_id_count += 1

            for i in data_quar_join:
                emailsQuarSetJoin.add(i[2])

            for i in data_quar_early:
                emailsQuarSetEarly.add(i[2])

            cursor.close()
            connect.close()
            #####
            #####

            emailsEligSet = emailsPoolSet - emailsQuarSet

            # take 1/90th (or whatever quar is) of BOTH the eligible emails, and the emails (by loc id) that are in quar already
            dailyCount = (len(emailsEligSet) // quar_days) + (loc_id_count // quar_days)

            if 0 <= dailyCount < 1:  # if a fraction, just round up to 1 for daily
                dailyCount = 1

            if dailyCount < max_invites:
                max_invites = dailyCount

            print(f'pool set: {len(emailsPoolSet)}')
            print(f'emails quar set: {len(emailsQuarSet)}')
            print(f'elig set: {len(emailsEligSet)}')
            print(f'daily count: {dailyCount}')
            print(f'max ivites: {max_invites}')

            randsend = random.sample(emailsEligSet, max_invites)

            # write feed files and quarantine
            for i in data:

                if i[6] and i[6] in randsend:
                    data_core.append(i)

            # check data against quar
            data_join_2 = [x for x in data_join if x[6] not in emailsQuarSetJoin]

            data_early_2 = [x for x in data_early if x[6] not in emailsQuarSetEarly]

            if not test_mode:
                # write core
                write_rows(id, data_core, 0)

                # write join/early if records exist
                if data_join_2:
                    write_rows(id, data_join_2, 1)

                if data_early_2:
                    write_rows(id, data_early_2, 2)

            print(f'sampling finished for location {id} \n')

        except Exception as e:
            eType = type(e)
            e_message = '\tERROR PROCESSING FILES FOR LOCATION: xyz,\n\te type\n\tTHE FOLLOWING ERROR OCCURRED: ' + str(e) + '\n'
            if mail_me:
                mailMe(e_message)
            else:
                print(e_message)


def main():

    # from units query, create dict with company name as key(s), config as value(s)
    # filter for active units only...
    d = {}

    for i in get_units():
        if i[7] == 'Active':
            if i[2] in d.keys():
                d[i[2]][i[0]] = {'location_name': i[1], 'brand': i[3], 'mms': i[4], 'site_cred_1': i[5], 'site_cred_2': i[6], 'status': i[7], 'member_type_exclusions': i[8], 'max_invites': i[9], 'quar_days': i[10], 'survey_types': i[11]}
            else:
                d[i[2]] = {i[0]: {'location_name': i[1], 'brand': i[3], 'mms': i[4], 'site_cred_1': i[5], 'site_cred_2': i[6], 'status': i[7], 'member_type_exclusions': i[8], 'max_invites': i[9], 'quar_days': i[10], 'survey_types': i[11]}}

    # print(d)

    for k, v in d.items():
        sampler(k, v)


if __name__ == "__main__":
    main()
