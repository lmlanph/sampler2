import glob
import os
import pysftp
from datetime import datetime
import boto3

'''
send files if mod date is today's date;
requires some timing coordination to avoid involving 2 consecutive dates (say around midnight)--could cause missed files;
may need better solution in the future;


to do:

how to route to different instances? may need another field in units table, associating unit with its instance;

optional: move sent files to a sub-directory? delete those files (either immediately or after some delay)?

'''


def get_param(param):  # get param store
    ssm_client = boto3.client("ssm", region_name="us-east-1")
    get_response = ssm_client.get_parameter(Name=param, WithDecryption=True)
    return get_response['Parameter']['Value']


base_path = os.environ.get('BASE_PATH')
base_path_ssh = os.environ.get('BASE_PATH_SSH')


# param store vs env vars
Prod = True

if Prod:
    feed_username_core = get_param('FEED_USERNAME_CORE')
    feed_username_join = get_param('FEED_USERNAME_JOIN')
    feed_username_early = get_param('FEED_USERNAME_EARLY')
    hostname_feed3 = get_param('HOSTNAME_FEED3')
    feed_password = get_param('FEED_PASSWORD')
else:
    feed_username_core = os.environ.get('FEED_USERNAME_CORE')
    feed_username_join = os.environ.get('FEED_USERNAME_JOIN')
    feed_username_early = os.environ.get('FEED_USERNAME_EARLY')
    hostname_feed3 = os.environ.get('HOSTNAME_FEED3')
    feed_password = os.environ.get('FEED_PASSWORD')


username = ''


def send_file():

    list_of_files = glob.glob(base_path + 'sample/out/*_ML.csv')  # note wildcard to pick up all files, but still differentiate from other potential files in /out

    now = datetime.now()

    for file in list_of_files:
        # check if file mtime is today's date
        t = os.path.getmtime(file)
        dt = datetime.fromtimestamp(t)

        if '_core_' in file:
            username = feed_username_core
            print('sending core...')
        elif '_join_' in file:
            username = feed_username_join
            print('sending join...')
        elif '_early_' in file:
            username = feed_username_early
            print('sending early...')

        if now.date() == dt.date():

            try:
                with pysftp.Connection(host=hostname_feed3, username=username, password=feed_password) as sftp:
                    print("Connection succesfully stablished")

                    sftp.put(file)

            except Exception as e:
                print(str(e))


def main():

    send_file()


if __name__ == '__main__':
    main()
