"""
Gets list of users from Google Apps.

"""

from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
import apiclient
import apiclient.discovery
import httplib2
import logging
import logging.handlers
import os
import sys
import time

__all__ = ["get_users"]

BASE_PATH = os.path.dirname(os.path.abspath(__file__))+"/"


logger = logging.getLogger('google-user-list')
logger.setLevel("INFO")
handler = logging.handlers.SysLogHandler(address = '/dev/log')
formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def get_users(domain):
    storage = Storage(BASE_PATH+'get_users_credentials')
    scopes = ('https://www.googleapis.com/auth/admin.directory.user.readonly')
    credentials = storage.get()
    if not credentials:
        flow = flow_from_clientsecrets(BASE_PATH+'client_secrets_get_users.json',
                               scope=scopes,
                               redirect_uri='urn:ietf:wg:oauth:2.0:oob')
        auth_uri = flow.step1_get_authorize_url()
        print auth_uri
        code = raw_input("Auth token: ")
        credentials = flow.step2_exchange(code)
        storage.put(credentials)

    http = httplib2.Http()
    http = credentials.authorize(http)

    service = apiclient.discovery.build("admin", 'directory_v1', http=http)

    users = []
    next_page_token = None
    while True:
        for retrycount in range(1, 4):
            try:
                users_page = service.users().list(fields='nextPageToken,users(primaryEmail,name)', domain=domain, pageToken=next_page_token, maxResults=500).execute()
                next_page_token = users_page.get("nextPageToken")
                users_page = users_page.get("users")
                users.extend(users_page)
                break
            except IOError:
                logger.warning("Downloading list of users failed")
                time.sleep(retrycount)
        if not next_page_token:
            break
    users_filtered = {}
    for user in users:
        users_filtered[user["name"]["fullName"]] = user["primaryEmail"]
    return users_filtered

def usage():
    print """
Usage: %s <domain>

Prints list of users from Google Apps
"""

def main():
    if len(sys.argv) == 1:
        usage()
        sys.exit(1)
    for domain in sys.argv[1:]:
        users = get_users(domain)
        for item in users:
            print item, users[item]

if __name__ == '__main__':
    main()

