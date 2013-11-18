#!/usr/bin/env python
# pylint: disable=C0301

"""
CREATE TABLE filestatus (fileid text, last_fetch datetime, CONSTRAINT only_once UNIQUE(fileid) ON CONFLICT REPLACE);
CREATE TABLE filetodo (fileid text, user_email text, last_mod datetime, CONSTRAINT only_once UNIQUE(fileid) ON CONFLICT REPLACE);
CREATE TABLE user (user_email text, last_fetch datetime, start_change_id long, CONSTRAINT only_once UNIQUE(user_email) ON CONFLICT REPLACE);
"""

from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials
from oauth2client.file import Storage
import Levenshtein
import apiclient.errors
import cPickle as pickle
import datetime
import dateutil.parser
import httplib2
import json
import logging
import logging.handlers
import random
import sqlite3
import time
import os
import sys
import gdata.contacts.data
import gdata.contacts.client
from get_users import get_users

__all__ = ["UserData", "DriveUser", "DriveAuth", "UserFreq"]

BASE_PATH = os.path.dirname(os.path.abspath(__file__))+"/"

with open(BASE_PATH+"private/settings.json") as datafile:
    settings = json.load(datafile)
db = sqlite3.connect(BASE_PATH+"private/data/data.db", isolation_level=None)

logger = logging.getLogger('drive_revisions')
logger.setLevel("INFO")
handler = logging.handlers.SysLogHandler(address = '/dev/log')
formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

MIN_TIMESTAMP = "2013-05-01"
http_post = httplib2.Http()


def chunks(l, n): 
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


class UserData:
    """ Holds user contacts. 'contacts.json' is a dictionary with "full name": "email address" pairs """

    def __init__(self, filename=BASE_PATH+"private/data/contacts.json"):
        self.filename = filename
        self._ts_file = self.filename+".timestamp"
        self._d_file = self.filename
        self.load()

    def __unicode__(self):
        return "UserData", self.filename

    def get_best_contacts(self, name):
        """ Gets the best matching contacts. For example, with names containing special characters, no exact match is often possible """
        self.load()
        best_value = 100
        best_hits = []
        for key in self.data:
            distance = Levenshtein.distance(key, name)
            if distance > 2:
                continue
            if distance == best_value:
                best_hits.append(self.data[key])
            elif distance < best_value:
                best_value = distance
                best_hits = [self.data[key]]
        if len(best_hits) > 1:
            return []
        return best_hits

    def need_reload(self):
        if not os.path.isfile(self._ts_file) or not os.path.isfile(self._d_file):
            return True
        with open(self._ts_file) as ts_file:
            last_timestamp = dateutil.parser.parse(ts_file.read())
        if datetime.datetime.now() - last_timestamp > datetime.timedelta(hours=3):
            return True
        return False

    def load(self, force=False):
        if force or self.need_reload():
            contacts = get_users(settings["google-domain"])
            json.dump(contacts, open(self._d_file, "w"))
            with open(self._ts_file, "w") as ts_file:
                ts_file.write(str(datetime.datetime.now()))
        else:
            with open(self._d_file) as datafile:
                contacts = json.load(datafile)
        self.data = contacts

class UserFreq:
    """ User frequency data. """
    def __init__(self, filename=BASE_PATH+"private/data/user_timestamps.pickle"):
        self.filename = filename
        try:
            with open(self.filename) as datafile:
                self.data = pickle.load(datafile)
        except (IOError, EOFError), e:
            raise e
            #self.data = {}
        self.unsaved_add_count = 0

    def __enter__(self):
        return self

    def __exit__(self, a_key, a_value, traceback):
        self.save()

    def __unicode__(self):
        return "UserFreq", self.filename

    def save(self):
        """ Save changes """
        if self.unsaved_add_count > 0:
            with open(self.filename, "w") as datafile:
                pickle.dump(self.data, datafile)
        self.unsaved_add_count = 0

    def check_and_add(self, parsed, user):
        """ Returns True if user do not have entries around proposed time. If True, adds user entry to dictionary """
        udata = self.data
        if parsed.year not in udata:
            udata[parsed.year] = {}
        if parsed.month not in udata[parsed.year]:
            udata[parsed.year][parsed.month] = {}
        if parsed.day not in udata[parsed.year][parsed.month]:
            udata[parsed.year][parsed.month][parsed.day] = {}
        udata = udata[parsed.year][parsed.month][parsed.day]
        if parsed.hour not in udata:
            udata[parsed.hour] = {}
        minute = parsed.minute - parsed.minute % 5
        udata = udata[parsed.hour]
        if minute not in udata:
            udata[minute] = set()
        if user not in udata[minute]:
            udata[minute].add(user)
            self.unsaved_add_count += 1
            if self.unsaved_add_count > 50:
                self.save()
            return True
        else:
            return False



class DriveAuth:
    """ Provides Google API service for Drive with caching """
    def __init__(self):
        self.storage = Storage(BASE_PATH+"private/main_credentials.json")
        self.service_account_email = settings["service-account-email"] 
        self.service_account_pkcs12 = BASE_PATH+"private/"+settings["service-account-key"]
        self.cache = {}
        with open(self.service_account_pkcs12, "rb") as keyfile:
            self.key = keyfile.read()

    def __unicode__(self):
        return "DriveAuth"

    def create_drive_service(self, user_email):
        """ Returns non-cached service """
        credentials = SignedJwtAssertionCredentials(self.service_account_email, self.key,
        scope='https://www.googleapis.com/auth/drive', sub=user_email)
        http = httplib2.Http()
        http = credentials.authorize(http)
        return build('drive', 'v2', http=http)

    def get_drive_service(self, email):
        """ Returns cached version if available, creates and caches a new service otherwise """
        if email not in self.cache:
            logger.debug("Service provider cache: %s not found" % email)
            self.cache[email] = self.create_drive_service(email)
        return self.cache[email]

class DriveUser:
    """ Represents a single user for retrieving Drive revisions. """

    def __enter__(self):
        return self

    def __init__(self, user_email, contacts, ufreq, auth):
        self.user_email = user_email
        self.contacts = contacts
        self.ufreq = ufreq
        self.auth = auth
        self.post_data = []

        self.service = self.auth.get_drive_service(self.user_email)

    def __unicode__(self):
        return "DriveUser", self.user_email

    def __exit__(self, type, value, tb):
        self.post()

    def post(self):
        """ Posts any available data to login server """
        DriveUser.execute_with_retry(self.__post)

    def __post(self):
        if len(self.post_data) > 0:
            for data in chunks(self.post_data, 100):
                _, content = http_post.request(settings["server-url"], "POST", body=json.dumps(data))
            self.post_data = []

    def _get_changes(self, page_token=None, start_change_id=None):
        """ Gets changes listing for the user. """
        request = self.service.changes().list(maxResults=1000, fields='items(file,fileId,modificationDate),largestChangeId,nextPageToken', pageToken=page_token, startChangeId=start_change_id, includeDeleted=False, includeSubscribed=False)
        data = request.execute()
        return data

    def _get_revisions(self, file_id):
        """ Gets revisions for the item. Returns empty list if exception occurs (for example, Drive only supports
            revisions for certain types of files, with no mechanism to determine which). """
        request = self.service.revisions().list(fileId=file_id, fields='items(lastModifyingUserName,modifiedDate)')
        try:
            data = request.execute()
        except apiclient.errors.HttpError:
            return {"items": []}
        return data

    def update_user_data(self, last_fetch, start_change_id):
        """ Saves user data to the database """
        cur = db.cursor()
        query = "INSERT INTO user VALUES (?,?,?);"
        cur.execute(query, (self.user_email, last_fetch, start_change_id))
        db.commit()

    @classmethod
    def execute_with_retry(cls, func, *args):
        """ Executes func three times, sleeping n seconds after each retry """
        for req in range(0, 3):
            try:
                data = func(*args)
                return data
            except:
                time.sleep(req)
        return None
        
    def get_changes(self, largest_change_id):
        """ Downloads all user changes after largest_change_id """
        data = DriveUser.execute_with_retry(self._get_changes, None, largest_change_id)
        if data is None:
            return
        cur = db.cursor()
        inserted = skip_min = skip_mod_status = skip_mod_todo = 0
        count = 0
        while True:
            count += 1
            if count % 50 == 0:
                self.post()
            logger.debug("User %s: %s changes", self.user_email, len(data["items"]))
            for item in data["items"]:
                if item["modificationDate"] < MIN_TIMESTAMP:
                    skip_min += 1
                    continue

                if "file" in item:
                    parsed = dateutil.parser.parse(item["modificationDate"])
                    if "lastModifyingUserName" in item["file"]:
                        users = self.contacts.get_best_contacts(item["file"]["lastModifyingUserName"])
                        if len(users) == 1:
                            user = users[0]
                            if self.ufreq.check_and_add(parsed, user):
                                self.post_data.append({"system": "drive_revision", "username": user, "timestamp": item["modificationDate"], "is_utc": True, "data": item["file"]["id"]})
 
                # Check whether file has been checked after this modification
                query = "SELECT last_fetch FROM filestatus WHERE fileid=?;"
                cur.execute(query, (item["fileId"],))
                result = cur.fetchone()
                if result is not None:
                    (last,) = result
                    if last >= item["modificationDate"]:
                        skip_mod_status += 1
                        continue
                query = "SELECT last_mod FROM filetodo WHERE fileid=?;"
                cur.execute(query, (item["fileId"],))
                result = cur.fetchone()
                if result is not None:
                    (last,) = result
                    if last >= item["modificationDate"]:
                        skip_mod_todo += 1
                        continue

                query = "INSERT INTO filetodo VALUES (?,?,?);"
                cur.execute(query, (item["fileId"], self.user_email, item["modificationDate"]))
                db.commit()
                inserted += 1
            next_page_token = data.get("nextPageToken")
            largest_change_id = data.get("largestChangeId")
            self.update_user_data(datetime.datetime.now(), largest_change_id)
            if not next_page_token or not largest_change_id:
                break
            data = DriveUser.execute_with_retry(self._get_changes, next_page_token, largest_change_id)
            if data is None:
                return

        logger.debug("Statistics: %s; inserted=%s, skip_min=%s, skip_mod_status=%s, skip_mod_todo=%s", self.user_email, inserted, skip_min, skip_mod_status, skip_mod_todo)
        # End of user data
        self.update_user_data(datetime.datetime.now(), largest_change_id)
        self.ufreq.save()
        self.post()
        return

    def get_item_revisions(self, file_id, last_mod):
        """ Downloads and posts all revisions for file_id, made after filestatus->last_fetch. last_mod parameter is for saving the most recent timestamp. """
        cur2 = db.cursor()
        query = "SELECT last_fetch FROM filestatus WHERE fileid=?;"
        cur2.execute(query, (file_id, ))
        result = cur2.fetchone()
        if result is None:
            last_modified = "1970-01-01T00:00:00"
        else:
            (last_modified,) = result
        last_modified_save = last_mod
        data = DriveUser.execute_with_retry(self._get_revisions, file_id)
        if not data:
            return

        for item in data["items"]:
            if "modifiedDate" not in item or "lastModifyingUserName" not in item:
                continue
            if item["modifiedDate"] < last_modified:
                continue
            if item["modifiedDate"] > last_modified_save:
                last_modified_save = item["modifiedDate"]
            if item["modifiedDate"] < MIN_TIMESTAMP:
                continue
            users = self.contacts.get_best_contacts(item["lastModifyingUserName"])
            if len(users) != 1:
                continue
            user = users[0]

            parsed = dateutil.parser.parse(item["modifiedDate"])
            if self.ufreq.check_and_add(parsed, user):
                self.post_data.append({"system": "drive_revision", "username": user, "timestamp": item["modifiedDate"], "is_utc": True, "data": file_id})

        query = "DELETE FROM filetodo WHERE fileid=?;"
        cur2.execute(query, (file_id,))
        db.commit()
        query = "INSERT INTO filestatus VALUES (?,?);"
        cur2.execute(query, (file_id, last_modified_save))
        db.commit()
        self.post()

    def process(self):
        """ Downloads all past changes and all pending revisions """
        db.commit()
        cur = db.cursor()
        query = "SELECT start_change_id FROM user WHERE user_email=?;"
        cur.execute(query, (self.user_email,))

        sets = cur.fetchone()
        if sets is None:
            start_change_id = None
        else:
            (start_change_id,) = sets
        self.get_changes(start_change_id)
        query = "SELECT fileid, last_mod FROM filetodo WHERE user_email=?;"
        db.commit()
        cur = db.cursor()
        cur.execute(query, (self.user_email,))
        count = 0
        while True:
            item = cur.fetchone()
            if item is None:
                break
            count += 1
            (item, last_mod) = item
            self.get_item_revisions(item, last_mod)
            if count % 100 == 0:
                self.ufreq.save()
        self.ufreq.save()



class LockFile:
    """ PID file handling """

    def __init__(self, filename):
        self.pidfile = filename
        self.pid = str(os.getpid())

    def __enter__(self):
        return self

    def __exit__(self, a_type, a_value, traceback):
        self.cleanup()

    def cleanup(self):
        try:
            os.remove(self.pidfile)
        except OSError:
            pass

    def start(self):
        with open(self.pidfile, "w") as piddata:
            piddata.write(self.pid)

    def is_running(self):
        if os.path.isfile(self.pidfile):
            with open(self.pidfile) as piddata:
                old_pid = piddata.read()
            try:
                os.kill(int(old_pid), 0)
                return True
            except OSError:
                pass
        return False


def main():
    """ Fetches all new revisions for all users """
    with LockFile(BASE_PATH+"drive_download_revisions.pid") as lock:
        if lock.is_running():
            logger.info("Lock file exists and process is running. Skipping execution.")
            sys.exit(1)
        lock.start()
        contacts = UserData()
        auth = DriveAuth()
        with UserFreq() as ufreq: 
            while True:
                emails = contacts.data.values()
                random.shuffle(emails)
                for user_email in emails:
                    with DriveUser(user_email, contacts, ufreq, auth) as user:
                        user.process()

if __name__ == '__main__':
    main()
