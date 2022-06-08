from __future__ import print_function

import os.path
import json
import re

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError



# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive']

FOLDER_MIME_TYPE = 'application/vnd.google-apps.folder'

CACHE = None

class Memory:
    file_name = 'cache.json'
    data = {}

    def __init__(self) -> None:
        self.load()

    def load(self):
        try:
            with open(self.file_name, 'r') as fh:
                self.data = json.load(fh)
        except FileNotFoundError:
            pass

    def save(self):
        with open(self.file_name, 'w') as fh:
            json.dump(self.data, fh, indent='  ')

    def get_location(self, location):
        if 'locations' not in self.data:
            self.data['locations'] = {}

        path = '/'.join(location)
        return self.data['locations'].get(path, None)

    def set_location(self, location, file):
        if 'locations' not in self.data:
            self.data['locations'] = {}

        path = '/'.join(location)
        self.data['locations'][path] = file
        self.save()

    def add_moved_file(self, file):
        if 'moved_files' not in self.data:
            self.data['moved_files'] = []

        self.data['moved_files'].append(file)
        self.save()


def get_credentials():
    """
    Get credentials from files or ask for login and store credentials
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    return creds


def search_files(service, query):
    page_token = None
    while True:
        response = service.files().list(
            q=query,
            spaces='drive',
            fields='nextPageToken, files(id, name, parents, mimeType)',
            pageToken=page_token,
        ).execute()

        for file in response.get('files', []):
            # print('Found file: %s (%s)' % (file.get('name'), file['id']))
            yield file
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break

    return StopIteration


def move_file(service, file, parent_folder):
    found_file = service.files().get(
        fileId=file['id'],
        fields='parents'
    ).execute()
    previous_parents = ",".join(found_file.get('parents'))
    # Move the file to the new folder
    moved_file = service.files().update(
        fileId=file['id'],
        addParents=parent_folder['id'],
        removeParents=previous_parents,
        fields='id, parents'
    ).execute()

    print('Moved `%s` into `%s`' % (file['id'], parent_folder['id']))
    return moved_file


def get_folder(service, name, parent_folder=None):
    query = "name='%s' and mimeType='%s'" % (name, FOLDER_MIME_TYPE)
    if parent_folder:
        query += " and '%s' in parents" % parent_folder['id']
    return next(search_files(service, query), None)


def create_folder(service, name, parent_folder):
    file_metadata = {
        'name': name,
        'mimeType': FOLDER_MIME_TYPE
    }

    if parent_folder:
        file_metadata['parents'] = [parent_folder['id']]

    folder = service.files().create(
        body=file_metadata,
        fields='id',
    ).execute()
    print('Created folder ID: `%s` with name `%s`' % (folder['id'], name))

    return folder


def create_folder_if_not_exists(service, name, parent_folder=None):
    if folder := get_folder(service, name, parent_folder):
        return folder
    else:
        return create_folder(service, name, parent_folder)


def crawl_folder(service, current_folder, recurse=True):
    for file in search_files(service, "mimeType!='%s' and '%s' in parents" % (FOLDER_MIME_TYPE, current_folder['id'])):
        handle_file(service, file)

    if recurse:
        for folder in search_files(service, "mimeType='%s' and '%s' in parents" % (FOLDER_MIME_TYPE, current_folder['id'])):
            crawl_folder(service, folder, recurse=False)


def handle_file(service, file):
    # Compute location of file
    file_name = file['name']
    if not re.match('^20[0-9]{2}[0-1][0-9][0-3][0-9]_[0-9]{6}', file_name):
        raise Exception('File name is not right: `%s`' % file_name)

    file_location = [file_name[0:4], file_name[0:4] + '-' + file_name[4:6]]

    parent_folder = get_file_from_location(service, file_location)
    moved_file = move_file(service, file, parent_folder)
    # CACHE.add_moved_file(moved_file)


def get_file_from_location(service, file_location):
    parent_folder = CACHE.get_location(file_location)

    if parent_folder is None:
        if len(file_location) > 1:
            parent = get_file_from_location(service, file_location[:-1])
        else:
            parent = CACHE.data.get('root_folder')

        parent_folder = create_folder_if_not_exists(service, file_location[-1], parent)
        CACHE.set_location(file_location, parent_folder)

    return parent_folder


def main():
    """
    Check out:
    - https://developers.google.com/drive/api/quickstart/python
    - https://developers.google.com/drive/api/guides/folder
    """
    global CACHE
    CACHE= Memory()

    creds = get_credentials()
    service = build('drive', 'v3', credentials=creds)

    if not (root_folder := CACHE.data.get('root_folder')):
        root_folder = next(
            search_files(service, "mimeType='application/vnd.google-apps.folder' and name='Camera'"),
        )
        CACHE.data['root_folder'] = root_folder
        CACHE.save()

    crawl_folder(service, root_folder)


if __name__ == '__main__':
    main()
