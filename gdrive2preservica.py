import io
import pickle
import tempfile

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from pyPreservica import *

## export rules
export_formats = {'application/vnd.google-apps.spreadsheet':
                      ['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'xlsx'],
                  'application/vnd.google-apps.document':
                      ['application/vnd.openxmlformats-officedocument.wordprocessingml.document', 'docx'],
                  'application/vnd.google-apps.presentation':
                      ['application/vnd.openxmlformats-officedocument.presentationml.presentation', 'pptx'],
                  'application/vnd.google-apps.drawing': ['application/pdf', 'pdf']}


def authenticate():
    CLIENT_SECRET_FILE = 'client_secrets.json'
    API_SERVICE_NAME = 'drive'
    API_VERSION = 'v3'
    SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/drive.file']

    cred = None

    pickle_file = f'token_{API_SERVICE_NAME}_{API_VERSION}.pickle'

    if os.path.exists(pickle_file):
        with open(pickle_file, 'rb') as fd:
            cred = pickle.load(fd)

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            cred = flow.run_local_server()

        with open(pickle_file, 'wb') as fd:
            pickle.dump(cred, fd)

    try:
        api = build(API_SERVICE_NAME, API_VERSION, credentials=cred)
        print(API_SERVICE_NAME, 'service created successfully')
        return api
    except Exception as e:
        print('Unable to connect.')
        print(e)
        return None


def export_file(service, file):
    if file['mimeType'] == 'application/vnd.google-apps.folder':
        print(f"{file['name']} is a Folder, ignore download")
        return None

    if file['mimeType'] in export_formats:
        export_format = export_formats[file['mimeType']]
        request = service.files().export_media(fileId=file['id'], mimeType=export_format[0])
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        file_name = os.path.join(tempfile.gettempdir(), f"{file['name']}.{export_format[1]}")
        with open(file_name, "wb") as fd:
            fd.write(fh.getvalue())
        return file_name
    else:
        request = service.files().get_media(fileId=file['id'])
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        file_name = os.path.join(tempfile.gettempdir(), f"{file['name']}")
        with open(file_name, "wb") as fd:
            fd.write(fh.getvalue())
        return file_name


def ingest(file_name, file, upload, folder):
    identifiers = {"google-drive-id": file['id']}
    title = file['name']

    # create XML schema from metadata attributes?
    # version = file['version']
    # viewedByMeTime = file['viewedByMeTime']
    # createdTime = file['createdTime']
    # modifiedTime = file['modifiedTime']

    package = simple_asset_package(preservation_file=file_name, parent_folder=folder,  Title=title, Identifiers=identifiers)
    upload.upload_zip_package(path_to_zip_package=package, folder=folder,   delete_after_upload=True)


def main():
    # authenticate with google drive
    service = authenticate()

    # authenticate with Preservica
    entity = EntityAPI()
    upload = UploadAPI()

    # Preservica Folder to ingest files into
    folder = entity.folder("<Preservica Folder ID>")

    results = service.files().list(pageSize=25, fields="*").execute()
    files = results.get('files', [])
    for f in files:
        file_name = export_file(service, f)
        if file_name is not None:
            print(file_name)
            ingest(file_name, f, upload, folder)
    token = results.get('nextPageToken', None)
    while token is not None:
        results = service.files().list(pageSize=25, pageToken=token, fields="*").execute()
        files = results.get('files', [])
        for f in files:
            file_name = export_file(service, f)
            if file_name is not None:
                print(file_name)
                ingest(file_name, f, upload, folder)
        token = results.get('nextPageToken', None)


if __name__ == "__main__":
    main()
