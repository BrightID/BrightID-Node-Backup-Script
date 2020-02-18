import os
import requests
from datetime import date, datetime, timedelta
from google.cloud import storage

dir_path = os.path.dirname(os.path.realpath(__file__))
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(dir_path, 'google.json')
COLLECTIONS_FILE = os.path.join(dir_path, 'collections.json')
BUCKET_NAME = 'brightid-backups'
BACKUP_CMD = ' && '.join([
    'rm /tmp/dump -rf',
    'arangodump --compress-output false --server.password "" --output-directory "/tmp/dump" --maskings {}'.format(COLLECTIONS_FILE),
    'cd /tmp',
    'tar -zcvf brightid.tar.gz dump'
])

def upload(fpath):
    print('Uploading brightid.tar.gz')
    client = storage.Client()
    bucket = client.get_bucket(BUCKET_NAME)
    fname = os.path.basename(fpath)
    blob = bucket.blob(fname)
    with open(fpath, 'rb') as f:
        blob.upload_from_file(f)
    # copy the brightid.tar.gz to brightid_yymmdd.tar.gz
    date_str = date.today().strftime('%y%m%d')
    fname_with_date = fname.replace('.tar.gz', '_{}.tar.gz'.format(date_str))
    print('Copying brightid.tar.gz to {}'.format(fname_with_date))
    bucket.copy_blob(blob, bucket, fname_with_date)

def get_date(fname):
    s = fname.strip('brightid_').strip('.tar.gz')
    return datetime.strptime(s, '%y%m%d').date()

def delete_extra_files():
    print('Deleting extra files')
    client = storage.Client()
    blobs = client.list_blobs(BUCKET_NAME)
    blobs = {blob.name: blob for blob in blobs if blob.name.startswith('brightid_')}
    fnames = blobs.keys()
    today = date.today()
    for fname in fnames:
        days = (today - get_date(fname)).days
        keep = False
        # keep one file per day for a week
        if days < 7:
            keep = True
        # keep one file per week for a month
        elif days < 30 and get_date(fname).weekday() == 1:
            keep = True
        # keep one file per month forever
        elif get_date(fname).day == 1:
            keep = True
        if not keep:
            print('delete {} from cloud'.format(fname))
            blobs[fname].delete()

if __name__ == '__main__':
    assert os.system(BACKUP_CMD)==0, 'backup failed'
    upload('/tmp/brightid.tar.gz')
    delete_extra_files()
