import os
import json
import requests
from datetime import date, datetime, timedelta
from google.cloud import storage

dir_path = os.path.dirname(os.path.realpath(__file__))

COLLECTIONS_FILE = os.path.join(dir_path, 'collections.json')
BACKUP_CMD = ' && '.join([
    'rm /tmp/dump -rf',
    'arangodump --compress-output false --server.password "" --output-directory "/tmp/dump" --maskings {}'.format(COLLECTIONS_FILE),
    'cd /tmp',
    'tar -zcvf brightid.tar.gz dump',
])

BUCKET_NAME = 'brightid-backups'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(dir_path, 'google.json')
client = storage.Client()
bucket = client.get_bucket(BUCKET_NAME)

def upload(fpath):
    fname = os.path.basename(fpath)
    blob = bucket.blob(fname)
    with open(fpath, 'rb') as f:
        blob.upload_from_file(f)
    return blob

def get_date(fname):
    s = fname.strip('brightid_').strip('.tar.gz')
    return datetime.strptime(s, '%y%m%d').date()

def delete_extra_files():
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

def records(table):
    pattern = lambda fname: fname.endswith('.data.json') and fname.count(table) > 0
    fname = list(filter(pattern, os.listdir('/tmp/dump')))[0]
    with open(os.path.join('/tmp/dump', fname)) as f:
        ol = [json.loads(line) for line in f.read().split('\n') if line.strip()]
    d = {}
    for o in ol:
        if o['type'] == 2300:
            d[o['data']['_key']] = o['data']
        elif o['type'] == 2302 and o['data']['_key'] in d:
            del d[o['data']['_key']]
    return dict((d[k]['_id'].replace(table+'/', ''), d[k]) for k in d)

def load_json():
    user_groups = records('usersInGroups')
    users = records('users')
    groups = records('groups')
    connections = records('connections')
    ret = {'nodes': [], 'edges': []}
    buf = {}
    for u in users:
        users[u] = {'rank': users[u]['score'], 'name': u , 'groups': []}
        ret['nodes'].append(users[u])
    for user_group in user_groups.values():
        u = user_group['_from'].replace('users/', '')
        g = user_group['_to'].replace('groups/', '')
        users[u]['groups'].append(g)
        if groups[g].get('seed', False):
            users[u]['node_type'] = 'Seed'
    for c in connections.values():
        ret['edges'].append([c['_from'].replace('users/', ''), c['_to'].replace('users/', '')])
    return json.dumps(ret)


if __name__ == '__main__':
    assert os.system(BACKUP_CMD)==0, 'backup failed'

    print('Uploading brightid.tar.gz')
    blob = upload('/tmp/brightid.tar.gz')

    date_str = date.today().strftime('%y%m%d')
    fname_with_date = 'brightid_{}.tar.gz'.format(date_str)
    print('Copying the brightid.tar.gz to {}'.format(fname_with_date))
    bucket.copy_blob(blob, bucket, fname_with_date)

    print('Deleting extra files')
    delete_extra_files()

    with open('/tmp/brightid.json', 'w') as f:
        f.write(load_json())

    print('Uploading brightid.json')
    upload('/tmp/brightid.json')
