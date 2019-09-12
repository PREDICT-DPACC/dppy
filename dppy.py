#!/usr/bin/env python

import os
import sys
import logging
import pymongo
from pymongo import MongoClient
from celery import Celery, states
from celery.exceptions import Ignore
from celery.task.control import inspect
import ssl
import re
import json
import collections as col
from datetime import datetime
import dppylib
from dppylib.tools import database as dbtools

# Load celery configurations
celery_config = 'config.json'
if os.getenv('dppy_config'):
    celery_config = os.getenv('dppy_config')
with open(celery_config, 'r') as json_file:
    configuration = json.load(json_file)

# Set up Celery App and its options
app = Celery(
    configuration['celery']['queue'],
    broker=configuration['celery']['broker'],
    backend=configuration['celery']['backend'])

app.conf.update(
    CELERY_RESULT_BACKEND = configuration['celery']['CELERY_RESULT_BACKEND'],
    CELERY_RESULT_PERSISTENT = configuration['celery']['CELERY_RESULT_PERSISTENT'],
    CELERY_TASK_SERIALIZER = configuration['celery']['CELERY_TASK_SERIALIZER'],
    CELERY_RESULT_SERIALIZER = configuration['celery']['CELERY_RESULT_SERIALIZER'],
    CELERY_IGNORE_RESULT = configuration['celery']['CELERY_IGNORE_RESULT'],
    CELERY_ACCEPT_CONTENT = configuration['celery']['CELERY_ACCEPT_CONTENT'],
    CELERYD_STATE_DB = configuration['celery']['CELERYD_STATE_DB'],
    BROKER_USE_SSL = {
        'keyfile': configuration['celery']['BROKER_USE_SSL']['keyfile'],
        'certfile': configuration['celery']['BROKER_USE_SSL']['certfile'],
        'ca_certs': configuration['celery']['BROKER_USE_SSL']['ca_certs'],
        'cert_reqs': ssl.CERT_REQUIRED
    },
    BROKER_TRANSPORT_OPTIONS = configuration['celery']['BROKER_TRANSPORT_OPTIONS'])

# Set up logger
logger = logging.getLogger(os.path.basename(__file__))
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Celery main task
@app.task(bind=True, name='import', queue=configuration['celery']['queue'])
def main(self, root_dir, root_directory, study, subject, dbusername, dbpassword, dbhost, dbport, dbauthsource, dbname):

    # Raise an exception if the import directory does not exist.
    if not os.path.isdir(root_directory):
        logger.error('{PATH} does not exist. Exiting.'.format(PATH=root_directory))
        self.update_state(state=states.FAILURE, meta='{PATH} does not exist.'.format(PATH=root_directory))
        raise Ignore()

    # Celery status update
    if subject and study:
        self.update_state(state='PROCESSING {SUB} in {STUDY}'.format(SUB=subject, STUDY=study))
    else:
        self.update_state(state='PROCESSING')

    # Connect to the database
    logger.info('connecting to database')
    uri = 'mongodb://' + str(dbusername) + ':' + str(dbpassword) + '@' + str(dbhost) + ':' + str(dbport) + '/' + str(dbauthsource)
    client = MongoClient(uri,
                        ssl=True,
                        ssl_certfile=configuration['mongodb']['ssl']['ssl_certfile'],
                        ssl_keyfile=configuration['mongodb']['ssl']['ssl_keyfile'],
                        ssl_cert_reqs=ssl.CERT_REQUIRED,
                        ssl_ca_certs=configuration['mongodb']['ssl']['ca_certs'])
    db = client[dbname]

    # clean out incomplete table-of-content entries
    logger.info('Sanitizing database')
    dbtools.sanitize(db)

    try:
        for import_dir, dirs, files in os.walk(root_directory):
            # Filter out hidden files and directories
            files = [f for f in files if not f[0] == '.']
            dirs[:] = [d for d in dirs if not d[0] == '.']

            # Filter out directories for the raw data
            dirs[:] = [d for d in dirs if not d == 'raw']

            for file_name in files:
                if len(file_name.split('-')) == 4 or file_name.endswith('_metadata.csv'):
                    import_file(import_dir, file_name, db)

    except Exception as e:
        logger.error(e)

    if subject and study:
        clean_toc_subject(db, study, subject)
    elif study:
        clean_toc_study(db, study)
    else:
        clean_toc(db)

    max_days = get_lastday(db)
    if max_days:
        clean_metadata(db, max_days)

# Get the highest number of time_end
def get_lastday(db):
    return list(db.toc.aggregate([
        {
            '$group' : {
                '_id' : { 'study': '$study', 'subject' : '$subject'},
                'days' : {'$max' : '$time_end'},
                'synced' : {'$max' : '$updated'}
            }
        }
    ]))

# clean metadata collection
def clean_metadata(db, max_days):
    studies = col.defaultdict()
    subjects = []

    for subject in max_days:
        if subject['_id']['study'] not in studies:
            studies[subject['_id']['study']] = {}
            studies[subject['_id']['study']]['subject'] = []
            studies[subject['_id']['study']]['max_day'] = 0

            #If there are more than 2, drop unsynced
            metadata = list(db.metadata.find(
                {'study' : subject['_id']['study']},
                {'_id' : True, 'collection' : True, 'synced' : True }))

            if len(metadata) > 1:
                for doc in metadata:
                    if doc['synced'] is False and 'collection' in doc:
                        db[doc['collection']].drop()
                    if doc['synced'] is False:
                        db.metadata.delete_many({'_id': doc['_id']})

        subject_metadata = col.defaultdict()
        subject_metadata['subject'] = subject['_id']['subject']
        subject_metadata['synced'] = subject['synced']
        subject_metadata['days'] = subject['days']
        subject_metadata['study'] = subject['_id']['study']

        studies[subject['_id']['study']]['max_day'] = studies[subject['_id']['study']]['max_day'] if (studies[subject['_id']['study']]['max_day'] >= subject['days'] ) else subject['days']

        studies[subject['_id']['study']]['subject'].append(subject_metadata)

    for study, subject in studies.iteritems():
        bulk_metadata = db.metadata.initialize_ordered_bulk_op()
        bulk_metadata.find({'study' : study}).upsert().update({'$set' :
            {
                'synced' : True,
                'subjects' : studies[study]['subject'],
                'days' : studies[study]['max_day']
            }
        })

        bulk_metadata.find({'study' : study, 'synced' : False}).remove()
        bulk_metadata.find({'study' : study }).update({'$set' : {'synced' : False}})

        try:
            bulk_metadata.execute()
        except BulkWriteError as e:
            logger.error(e)


# delete any items in toc that has not been synced for all
def clean_toc(db):
    logger.info('cleaning table of contents')
    out_of_sync_tocs = db.toc.find(
        {'synced' : False},
        {'_id' : False, 'collection' : True, 'path' : True })
    for doc in out_of_sync_tocs:
        db[doc['collection']].delete_many({
            'path' : doc['path']
        })

    bulk = db.toc.initialize_ordered_bulk_op()
    bulk.find({'synced' : False}).remove()
    bulk.find({}).update({'$set' : {'synced' : False}})
    try:
        bulk.execute()
    except BulkWriteError as e:
        logger.error(e)

# delete any items in toc that has not been synced for the study
def clean_toc_study(db, study):
    logger.info('cleaning table of contents for {STU}'.format(STU=study))
    out_of_sync_tocs = db.toc.find(
        {'study' : study, 'synced' : False},
        {'_id' : False, 'collection' : True, 'path' : True })
    for doc in out_of_sync_tocs:
        db[doc['collection']].delete_many({
            'path' : doc['path']
        })

    bulk = db.toc.initialize_ordered_bulk_op()
    bulk.find({'study' : study, 'synced' : False}).remove()
    bulk.find({'study' : study}).update({'$set' : {'synced' : False}})
    try:
        bulk.execute()
    except BulkWriteError as e:
        logger.error(e)

# delete any items in toc that has not been synced for the subject
def clean_toc_subject(db, study, subject):
    logger.info('cleaning table of contents for {SUB}'.format(SUB=subject))
    out_of_sync_tocs = db.toc.find(
        {'study' : study, 'subject' : subject, 'synced' : False},
        {'_id' : False, 'collection' : True, 'path' : True })
    for doc in out_of_sync_tocs:
        db[doc['collection']].delete_many({
            'path' : doc['path']
        })
    bulk = db.toc.initialize_ordered_bulk_op()
    bulk.find({'study' : study, 'subject' : subject, 'synced' : False}).remove()
    bulk.find({'study' : study, 'subject' : subject}).update({'$set' : {'synced' : False}})
    try:
        bulk.execute()
    except BulkWriteError as e:
        logger.error(e)

# Retrieve the file informtion and try import
def import_file(import_dir, file_name, db):
    file_path = os.path.join(import_dir, file_name)

    # Get file information
    try:
        file_info = dppylib.stat_file(import_dir, file_name, file_path)
        if not file_info:
            return

        dppylib.import_file(db, file_info)
    except Exception as e:
        logger.error(e)

# Check if a directory is valid, then return its child directories
def scan_dir(path):
    if os.path.isdir(path):
        try:
            return os.listdir(path)
        except Exception as e:
            logger.error(e)
            return []
    else:
        return []

if __name__ == '__main__':
    main()
