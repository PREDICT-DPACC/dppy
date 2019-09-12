import os
import re
import logging
import hashlib
import uuid
import mimetypes as mt
from datetime import datetime
from urllib import quote

from dppylib.tools import database as dbtools
from dppylib.tools import reader

TIME_UNITS = {
    'day' : 'days',
    'hr' : 'hours'
}

_UNITS = '|'.join(TIME_UNITS.keys())
_EXTENSION = '.csv'

FILE_REGEX = re.compile(r'(?P<study>\w+)\-(?P<subject>\w+)\-(?P<assessment>\w+)\-(?P<units>{UNITS})(?P<start>[+-]?\d+(?:\.\d+)?)to(?P<end>[+-]?\d+(?:\.\d+)?)(?P<extension>{EXTENSION})'.format(UNITS=_UNITS, EXTENSION=_EXTENSION))
FILE_SUB =  re.compile(r'(\w+\-\w+\-\w+\-{UNITS})[+-]?\d+(?:\.\d+)?to[+-]?\d+(?:\.\d+)?(.*)'.format(UNITS=_UNITS))
METADATA_REGEX = re.compile(r'(?P<study>\w+)\_metadata(?P<extension>{EXTENSION})'.format(EXTENSION='.csv'))

logger = logging.getLogger(__name__)

# Verify if a file is DPdash-compatible file, and return file info if so.
def stat_file(import_dir, file_name, file_path):
    file_info = match_file(file_name, import_dir)
    if not file_info:
        return None

    filetype,encoding = guess_type(file_info['extension'])
    if not os.path.exists(file_path):
        return None

    file_stat = os.stat(file_path)
    file_info.update({
        'path' : file_path,
        'filetype' : filetype,
        'encoding' : encoding,
        'basename' : file_name,
        'dirname' : import_dir,
        'dirty' : True,
        'synced' : False,
        'mtime' : file_stat.st_mtime,
        'size' : file_stat.st_size,
        'uid' : file_stat.st_uid,
        'gid' : file_stat.st_gid,
        'mode' : file_stat.st_mode
    })

    return file_info

def import_file(db, file_info):
    if file_info['role'] == 'data':
        collection = db['toc']
    elif file_info['role'] == 'metadata':
        collection = db['metadata']
    else:
        logger.error('{FILE} is not compatible with DPdash. Exiting import.'.format(FILE=file_info['path']))
        return

    diff_files(db, collection, file_info)

# Match the file info with the record stored in the database
def diff_files(db, collection, file_info):
    file_path = file_info['path']
    db_data = collection.find_one({ 'path' : file_path })
    if not db_data:
        logger.info('{FILE} does not exist in the database. Importing.'.format(FILE=file_path))
        import_data(db, collection, file_info)
    else:
        if db_data['mtime'] != file_info['mtime'] or db_data['size'] != file_info['size']:
            logger.info('{FILE} has been modified. Re-importing.'.format(FILE=file_path))
            dbtools.remove_doc(db, collection, db_data, file_info['role'])
            import_data(db, collection, file_info)
        else:
            logger.info('Database already has {FILE}. Skipping.'.format(FILE=file_path))
            logged = log_success(collection, db_data['_id'])
            if logged == 0:
                logger.info('Journaling complete for {FILE}'.format(FILE=file_info['path']))

# Import data into the database
def import_data(db, ref_collection, file_info):
    if file_info['role'] == 'metadata':
        file_info.update({'collection': str(uuid.uuid4())})
    ref_id = insert_reference(ref_collection, file_info)
    if ref_id is None:
        logger.error('Unable to import {FILE}'.format(FILE=file_info['path']))
        return

    inserted = insert_data(db, file_info)
    if inserted == 0:
        logger.info('Import success for {FILE}'.format(FILE=file_info['path']))

        logged = log_success(ref_collection, ref_id)
        if logged == 0:
            logger.info('Journaling complete for {FILE}'.format(FILE=file_info['path']))

# Mark the sync as successful
def log_success(ref_collection, ref_id):
    update_ref = {
        '$set' : {
            'dirty': False,
            'synced' : True,
            'updated' : datetime.utcnow()
        }
    }

    try:
        ref_collection.update({
            '_id' : ref_id
        }, update_ref)
        return 0
    except Exception as e:
        logger.error(e)
        return 1

# Insert the reference doc, returns the inserted id
def insert_reference(collection, reference):
    try:
        ref_id = collection.insert_one(reference).inserted_id
        return ref_id
    except Exception as e:
        logger.error(e)
        return None

# Insert the data
def insert_data(db, file_info):
    try:
        # Import data
        data_blob = []
        import_collection = db[file_info['collection']]
        for chunk in reader.read_csv(file_info['path']):
            if len(chunk) > 0:
                if file_info['role'] != 'metadata':
                    chunk_columns = sanitize_columns(chunk.columns.values.tolist())
                    chunk.columns = chunk_columns
                chunk['path'] = file_info['path']
                data_blob.extend(chunk.to_dict('records'))

                if len(data_blob) >= 100000:
                    import_collection.insert_many(data_blob, False)
                    data_blob = []
        if data_blob:
            import_collection.insert_many(data_blob, False)
        return 0
    except Exception as e:
        logger.error(e)
        logger.error('Unable to import {FILE}'.format(FILE=file_info['path']))
        return 1

# Rename columns to encode special characters
def sanitize_columns(columns):
    new_columns = []
    for column in columns:
        new_column = quote(unicode(column).encode('utf-8'), safe='~()*!.\'').replace('.', '%2E')
        new_columns.append(new_column)

    return new_columns

# Match the filename to distinguish data from metadata files
def match_file(file_name, sub_dir):
    matched_file = FILE_REGEX.match(file_name)
    if not matched_file:
        matched_metadata = METADATA_REGEX.match(file_name)
        if not matched_metadata:
            return None
        else:
            return scan_metadata(matched_metadata, file_name, sub_dir)
    else:
        return scan_data(matched_file, file_name, sub_dir)

# Return file_info for the metadata
def scan_metadata(match, file_name, sub_dir):
    file_info = match.groupdict()

    file_info.update({
        'glob' : os.path.join(sub_dir, file_name),
        'role' : 'metadata'
    })

    return file_info

# Return file_info for the data
def scan_data(match, file_name, sub_dir):
    file_info = match.groupdict()

    m = hashlib.sha256('{STUDY}{SUBJECT}{ASSESSMENT}'.format(
        STUDY= file_info['study'],
        SUBJECT=file_info['subject'],
        ASSESSMENT=file_info['assessment'] 
    ))

    file_info.update({
        'collection': m.hexdigest(),
        'subject' : file_info['subject'],
        'assessment' : file_info['assessment'],
        'glob' : os.path.join(sub_dir, FILE_SUB.sub('\\1*\\2', file_name)),
        'time_units' : str(file_info['units']),
        'time_start' : int(file_info['start']),
        'time_end' : int(file_info['end']),
        'role' : 'data'
    })

    return file_info

# get mime type and encoding
def guess_type(extension):
    return mt.guess_type('file{}'.format(extension))

class StatError(Exception):
    pass

class ParserError(Exception):
    pass
