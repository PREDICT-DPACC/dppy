import logging

logger = logging.getLogger(__name__)

def sanitize(db):
    dirty_files = db.toc.find({
        'dirty' : True
    })
    dirty_metadata = db.metadata.find({
        'dirty' : True
    })
    for doc in dirty_files:
        logger.info('{FILE} is outdated. Deleting from the database.'.format(FILE=doc['path']))
        remove_doc(db, db['toc'], doc, 'data')

    for doc in dirty_metadata:
        logger.info('{FILE} is outdated. Deleting from the database.'.format(FILE=doc['path']))
        remove_doc(db, db['metadata'], doc, 'metadata')

def remove_doc(db, collection, doc, role):
    try:
        collection.delete_many({
            '_id' : doc['_id']
        })
        if role == 'metadata':
            db[doc['collection']].drop()
        else:
            
            db[doc['collection']].delete_many({
                'path' : doc['path']
            })
        return 0
    except Exception as e:
        logger.error(e)
        logger.error('Could not remove {FILE} from the database.'.format(FILE=doc['path']))
        return 1
