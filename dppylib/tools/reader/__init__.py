import pandas as pd
import logging

logger = logging.getLogger(__name__)

# Read in the file and yield the dataframe chunk
def read_csv(file_path):
    try:
        tfr = pd.read_csv(file_path, memory_map=True, keep_default_na=False, chunksize=1, engine='c', skipinitialspace=True)
        for df in tfr:
            yield df
    except pd.io.common.EmptyDataError as e:
        logger.error(e)
    except Exception as e:
        logger.error(e)
