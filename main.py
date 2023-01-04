# Standard module imports
import logging
import os
import sys

# Local module imports
from services.OpenGameDataLogSyncer import OpenGameDataLogSyncer 
from utils import Logger

from config.config import settings as script_settings

Logger.Log("Begin MySQL to BigQuery sync job", logging.INFO)

logSyncService = OpenGameDataLogSyncer(script_settings)
numDaysSynced = logSyncService.SyncAll() 

Logger.Log(str(numDaysSynced) + " days of logs were synced from MySQL to BigQuery", logging.INFO)


Logger.Log("End MySQL to BigQuery sync job", logging.INFO)

sys.exit(0)

