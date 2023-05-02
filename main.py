# Standard module imports
import logging
import os
import sys
from argparse import ArgumentParser, Namespace

# Local module imports
from services.OpenGameDataLogSyncer import OpenGameDataLogSyncer 
from utils import Logger

from config.config import settings as script_settings

parser = ArgumentParser(add_help=False)
parser.add_argument("game", type=str.upper,
                    help="The game to use with the given command.")
parser.add_argument("-m", "--max_days", type=int, required=False, default=100,
                    help="Tell the program the maximum number of days to sync.")

args : Namespace = parser.parse_args()

Logger.Log(f"Begin MySQL to BigQuery sync job on {args.game}, up to {args.max_days} days.", logging.INFO)

logSyncService = OpenGameDataLogSyncer(script_settings)
numDaysSynced = logSyncService.SyncAll(maxDaysToSync=args.max_days) 

Logger.Log(f"Successfully synced {numDaysSynced} / {args.max_days} days of logs from MySQL to BigQuery", logging.INFO)


Logger.Log("End MySQL to BigQuery sync job", logging.INFO)

sys.exit(0)

