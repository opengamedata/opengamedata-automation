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
range_parser = ArgumentParser(add_help=False, parents=[parser])
# range_parser.add_argument("start_date", nargs="?", default=None,
#                     help="The starting date of an export range in MM/DD/YYYY format (defaults to today).")
# range_parser.add_argument("end_date", nargs="?", default=None,
#                     help="The ending date of an export range in MM/DD/YYYY format (defaults to today).")
range_parser.add_argument("-M", "--max_days", default="",
                    help="Tell the program the maximum number of days to sync.")
Logger.Log("Begin MySQL to BigQuery sync job", logging.INFO)

args : Namespace = parser.parse_args()

logSyncService = OpenGameDataLogSyncer(script_settings)
numDaysSynced = logSyncService.SyncAll(maxDaysToSync=args.max_days or 100) 

Logger.Log(str(numDaysSynced) + " days of logs were synced from MySQL to BigQuery", logging.INFO)


Logger.Log("End MySQL to BigQuery sync job", logging.INFO)

sys.exit(0)

