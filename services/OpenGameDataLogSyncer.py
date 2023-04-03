# Standard module imports
import logging
import sys
from datetime import datetime, time
from typing import Any, Dict, List, Tuple, Optional

# Local module imports
from interfaces.BigQueryInterface import BigQueryInterface, BigQueryWriteInterface, SourceDataRowFormatType
from interfaces.MySQLInterface import MySQLInterface
from schemas import BigQueryLogTableSchema # Specifies the list of columns for our BigQuery table schema - used for table creation calls
from utils import Logger

# pip module imports
from google.cloud.bigquery_storage_v1 import types

# This class facilitates the migration of log entries from MySQL to BigQuery
class OpenGameDataLogSyncer:
    
    def __init__(self, config:Dict[str,Any]):
        self._config  = config
        self._mysqlInterface: Optional[MySQLInterface] = None

    # Sync all log entries for a specified OGD log table, batched by date

    # Workflow:
    # 1. Get date of the oldest unmigrated row in MySQL table (oldest row where synced = 0)
    # 2. Create a BigQuery table for that date if a table doesn't already exist
    # 3. Get all MySQL rows for that date
    # 4. Send all MySQL rows for that date to BigQuery and commit
    # 5. Set the synced field to 1 for all MySQL rows for that date
    #    There is a risk that new log entries for that date will be added to MySQL between steps 3 and 5, but this should not
    #    happen. The alternative would be storing potentially millions of ids and using that as the WHERE criteria for the UPDATE, 
    #    which could be a performance nightmare.
    # 6. Go back to Step 1. 

    def SyncAll(self, maxDaysToSync:int = 100) -> int:
        """Function to synchronize as much data as possible to long-term storage.
        Uses a limit on the number of days to synchronize, to ensure we don't have the process run an overlong time.

        :param maxDaysToSync: The maximum number of days-worth of data to synchronize, defaults to 100
        :type maxDaysToSync: int, optional
        :return: The number of days synchronized to long-term storage.
        :rtype: int
        """
        
        # Establish a MySQL connection, set long timeouts for our session
        self._mysqlInterface = MySQLInterface(self._config)
        self._mysqlInterface.SetSessionVariables()
        
        # Get the oldest date for a log entry that we're able to sync
        dateToMigrate = self._mysqlInterface.GetOldestUnmigratedDate()

        if dateToMigrate is None:
            Logger.Log('No MySQL entries require migration to BigQuery', logging.INFO)
            return 0

        # Number of days we've sync'd for this execution of SyncAll()
        numDaysSynced = 0

        # While we still have a day with unmigrated rows, and haven't reached our limit of days
        while dateToMigrate is not None and numDaysSynced < maxDaysToSync:

            Logger.Log("Oldest unsynced log entry found for date: " + str(dateToMigrate))
            self.SyncDate(dateToMigrate)

            # Get the next oldest date with unmigrated rows
            dateToMigrate = self._mysqlInterface.GetOldestUnmigratedDate()

            numDaysSynced += 1
            
        return numDaysSynced

    def SyncDate(self, dateToMigrate:datetime.date) -> None:

        # 1. For a given day fetch a cursor for all the day's log rows in MySQL
        # 2. Create a BigQuery table following the naming convention {TableBasename}_YYYYMMDD if one doesn't exist
        # 3. Create a "PENDING" mode BigQuery write stream
        #      Pending mode: Records are buffered in a pending state until you commit the stream. When you commit a stream, 
        #      all of the pending data becomes available for reading. The commit is an atomic operation. Use this mode for 
        #      batch workloads, as an alternative to BigQuery load jobs.
        #      https://cloud.google.com/bigquery/docs/write-api#application-created_streams
        # 3. Iterate over the MySQL cursor, creating requests containing batches of rows. Send each request to the stream when it nears the 10 MB limit.
        # 4. Close, write, and commit the BigQuery write stream 
        # 5. Close the MySQL cursor

        mysqlTablePath = f"{self._config['MYSQL_CONFIG']['DB_NAME']}.{self._config['MYSQL_CONFIG']['DB_TABLE']}"

        bqFqTableId = f"{self._config['BIGQUERY_CONFIG']['PROJECT_ID']}.{self._config['BIGQUERY_CONFIG']['DATASET_ID']}\
                       .{self._config['BIGQUERY_CONFIG']['TABLE_BASENAME']}_{dateToMigrate.strftime('%Y%m%d')}"


        Logger.Log("Begin syncing log entries for: " + str(dateToMigrate) + " from MySQL: " + mysqlTablePath + " to BigQuery: " + bqFqTableId)

        # Get the number of migrated & unmigrated source rows for the given date
        if self._mysqlInterface is not None:
            migrationStatusCounts = self._mysqlInterface.GetMigrationStatusCountsByDate(dateToMigrate)

            Logger.Log(f'For: {str(dateToMigrate)} Found {str(migrationStatusCounts[0])} MySQL rows marked as requiring migration', logging.INFO)
            Logger.Log(f'For: {str(dateToMigrate)} Found {str(migrationStatusCounts[1])} MySQL rows marked as already migrated', logging.INFO)

            bqInterface = BigQueryInterface(self._config["BIGQUERY_CONFIG"])

            numBqTableEntriesBefore = 0

            # If the desination table exists
            if bqInterface.TableExists(bqFqTableId):

                # Get a count of existing entries
                numBqTableEntriesBefore = bqInterface.GetTableCount(bqFqTableId)
                Logger.Log(f"For: {str(dateToMigrate)} Found {str(numBqTableEntriesBefore)} existing BigQuery rows.", logging.INFO)
            else:
                # Create the table
                bqInterface.CreateTable(bqFqTableId, BigQueryLogTableSchema.schema)

            # Get a write interface instance
            bqWriteInterface = BigQueryWriteInterface(self._config["BIGQUERY_CONFIG"], bqFqTableId)

            # Get a cursor for all source log entries on the given day
            logEntriesCursor = self._mysqlInterface.GetLogEntriesByDate(dateToMigrate, SourceDataRowFormatType[self._config["MYSQL_CONFIG"]["SOURCE_TYPE"]])
                    
            # Rows are dictionaries keyed by column name
            mysqlRow = logEntriesCursor.fetchone()

            numRequests = 0
            numExportedRows = 0
            numRowsInRequest = 0
            maxRequestSizeInBytes = 10000000

            # Explicitly set DEBUG log level to see the caught exceptions and debugging output from the Google libraries and API calls
            if self._config["DEBUG_LEVEL"] == "DEBUG":
                logging.basicConfig(level=logging.DEBUG)

            estimatedRequestSize = 0
            bqAppendRowsResponses = []

            protoRows = types.ProtoRows()
            bqAppendRowsRequest = None

            # Offset of the first request should be zero
            # Offset of subsequent requests should be equal to the number of rows we've previously sent
            offset = 0

            while mysqlRow is not None:

                serializedRowData = BigQueryWriteInterface.AssembleSerializedRowData(mysqlRow, SourceDataRowFormatType[self._config["MYSQL_CONFIG"]["SOURCE_TYPE"]])
                sizeOfserializedRowData = sys.getsizeof(serializedRowData)
                
                # If adding this row to the request would push it over the max request limit of 10 MB
                # we'll send the request and start a new request before adding the row
                if sizeOfserializedRowData + estimatedRequestSize >= maxRequestSizeInBytes:

                    Logger.Log(f"Estimated request size: {str(estimatedRequestSize)} bytes", logging.DEBUG)
                    Logger.Log(f"Creating append rows request number: {str(numRequests + 1)} containing {str(numRowsInRequest)} rows with offset: {str(offset)} and sending", logging.INFO)

                    # The size of a single AppendRowsRequest must be less than 10 MB in size
                    # https://cloud.google.com/python/docs/reference/bigquerystorage/latest/google.cloud.bigquery_storage_v1.client.BigQueryWriteClient
                    bqAppendRowsRequest = BigQueryWriteInterface.GetAppendRowsRequest(protoRows, offset)

                    # Send the request via the stream, store the repsonse in the array of responses
                    bqAppendRowsResponses.append(bqWriteInterface.SendAppendRowsRequest(numRequests, bqAppendRowsRequest))
                    
                    # TODO: Could try deferring the response logging until all the requests have been sent. I believe the sends are supposed to be asynchronous, so .result() might wait until they resolve.
                    # Not seeing much difference with days that don't have millions of log entries
                    Logger.Log(f"For request number: {str(numRequests + 1)} with offset: {str(offset)} Request response result: {str(bqAppendRowsResponses[len(bqAppendRowsResponses) - 1].result())}", logging.DEBUG)
                    
                    # Update our offset
                    offset += numRowsInRequest
                    
                    # Reset our request size and number of rows in the request, increase our offset
                    estimatedRequestSize = 0
                    numRowsInRequest = 0

                    numRequests += 1

            
                # If we're starting a new request
                if numRowsInRequest == 0:
                    # Create a batch of row data by appending proto2 serialized bytes to the serialized_rows repeated field.
                    protoRows = types.ProtoRows()

                # Add this row to the request
                protoRows.serialized_rows.append(BigQueryWriteInterface.AssembleSerializedRowData(mysqlRow, SourceDataRowFormatType[self._config["MYSQL_CONFIG"]["SOURCE_TYPE"]]))
                numRowsInRequest += 1
                numExportedRows += 1
                estimatedRequestSize += sizeOfserializedRowData

                #Logger.Log(mysqlRow, logging.INFO)

                # Get the next row
                mysqlRow = self._mysqlInterface._db_cursor.fetchone()

            self._mysqlInterface._db_cursor.close()

            # If we have a request with rows that hasn't been sent yet
            if not numRowsInRequest == 0:

                # Send it now
                Logger.Log(f"Estimated request size: {str(estimatedRequestSize)} bytes", logging.DEBUG)
                Logger.Log(f"Creating final append rows request number: {str(numRequests + 1)} containing {str(numRowsInRequest)} rows with offset: {str(offset)} and sending", logging.INFO)
                bqAppendRowsRequest = BigQueryWriteInterface.GetAppendRowsRequest(protoRows, offset)

                bqAppendRowsResponses.append(bqWriteInterface.SendAppendRowsRequest(numRequests, bqAppendRowsRequest))
                Logger.Log(f"For request number: {str(numRequests + 1)} with offset: {str(offset)} Request response result: {str(bqAppendRowsResponses[len(bqAppendRowsResponses) - 1].result())}", logging.DEBUG)

                numRequests += 1

            bqWriteInterface.CloseFinalizeAndCommit()

            Logger.Log(f"{str(numExportedRows)} MySQL log entries sent to: {bqFqTableId}", logging.INFO)

            numBqTableEntriesAfter = bqInterface.GetTableCount(bqFqTableId)
            Logger.Log(f"For: {str(dateToMigrate)} found {str(numBqTableEntriesAfter)} BigQuery rows", logging.INFO)

            numRowsConfirmedInserted = numBqTableEntriesAfter - numBqTableEntriesBefore

            if numRowsConfirmedInserted < migrationStatusCounts[0]:
                Logger.Log(f"Expected to migrate {str(migrationStatusCounts[0])} rows from MySQL, but only {str(numRowsConfirmedInserted)} new rows found in BigQuery", logging.FATAL)
                raise Exception("Missing expected log entries in BigQuery")
                sys.exit(1) # This is unrecoverable, don't allow catching or continuing

            self._mysqlInterface.MarkLogEntriesAsSynced(dateToMigrate)        
            Logger.Log(f"MySQL entries for {str(dateToMigrate)} have all been marked as synced")

            
            Logger.Log(f"Completed syncing log entries for: {str(dateToMigrate)}")
        else:
            Logger.Log(f"Could not sync log entries for {str(dateToMigrate)}, the MySQLInterface was None!")