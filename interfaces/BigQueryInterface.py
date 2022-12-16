import logging
import os
import time
import json
from enum import Enum
from typing import Any, Dict

## pip module imports
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types
from google.cloud.bigquery_storage_v1 import writer
from google.cloud.exceptions import NotFound
from google.protobuf import descriptor_pb2
import google.api_core

## Local module imports
from schemas import BigQueryOgdLogRecord_pb2 # ProtoBuf 2 schema for our destination BigQuery table(s)
from utils import Logger
from interfaces import DataInterface

class BigQueryWriteInterface:

    def __init__(self, config, fq_table_id: str):
        
        self._config = config

        if "GITHUB_ACTIONS" not in os.environ:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self._config["CREDENTIALS_FILEPATH"]

        self.fq_table_id    = fq_table_id
        self.write_client: bigquery_storage_v1.BigQueryWriteClient = bigquery_storage_v1.BigQueryWriteClient()
        self.write_stream = None
        self.row_request_template = None
        self.append_rows_stream = None

    # Initialize an Append Rows Stream, along with the required Write Stream and request template for data rows
    def initAppendRowsStream(self, forceNewStream: bool = False) -> None:
        """
        :param forceNewStream: If we have an existing write stream, request template, or append rows stream, create a new instance of them (use this if the stream cannot be revived)
        :type forceNewStream: bool, optional
        """
        if forceNewStream or self.write_stream is None:
            self.write_stream = self.GetWriteStream()
            Logger.Log("New BQ write stream created with name: " + self.write_stream.name, logging.INFO)

        if forceNewStream or self.row_request_template is None:
            self.row_request_template = BigQueryWriteInterface.GetAppendRowsRequestTemplate(self.write_stream.name)

        if forceNewStream or self.append_rows_stream is None:
            self.append_rows_stream = writer.AppendRowsStream(self.write_client, self.row_request_template)

    # Send the given appendRowsRequest to BigQuery
    def SendAppendRowsRequest(self, numPreviousRequests: int, appendRowsRequest: google.cloud.bigquery_storage_v1.types.storage.AppendRowsRequest)\
         -> bigquery_storage_v1.types.AppendRowsResponse.AppendResult:

        # Initializes an Append Rows Stream, if it hasn't already been done
        self.initAppendRowsStream()
        
        # If this is the first request we're sending to the stream 
        # The stream will attempt an initial connection, 
        # which fails with some frequency. We'll retry the connection automatically.

        # If this is the first request we're sending
        if numPreviousRequests == 0:
            numRetries = 0
            maximumRetries = 100
            
            # While we heaven't reached our allowed number of retries
            while numRetries < maximumRetries:
                try:

                    if not numRetries == 0 :
                        Logger.Log("Initial BQ append rows stream connection failed, reattempt number " + str(numRetries) + " of " + str(maximumRetries), logging.WARN)

                    # response is an instance of append_result
                    # https://cloud.google.com/python/docs/reference/bigquerystorage/latest/google.cloud.bigquery_storage_v1.types.AppendRowsResponse.AppendResult
                    # { offset { value: 12345 } }
                    # value property will not be returned if the given offset in the request was zero
                    response = self.append_rows_stream.send(appendRowsRequest)
                    return response

                # Unknown is the exception type for a "404 Requested entity was not found" response
                except google.api_core.exceptions.Unknown as ex: 

                    Logger.Log("Send failed with exception type google.api_core.exceptions.Unknown", logging.WARN)
                    Logger.Log("Creating new stream", logging.WARN)

                    # Force a re-initialization of an append rows stream
                    self.initAppendRowsStream(True)

                    numRetries += 1
                    time.sleep(5)

                # StreamClosedError is the exception type for a "This manager has been closed and can not be used" response
                except google.cloud.bigquery_storage_v1.exceptions.StreamClosedError as ex: 

                    Logger.Log("Send failed with exception type google.cloud.bigquery_storage_v1.exceptions.StreamClosedError", logging.WARN)
                    Logger.Log("Creating new stream", logging.WARN)

                    # Force a re-initialization of an append rows stream
                    self.initAppendRowsStream(True)

                    numRetries += 1
                    time.sleep(5)
        
        return self.append_rows_stream.send(appendRowsRequest)

    # Close the append rows stream, finalize the write stream, commit the write stream
    def CloseFinalizeAndCommit(self) -> None:

        # Shutdown background threads and close the streaming connection.
        self.append_rows_stream.close()

        # A PENDING type stream must be "finalized" before being committed. No new
        # records can be written to the stream after this method has been called.
        self.write_client.finalize_write_stream(name=self.write_stream.name)

        # Commit the write stream
        batch_commit_write_streams_request = types.BatchCommitWriteStreamsRequest()
        batch_commit_write_streams_request.parent = self.getParentStringForFqTableId(self.fq_table_id)
        batch_commit_write_streams_request.write_streams = [self.write_stream.name]
        self.write_client.batch_commit_write_streams(batch_commit_write_streams_request)

    @staticmethod
    # Return a data row for inserting into a BigQuery table
    def AssembleSerializedRowData(mysqlRow: Dict[str, Any], formatType) -> None:
   
        if formatType == SourceDataRowFormatType.LOGGER_LOG:
            return BigQueryWriteInterface._assembleSerializedRowDataForLoggerLog(mysqlRow)
        elif formatType == SourceDataRowFormatType.OPEN_GAME_DATA:
            return BigQueryWriteInterface._assembleSerializedRowDataForOgd(mysqlRow)

        raise Exception("Unsupported source data format type: " + str(formatType))

    @staticmethod
    def _assembleSerializedRowDataForLoggerLog(mysqlRow):
        
        raise Exception("The logger.log data format is not yet supported")

        row = BigQueryOgdLogRecord_pb2.LogRecord()

        # Skipping the auto_increment primary key in mysqlRow[0] since it isn't useful
        row.session_id = mysqlRow['session_id']
        
        #row.user_id
        #row.user_data

         # Convert from MySQL timestamps in seconds to BigQuery timestamp in microseconds
        
        # We'll add in the milliseconds column (client_time_ms) from MySQL
        row.client_time = int(round(mysqlRow['client_time'].timestamp())) * 1000000 + mysqlRow['client_time_ms'] * 1000

        # server_time in MySQL is not UTC, it's local America/Chicago, but in the future might be logged as UTC
        # When we send this to BigQuery, BigQuery always assumes the timestamp is UTC
        row.server_time = int(round(mysqlRow['server_time'].timestamp())) * 1000000

        row.event_name = mysqlRow['event'] # event

        if mysqlRow['event_data_complex'] is None:
            row.event_data = "{}"
        else:
            try:
                event_custom_obj = json.loads(mysqlRow['event_data_complex'])
                row.event_data = mysqlRow['event_data_complex']
            except json.JSONDecodeError:
                row.event_data = "{}"
                Logger.Log("Unable to decode event_data_complex json string: " + str(mysqlRow['event_data_complex']) + " for sesson_id: " + str(mysqlRow['session_id']) + " session_n: " + str(mysqlRow['session_n']) + " id: " + str(mysqlRow['id']), logging.WARN)

        row.event_source = 'GAME' # TODO: Confirm if this should be GENERATED OR GAME
        # row.game_state
        row.app_version = mysqlRow['app_version']
        # row.app_branch
        row.log_version = 0
        row.event_sequence_index = mysqlRow['session_n'] # TODO: Confirm session_n is correct?
        row.remote_addr = mysqlRow['remote_addr']

        if mysqlRow[18] is not None:
            row.http_user_agent = mysqlRow['http_user_agent']

        return row.SerializeToString()

    @staticmethod
    def _assembleSerializedRowDataForOgd(mysqlRow: Dict[str, Any]) -> None:

        row = BigQueryOgdLogRecord_pb2.LogRecord()

        # Skipping the auto_increment primary key in mysqlRow[0] since it isn't useful
        row.session_id = mysqlRow['session_id']
        row.user_id = mysqlRow['user_id']

        if mysqlRow['user_data'] is None or mysqlRow['user_data'] == "":
            pass
            #row.user_data = None
        else:
            try:
                user_data_obj = json.loads(mysqlRow['user_data'])
                row.user_data = mysqlRow['user_data']
            except json.JSONDecodeError:
                #row.user_data = None
                Logger.Log("Unable to decode user_data json string: " + str(mysqlRow['user_data']) + " for sesson_id: " + str(mysqlRow['session_id']) + " event_sequence_index: "\
                     + str(mysqlRow['event_sequence_index']) + " id: " + str(mysqlRow['id']), logging.WARN)

        # client_time is NOT NULL in MySQL, however it can be 0000-00-00 which is cast to None
        # so we have to check it here
        if not mysqlRow['client_time'] is None:
            # Convert from MySQL timestamps in seconds to BigQuery timestamp in microseconds
            # We'll add in the milliseconds column (client_time_ms) from MySQL
            row.client_time = int(round(mysqlRow['client_time'].timestamp())) * 1000000 + mysqlRow['client_time_ms'] * 1000

        # casting datetime.timedelta type to integer number of seconds because
        # BigQuery's TIME type cannot store negative values
        # and casting to string produces day-based offsets for negative values e.g. "-1 day, 19:00:00" for "-06:00:00"
        row.client_offset = round(mysqlRow['client_offset'].total_seconds()) 

        # server_time in MySQL is not UTC, it's local America/Chicago, but in the future might be logged as UTC
        # When we send this to BigQuery, BigQuery always assumes the timestamp is UTC
        row.server_time = int(round(mysqlRow['server_time'].timestamp())) * 1000000

        row.event_name = mysqlRow['event_name']

        if mysqlRow['event_data'] is None:
            row.event_data = "{}"
        else:
            try:
                event_obj = json.loads(mysqlRow['event_data'])
                row.event_data = mysqlRow['event_data']
            except json.JSONDecodeError:
                row.event_data = "{}"
                Logger.Log("Unable to decode event_data json string: " + str(mysqlRow['event_data']) + " for sesson_id: " + str(mysqlRow['session_id'])\
                     + " event_sequence_index: " + str(mysqlRow['event_sequence_index']) + " id: " + str(mysqlRow['id']), logging.WARN)

        row.event_source = mysqlRow['event_source']

        if mysqlRow['game_state'] is None or mysqlRow['game_state'] == "":
            # row.game_state = None
            pass
        else:
            try:
                game_state_obj = json.loads(mysqlRow['game_state'])
                row.game_state = mysqlRow['game_state']
            except json.JSONDecodeError:
                # row.game_state = None
                Logger.Log("Unable to decode game_state json string: " + str(mysqlRow['game_state']) + " for sesson_id: " + str(mysqlRow['session_id'])\
                     + " event_sequence_index: " + str(mysqlRow['event_sequence_index']) + " id: " + str(mysqlRow['id']), logging.WARN)

        row.app_version = mysqlRow['app_version']

        if mysqlRow['app_branch'] is not None:
            row.app_branch = mysqlRow['app_branch']
        row.log_version = mysqlRow['log_version']
        row.event_sequence_index = mysqlRow['event_sequence_index']
        row.remote_addr = mysqlRow['remote_addr']

        if mysqlRow['http_user_agent'] is not None:
            row.http_user_agent = mysqlRow['http_user_agent']

        return row.SerializeToString()

    def GetWriteStream(self):

        #aqualab-57f88.nightly_dumps_testing
        #parent = write_client.table_path(project_id, dataset_id, table_id)

        parentPath = self.getParentStringForFqTableId(self.fq_table_id)
        
        #Logger.Log(parentPath, logging.INFO)
        
        writeStream = types.WriteStream()

        # When creating the stream, choose the type. Use the PENDING type to wait
        # until the stream is committed before it is visible. See:
        # https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.WriteStream.Type
        writeStream.type_ = types.WriteStream.Type.PENDING
        writeStream = self.write_client.create_write_stream(
            parent=parentPath, write_stream=writeStream
        )

        return writeStream
    
    def getParentStringForFqTableId(self, fqTableId):
        
        tablePathChunks = fqTableId.split('.')

        if len(tablePathChunks) != 3:
            raise Exception("Unexpected format of fully qualified table id. Expected format is project_id.dataset_id.table_id")
        
        # parent is a string in the format of projects/{project}/datasets/{dataset}/tables/{table}
        parentPath = self.write_client.table_path(tablePathChunks[0], tablePathChunks[1], tablePathChunks[2])

        return parentPath

    @staticmethod
    def GetAppendRowsRequestTemplate(stream_name):
            
        # Create a template with fields needed for the first request.
        request_template = types.AppendRowsRequest()

        # The initial request must contain the stream name.
        request_template.write_stream = stream_name

        # So that BigQuery knows how to parse the serialized_rows, generate a
        # protocol buffer representation of your message descriptor.
        proto_schema = types.ProtoSchema()
        proto_descriptor = descriptor_pb2.DescriptorProto()
        BigQueryOgdLogRecord_pb2.LogRecord.DESCRIPTOR.CopyToProto(proto_descriptor)
        proto_schema.proto_descriptor = proto_descriptor
        proto_data = types.AppendRowsRequest.ProtoData()
        proto_data.writer_schema = proto_schema
        request_template.proto_rows = proto_data

        return request_template
    
    @staticmethod
    def GetAppendRowsRequest(protoRows, offset):
        # Set an offset to allow resuming this stream if the connection breaks.
        # Keep track of which requests the server has acknowledged and resume the
        # stream at the first non-acknowledged message. If the server has already
        # processed a message with that offset, it will return an ALREADY_EXISTS
        # error, which can be safely ignored.
        #
        # The first request must always have an offset of 0.
        request = types.AppendRowsRequest()
        request.offset = offset
        proto_data = types.AppendRowsRequest.ProtoData()
        proto_data.rows = protoRows
        request.proto_rows = proto_data

        return request

# Enum representing the different source database schemas we might pulling from
class SourceDataRowFormatType(Enum):
    LOGGER_LOG = 'LOGGER_LOG'
    OPEN_GAME_DATA = 'OPEN_GAME_DATA'


class BigQueryInterface:

    def __init__(self, config):

        self._config = config

        if "GITHUB_ACTIONS" not in os.environ:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self._config["CREDENTIALS_FILEPATH"]

        self._client: bigquery.Client = bigquery.Client()

    def TableExists(self, fqTableId: str) -> bool:
        try:
            self._client.get_table(fqTableId)
            return True
        except NotFound:
            return False

    def DeleteTable(self, fqTableId: str) -> None:
        self._client.delete_table(fqTableId)
        Logger.Log("Deleted table: " + fqTableId, logging.INFO)

    def CreateTable(self, fqTableId: str, schema: Any) -> None:
        bigquery_table = bigquery.Table(fqTableId, schema)
        self._client.create_table(bigquery_table)
        Logger.Log("Created table: " + fqTableId, logging.INFO)

    def GetTableCount(self, fqTableId: str) -> int:
        query = "SELECT COUNT(*) mycount FROM `" + fqTableId + "`"
        job = self._client.query(query)

        for row in job:
            return row["mycount"] # row values can be accessed by index [0] or field name


