# import libraries
from mysql.connector import connection, cursor
import logging
import sshtunnel
import sys
import traceback
from datetime import datetime, date, time, timedelta
from typing import Any, Dict, List, Tuple, Optional

# import locals
from interfaces.DataInterface import DataInterface
from interfaces.BigQueryInterface import SourceDataRowFormatType
from utils import Logger


## Dumb struct to collect data used to establish a connection to a SQL database.
class SQLLogin:
    def __init__(self, host: str, port: int, db_name: str, user: str, pword: str):
        self.host    = host
        self.port    = port
        self.db_name = db_name
        self.user    = user
        self.pword   = pword
 
## Dumb struct to collect data used to establish a connection over ssh.
class SSHLogin:
    def __init__(self, host: str, port: int, user: str, pword: str):
        self.host    = host
        self.port    = port
        self.user    = user
        self.pword   = pword

## @class SQL
#  A utility class containing some functions to assist in retrieving from a database.
#  Specifically, helps to connect to a database, make selections, and provides
#  a nicely formatted 500 error message.
class SQL:

    # Function to set up a connection to a database, via an ssh tunnel if available.
    @staticmethod
    def ConnectDB(db_settings:Dict[str,Any], ssh_settings:Optional[Dict[str,Any]]=None) -> Tuple[Optional[sshtunnel.SSHTunnelForwarder], Optional[connection.MySQLConnection]]:
        """
        Function to set up a connection to a database, via an ssh tunnel if available.

        :param db_settings: A dictionary mapping names of database parameters to values.
        :type db_settings: Dict[str,Any]
        :param ssh_settings: A dictionary mapping names of ssh parameters to values, or None if no ssh connection is desired., defaults to None
        :type ssh_settings: Optional[Dict[str,Any]], optional
        :return: A tuple consisting of the tunnel and database connection, respectively.
        :rtype: Tuple[Optional[sshtunnel.SSHTunnelForwarder], Optional[connection.MySQLConnection]]
        """
        tunnel  : Optional[sshtunnel.SSHTunnelForwarder] = None
        db_conn : Optional[connection.MySQLConnection]   = None
        # Load settings, set up consts.
        DB_HOST = db_settings['DB_HOST']
        DB_NAME = db_settings["DB_NAME"]
        DB_PORT = int(db_settings['DB_PORT'])
        DB_USER = db_settings['DB_USER']
        DB_PW = db_settings['DB_PW']
        sql_login = SQLLogin(host=DB_HOST, port=DB_PORT, db_name=DB_NAME, user=DB_USER, pword=DB_PW)
        Logger.Log("Preparing database connection...", logging.INFO)
        if ssh_settings is not None:
            SSH_USER = ssh_settings['SSH_USER']
            SSH_PW   = ssh_settings['SSH_PW']
            SSH_HOST = ssh_settings['SSH_HOST']
            SSH_PORT = ssh_settings['SSH_PORT']
            if (SSH_HOST != "" and SSH_USER != "" and SSH_PW != ""):
                ssh_login = SSHLogin(host=SSH_HOST, port=SSH_PORT, user=SSH_USER, pword=SSH_PW)
                tunnel,db_conn = SQL._connectToMySQLviaSSH(sql=sql_login, ssh=ssh_login)
            else:
                db_conn = SQL._connectToMySQL(login=sql_login)
                tunnel = None
        else:
            db_conn = SQL._connectToMySQL(login=sql_login)
            tunnel = None
        Logger.Log("Done preparing database connection.", logging.INFO)
        return (tunnel, db_conn)

    # Function to help connect to a mySQL server.
    @staticmethod
    def _connectToMySQL(login:SQLLogin) -> Optional[connection.MySQLConnection]:
        """Function to help connect to a mySQL server.

        Simply tries to make a connection, and prints an error in case of failure.
        :param login: A SQLLogin object with the data needed to log into MySQL.
        :type login: SQLLogin
        :return: If successful, a MySQLConnection object, otherwise None.
        :rtype: Optional[connection.MySQLConnection]
        """
        try:
            db_conn = connection.MySQLConnection(host     = login.host,    port    = login.port,
                                                 user     = login.user,    password= login.pword,
                                                 database = login.db_name, charset = 'utf8')
            Logger.Log(f"Connected to SQL (no SSH) at {login.host}:{login.port}/{login.db_name}, {login.user}", logging.DEBUG)
            return db_conn
        #except MySQLdb.connections.Error as err:
        except Exception as err:
            msg = f"""Could not connect to the MySql database.
            Login info: host={login.host}, port={login.port} w/type={type(login.port)}, db={login.db_name}, user={login.user}.
            Full error: {type(err)} {str(err)}"""
            Logger.Log(msg, logging.ERROR)
            traceback.print_tb(err.__traceback__)
            return None

    ## Function to help connect to a mySQL server over SSH.
    @staticmethod
    def _connectToMySQLviaSSH(sql:SQLLogin, ssh:SSHLogin) -> Tuple[Optional[sshtunnel.SSHTunnelForwarder], Optional[connection.MySQLConnection]]:
        """Function to help connect to a mySQL server over SSH.

        Simply tries to make a connection, and prints an error in case of failure.
        :param sql: A SQLLogin object with the data needed to log into MySQL.
        :type sql: SQLLogin
        :param ssh: An SSHLogin object with the data needed to log into MySQL.
        :type ssh: SSHLogin
        :return: An open connection to the database if successful, otherwise None.
        :rtype: Tuple[Optional[sshtunnel.SSHTunnelForwarder], Optional[connection.MySQLConnection]]
        """
        tunnel    : Optional[sshtunnel.SSHTunnelForwarder] = None
        db_conn   : Optional[connection.MySQLConnection] = None
        MAX_TRIES : int = 5
        tries : int = 0
        connected_ssh : bool = False

        # First, connect to SSH
        while connected_ssh == False and tries < MAX_TRIES:
            if tries > 0:
                Logger.Log("Re-attempting to connect to SSH.", logging.INFO)
            try:
                tunnel = sshtunnel.SSHTunnelForwarder(
                    (ssh.host, ssh.port), ssh_username=ssh.user, ssh_password=ssh.pword,
                    remote_bind_address=(sql.host, sql.port), logger=Logger.std_logger
                )
                tunnel.start()
                connected_ssh = True
                Logger.Log(f"Connected to SSH at {ssh.host}:{ssh.port}, {ssh.user}", logging.DEBUG)
            except Exception as err:
                msg = f"Could not connect to the SSH: {type(err)} {str(err)}"
                Logger.Log(msg, logging.ERROR)
                Logger.Print(msg, logging.ERROR)
                traceback.print_tb(err.__traceback__)
                tries = tries + 1
        if connected_ssh == True and tunnel is not None:
            # Then, connect to MySQL
            try:
                db_conn = connection.MySQLConnection(host     = sql.host,    port    = tunnel.local_bind_port,
                                                     user     = sql.user,    password= sql.pword,
                                                     database = sql.db_name, charset ='utf8')
                Logger.Log(f"Connected to SQL (via SSH) at {sql.host}:{tunnel.local_bind_port}/{sql.db_name}, {sql.user}", logging.DEBUG)
                return (tunnel, db_conn)
            except Exception as err:
                msg = f"Could not connect to the MySql database: {type(err)} {str(err)}"
                Logger.Log(msg, logging.ERROR)
                Logger.Print(msg, logging.ERROR)
                traceback.print_tb(err.__traceback__)
                if tunnel is not None:
                    tunnel.stop()
                return (None, None)
        else:
            return (None, None)

    @staticmethod
    def disconnectMySQL(db:Optional[connection.MySQLConnection], tunnel:Optional[sshtunnel.SSHTunnelForwarder]=None) -> None:
        if db is not None:
            db.close()
            Logger.Log("Closed MySQL database connection", logging.DEBUG)
        else:
            Logger.Log("No MySQL database to close.", logging.DEBUG)
        if tunnel is not None:
            tunnel.stop()
            Logger.Log("Stopped MySQL tunnel connection", logging.DEBUG)
        else:
            Logger.Log("No MySQL tunnel to stop", logging.DEBUG)

    # Function to build and execute SELECT statements on a database connection.
    @staticmethod
    def SELECT(cursor        :cursor.MySQLCursor,          db_name        : str,                   table    : str,
               columns       :List[str]           = [],    filter         : Optional[str] = None,
               sort_columns  :Optional[List[str]] = None,  sort_direction : str           = "ASC", grouping : Optional[str] = None,
               distinct      :bool                = False, offset         : int           = 0,     limit    : int           = -1,
               fetch_results :bool                = True) -> Optional[List[Tuple]]:
        """Function to build and execute SELECT statements on a database connection.

        :param cursor: A database cursor, retrieved from the active connection.
        :type cursor: cursor.MySQLCursor
        :param db_name: The name of the database to which we are connected.
        :type db_name: str
        :param table: The name of the table from which we want to make a selection.
        :type table: str
        :param columns: A list of columns to be selected. If empty (or None), all columns will be used (SELECT * FROM ...). Defaults to None
        :type columns: List[str], optional
        :param filter: A string giving the constraints for a WHERE clause (The "WHERE" term itself should not be part of the filter string), defaults to None
        :type filter: str, optional
        :param sort_columns: A list of columns to sort results on. The order of columns in the list is the order given to SQL. Defaults to None
        :type sort_columns: List[str], optional
        :param sort_direction: The "direction" of sorting, either ascending or descending., defaults to "ASC"
        :type sort_direction: str, optional
        :param grouping: A column name to group results on. Subject to SQL rules for grouping, defaults to None
        :type grouping: str, optional
        :param distinct: A bool to determine whether to select only rows with distinct values in the column, defaults to False
        :type distinct: bool, optional
        :param limit: The maximum number of rows to be selected. Use -1 for no limit., defaults to -1
        :type limit: int, optional
        :param fetch_results: A bool to determine whether all results should be fetched and returned, defaults to True
        :type fetch_results: bool, optional
        :return: A collection of all rows from the selection, if fetch_results is true, otherwise None.
        :rtype: Optional[List[Tuple]]
        """
        d          = "DISTINCT" if distinct else ""
        cols = ",".join(columns) if columns is not None and len(columns) > 0 else "*"
        sort_cols  = ",".join(sort_columns) if sort_columns is not None and len(sort_columns) > 0 else None
        table_path = db_name + "." + str(table)
        params = []

        sel_clause = f"SELECT {d} {cols} FROM {table_path}"
        where_clause = "" if filter    is None else f"WHERE {filter}"
        group_clause = "" if grouping  is None else f"GROUP BY {grouping}"
        sort_clause  = "" if sort_cols is None else f"ORDER BY {sort_cols} {sort_direction} "
        lim_clause   = "" if limit < 0         else f"LIMIT {str(max(offset, 0))}, {str(limit)}" # don't use a negative for offset
        query = f"{sel_clause} {where_clause} {group_clause} {sort_clause} {lim_clause};"

        return SQL.Query(cursor=cursor, query=query, params=None, fetch_results=fetch_results)

    @staticmethod
    def Query(cursor:cursor.MySQLCursor, query:str, params:Optional[Tuple], fetch_results: bool = True) -> Optional[List[Tuple]]:
        result : Optional[List[Tuple]] = None
        # first, we do the query.
        Logger.Log(f"Running query: {query}\nWith params: {params}", logging.DEBUG)
        start = datetime.now()
        cursor.execute(query, params)
        time_delta = datetime.now()-start
        Logger.Log(f"Query execution completed, time to execute: {time_delta}", logging.DEBUG)
        # second, we get the results.
        if fetch_results:
            result = cursor.fetchall()
            time_delta = datetime.now()-start
            Logger.Log(f"Query fetch completed, total query time:    {time_delta} to get {len(result) if result is not None else 0:d} rows", logging.DEBUG)
        return result

class MySQLInterface(DataInterface):

    # *** BUILT-INS ***
    def __init__(self, config:Dict[str,Any]):
        self._tunnel    : Optional[sshtunnel.SSHTunnelForwarder] = None
        self._db        : Optional[connection.MySQLConnection] = None
        self._db_cursor : Optional[cursor.MySQLCursor] = None
        super().__init__(config=config)
        self.Open()

    # *** IMPLEMENT ABSTRACT FUNCTIONS ***

    def _open(self, force_reopen:bool = False) -> bool:
        if force_reopen:
            self.Close()
            self.Open(force_reopen=False)
        if not self._is_open:
            start = datetime.now()
            
            _sql_cfg = self._config["MYSQL_CONFIG"]
            _ssh_cfg = self._config["MYSQL_CONFIG"]["SSH_CONFIG"]
            self._tunnel, self._db = SQL.ConnectDB(db_settings=_sql_cfg, ssh_settings=_ssh_cfg)
            if self._db is not None:
                self._db_cursor = self._db.cursor()
                self._is_open = True
                time_delta = datetime.now() - start
                Logger.Log(f"Database Connection Time: {time_delta}", logging.INFO)
                return True
            else:
                Logger.Log(f"Unable to open MySQL interface.", logging.ERROR)
                SQL.disconnectMySQL(tunnel=self._tunnel, db=self._db)
                return False
        else:
            return True

    def _close(self) -> bool:
        if SQL is not None:
            SQL.disconnectMySQL(tunnel=self._tunnel, db=self._db)
            Logger.Log("Closed connection to MySQL.", logging.DEBUG)
            self._is_open = False
        return True

    # *** PUBLIC STATICS ***

    # Check if we can connect to MySQL with the given config settings and raise SystemExit
    @staticmethod
    def TestConnection(settings:Dict[str,Any]) -> None:
        
        Logger.Log("Testing connection to MySQL", logging.INFO)
        mysqlInterface = MySQLInterface(settings)

        # If connection was successful
        if not mysqlInterface._is_open:
            Logger.Log("MySQL connection unsuccessful", logging.ERROR)
            sys.exit(1)

        Logger.Log("MySQL connection successful", logging.INFO)
        sys.exit(0)

    # *** PUBLIC METHODS ***
    # Get the value of a given variable for our current session
    def GetSessionVariable(self, variableName) -> str:

        self._db_cursor = self._db.cursor()
        result = SQL.Query(self._db_cursor, "SHOW VARIABLES LIKE '" + variableName + "'", None, True)
        self._db_cursor.close()

        return result[0][1]


    # Set variables for the duration of our current connection/session
    def SetSessionVariables(self) -> None:

        self._db_cursor = self._db.cursor()

        SQL.Query(self._db_cursor, "SET SESSION net_read_timeout = 1000", None, True)

        # The default net_write_timeout is 60 seconds which is exceeded if 
        # a MySQL cursor is open but we're unable to connect to BigQuery a dozen times.
        SQL.Query(self._db_cursor, "SET SESSION net_write_timeout = 1000", None, True)

        self._db_cursor.close()

    # Get the number of log entries categorized [unsynced, synced, both synced + unsynced] for the given date
    def GetMigrationStatusCountsByDate(self, dateToSync: datetime) -> List[int]:
 
        # Get the datetime for the start and end of the day
        dateToSyncStart = datetime.combine(dateToSync, time.min)
        dateToSyncEnd = datetime.combine(dateToSync, time.max)

        whereClause =  "`server_time` BETWEEN '" + dateToSyncStart.isoformat() + "' AND '" + dateToSyncEnd.isoformat() + "'"
        selectColumns = ["COUNT(synced)", "synced"]
        
        self._db_cursor = self._db.cursor()

        response = SQL.SELECT(self._db_cursor,
                    self._config["MYSQL_CONFIG"]["DB_NAME"], # Database
                    self._config["MYSQL_CONFIG"]["DB_TABLE"], # Table
                    selectColumns, # Select columns
                    whereClause,  # Filter
                    ["synced"], # Sort columsn
                    "ASC",
                    "synced") # Group By

        self._db_cursor.close()

        return [response[0][0], response[0][1], response[0][0] + response[0][1]] # unsynced, synced, either/all

    # Mark all log entries as synced for the given date
    def MarkLogEntriesAsSynced(self, dateSynced: datetime) -> None:

        # Create new cursor for executing a prepared statement
        self._db_cursor = self._db.cursor(prepared=True)

         # Get the datetime for the start and end of the day
        dateSyncedStart = datetime.combine(dateSynced, time.min)
        dateSyncedEnd = datetime.combine(dateSynced, time.max)

        updateQuery = "UPDATE `" + self._config["MYSQL_CONFIG"]["DB_NAME"] + "`.`" + self._config["MYSQL_CONFIG"]["DB_TABLE"] + "`"\
        + " SET `synced` = %s WHERE `server_time` BETWEEN %s AND %s"

        queryParams = (1, dateSyncedStart.isoformat(), dateSyncedEnd.isoformat())

        self._db_cursor.execute(updateQuery, queryParams)
        self._db.commit() # Required if autcommit is off for the session
        self._db_cursor.close()

    # Get an open cursor for all the log entries for the given date.
    # The result rows will be a dictionary keyed by column name
    def GetLogEntriesByDate(self, dateToSync: datetime, rowFormatType: SourceDataRowFormatType) -> cursor.MySQLCursor:
        
        # Get the datetime for the start and end of the day
        dateToSyncStart = datetime.combine(dateToSync, time.min)
        dateToSyncEnd = datetime.combine(dateToSync, time.max)

        #whereClause =  "server_time >= '" + dateToSync.strftime('%Y-%m-%d') + " 00:00:00.000000' AND server_time <= '" + dateToSync.strftime('%Y-%m-%d') + " 23:59:59.999999'"
        whereClause =  "`server_time` BETWEEN '" + dateToSyncStart.isoformat() + "' AND '" + dateToSyncEnd.isoformat() + "'"

        offset = 0

        # Note, if we are providing a non-zero offset a positive limit is required here. MySQL can't do OFFSET only with it's LIMIT clause
        limitNumberOfRecordsToCopy = -1
        
        if rowFormatType == SourceDataRowFormatType.LOGGER_LOG:
            raise Exception("The logger.log data format is not yet supported as it does not have a synced column, and has not been fully mapped to the BigQuery schema")
            selectColumns = ['id','app_id','app_id_fast','app_version','session_id','persistent_session_id',
            'player_id','level','event','event_custom','event_data_simple','event_data_complex','client_time',
            'client_time_ms','server_time','remote_addr','req_id','session_n','http_user_agent']
        elif rowFormatType == SourceDataRowFormatType.OPEN_GAME_DATA:
            selectColumns = ['id','session_id','user_id','user_data','client_time','client_time_ms','client_offset',
            'server_time','event_name','event_data','event_source','game_state','app_version','app_branch',
            'log_version','event_sequence_index','remote_addr','http_user_agent']
        else:
            raise Exception("Unsupported source row format type: " + str(rowFormatType))

        self._db_cursor = self._db.cursor(dictionary=True)

        # Execute a query for the cursor, but don't return the results
        SQL.SELECT(self._db_cursor,
                    self._config["MYSQL_CONFIG"]["DB_NAME"], # Database
                    self._config["MYSQL_CONFIG"]["DB_TABLE"], # Table
                    selectColumns, # Select columns
                    whereClause, # Filter
                    None, # Sort
                    'ASC', # Order
                    None, # Grouping
                    False, # Distinct
                    offset, # Offset
                    limitNumberOfRecordsToCopy, # Limit
                    False) #return results

        return self._db_cursor

    # Get the date of the oldest log entry we're allowed to sync
    def GetOldestUnmigratedDate(self) -> Optional[datetime.date]:

        # Let's find the most recent server_time entry in the database
        selectColumns = ["MAX(server_time)"]
        whereClause = "server_time != '0000-00-00 00:00:00'"

        self._db_cursor = self._db.cursor()
        
        result = SQL.SELECT(self._db_cursor,
                    self._config["MYSQL_CONFIG"]["DB_NAME"], # Database
                    self._config["MYSQL_CONFIG"]["DB_TABLE"], # Table
                    selectColumns,
                    whereClause)

        self._db_cursor.close()

        # By default assume we aren't able to sync logs newer than two days ago, since server_time isn't 
        # guaranteed to be in the same time zone or in UTC it's possible that new entries are being logged
        # for "yesterday" while this script is running.
        maximumDateToSync = date.today() - timedelta(days=2) # two days ago

        # If we have a maximum server_time entry
        if result is not None:
            # if the max server_time entry is today
            if result[0][0].date() == date.today():
                # We'll allow syncing of entries through the end of yesterday
                maximumDateToSync = date.today() - timedelta(days=1)

        # Append 23:59:59 time component to the date
        maximumDatetimeToSync = datetime.combine(maximumDateToSync, time.max)

        # Find the minimum server_time of entries that haven't been synced
        selectColumns = ["MIN(server_time)"]
        whereClause = "synced = 0 AND server_time != '0000-00-00 00:00:00' AND server_time <= '" + maximumDatetimeToSync.isoformat() + "'"

        self._db_cursor = self._db.cursor()

        result = SQL.SELECT(self._db_cursor,
                    self._config["MYSQL_CONFIG"]["DB_NAME"], # Database
                    self._config["MYSQL_CONFIG"]["DB_TABLE"], # Table
                    selectColumns, # Select columns
                    whereClause) # Filter
                    
        self._db_cursor.close()

        # Return None if no unsynced entries, else the date of the oldest unsynced entry
        if result[0][0] is None:
            return None

        return result[0][0].date()

    # *** PROPERTIES ***

    # *** PRIVATE STATICS ***

    # *** PRIVATE METHODS ***