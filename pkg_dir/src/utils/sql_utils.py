## MODULE WITH UTIL FUNCTIONS - GENERAL PURPOSE





"----------------------------------------------------------------------------------------------------------------------"
####################################################### Imports ########################################################
"----------------------------------------------------------------------------------------------------------------------"


"--------------- Standard library imports ---------------"

"--------------- Third party imports ---------------"
import psycopg2
import mysql.connector
from mysql.connector import errorcode
import pandas as pd

"--------------- Local application imports ---------------"
from pkg_dir.config.config import creds_file_path as crds_loc
from pkg_dir.src.utils.general_utils import read_yaml





"----------------------------------------------------------------------------------------------------------------------"
############################## Ancillary functions #####################################################################
"----------------------------------------------------------------------------------------------------------------------"


## Converting date string into a dictionary suitable for the sql parameters
def datestring_to_sql_parameter(date_string):
    """
    Converting date string into a dictionary suitable for the sql parameters

    :param date_string (string): string that contain a start and end date in the format %Y%m%d_to_%Y%m%d
    :return date_sql_param (dict): dictionary with the date parameters defined to conduct the sql query
    """


    ## Dictionary comprehension to create dictionary parameter
    date_sql_param = {
        "$" + str(i + 1): "'" + date_string.split(sep="_to_")[i] + "'"
        for i in range(0, 1 + 1)
    }


    return date_sql_param





"----------------------------------------------------------------------------------------------------------------------"
############################## Configuration functions #################################################################
"----------------------------------------------------------------------------------------------------------------------"


## Get bd credentials
def get_db_crds(db_crds):
    """
    Get credentials to connect to database

    :param db_crds (string): Specification of the database the user wants to connect to
    :return db_crds_data (json): db credentials in json format
    """

    ## Reading file with crds
    crds = read_yaml(crds_loc)

    ## Selecting the right credentials
    db_crds_data = crds[db_crds]


    return db_crds_data



## Obtain the connection object for the sql database
def database_conection(db_crds):
    """
    Obtain the connection object for the sql database

    :param db_crds (string): Specification of the database the user wants to connect to
    :return conn (rdbms connection): connection to postgre database
    """


    ## Creating connection string based on credentials
    db_creds = get_db_crds(db_crds)


    ## Obtaining the connection object according to the db_crds

    ### Punto Clínico's database (production)
    if db_crds == "pc_db_prod":
        conn_string = "host=" + db_creds["host"] + " dbname=" + db_creds["dbname"] + " user=" + db_creds["user"] + " password=" + db_creds["psw"]
        conn = psycopg2.connect(conn_string)

    ### Punto Clínico's database (local backups)
    elif db_crds == "pc_db_backup":
        conn_string = "dbname=" + db_creds["dbname"] + " user=" + db_creds["user"] + " password=" + db_creds["psw"]
        conn = psycopg2.connect(conn_string)

    ### MySQL
    elif "mysql" in db_crds:

        try:
            conn = mysql.connector.connect(**get_db_crds(db_crds))

        except mysql.connector.Error as err:

            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something wrong with username of password")

            if err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")

            else:
                print(err)

    ### Not found
    else:
        raise NameError("No protocol defined for the RDBMS selected")


    return conn





"----------------------------------------------------------------------------------------------------------------------"
############################## Execution functions #####################################################################
"----------------------------------------------------------------------------------------------------------------------"


## Executing sql script with specified variables
def execute_sql_script(db_crds, sql_files_path, sql_script, sql_params):
    """
    Executing sql script with specified variables

    :param db_crds (string): Specification of the database the user wants to connect to
    :param sql_files_path (string): path where the sql script is located
    :param sql_script (string): name of sql script
    :param sql_params (dictionary): dict with mapping between variables and real values
    :return (tuples): contents obtained from sql query
    """


    ## Establishing connection to database
    conn = database_conection(db_crds)

    ## Creating cursor
    cur = conn.cursor()

    ## Opening sql script that will be executed
    sql_script = open(sql_files_path + sql_script, "r").read()

    ## Replacing variable's placeholders with real variables
    for var in sql_params:
        sql_script = sql_script.replace(var, sql_params[var])

    # ## Executing sql file
    # cur.execute(sql_script)

    ## Executing file with pandas library
    dfx = pd.read_sql_query(sql_script, conn)

    # Obtaining results from script execution
    # tuples = cur.fetchall()

    ## Closing cursor and connection
    cur.close()
    conn.close()


    # return tuples
    return dfx



## Obtain dataframe from sql query
def sql_to_df(db_crds, sql_files_path, sql_script, sql_params):
    """
    Obtain dataframe from sql query

    :param db_crds (string): Specification of the database the user wants to connect to
    :param sql_files_path (string): path where the sql script is located
    :param sql_script (string): name of sql script
    :param sql_params (dictionary): dict with sql variables and column names
    :return dfx (dataframe): df obtained from sql query
    """


    ## Obtaining query results as tuples
    # tuples = execute_sql_script(db_crds, sql_files_path, sql_script, sql_params["params"])
    dfx = execute_sql_script(db_crds, sql_files_path, sql_script, sql_params["params"])

    ## Converting tuples into dataframe
    # dfx = pd.DataFrame(tuples)

    ## Naming columns based on guide
    if 'colnames' in sql_params:
        dfx.rename(columns=sql_params["colnames"], inplace=True)


    return dfx





"----------------------------------------------------------------------------------------------------------------------"
"----------------------------------------------------------------------------------------------------------------------"
####################################################### END OF FILE ####################################################
"----------------------------------------------------------------------------------------------------------------------"
"----------------------------------------------------------------------------------------------------------------------"
