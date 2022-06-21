## MODULE WITH UTIL FUNCTIONS - GENERAL PURPOSE





"----------------------------------------------------------------------------------------------------------------------"
####################################################### Imports ########################################################
"----------------------------------------------------------------------------------------------------------------------"


"--------------- Standard library imports ---------------"
import yaml
import json
import os
from datetime import datetime
import dateutil

"--------------- Third party imports ---------------"
import pandas as pd
import unidecode

"--------------- Local application imports ---------------"
from pkg_dir.config import *





"----------------------------------------------------------------------------------------------------------------------"
############################## Generic functions #######################################################################
"----------------------------------------------------------------------------------------------------------------------"


## Read contents of yaml file
def read_yaml(crds_loc):
    """
    Read yaml file

    :param crds_loc: (string) location of yaml file
    :return config: (?) read file
    """

    ## Configuration parameter
    config = None

    ## Safe loading file
    try:
        with open(crds_loc, "r") as file:
            config = yaml.safe_load(file)
    except:
        raise FileNotFoundError("Read yaml file error: could not load file")

    return config



## Format json
def format_json(json):
    """
    Format json

    :param json: (json) raw json
    :return json_clean: (json) cleaned json
    """

    ## Formatting response
    json_clean = json.dumps(json.loads(json), ensure_ascii=False, indent=2)

    return json_clean



## Generating the data dictionary
def generate_data_dictionary(df_cols, data_dicts_loc, data_dict_filename):
    """

    :param dfx (list): df columns obtained using df.columns
    :param data_dicts_loc (string): absolute path to the data dicts directory
    :param data_dict_filename (stirng): name of the resulting json file containing the data dict
    :return None:
    """


    ## Create the dictionary as a python object
    data_dict = {
        col: {
            "Relevant": False,
            "Rename": col,
        }

        for col in df_cols
    }

    ## Converting the python object into a json
    with open(data_dicts_loc + data_dict_filename, "w") as outfile:
        json.dump(
            data_dict,
            outfile,
            ensure_ascii=False,
            indent=2,
        )


    return



## Reading json file
def read_json(file_path):
    """

    :param file_path (string): json file location
    :return json_contents (dictionary): contents in json file
    """


    ## Reading json file
    with open(file_path) as json_file:
        json_contents = json.load(json_file)


    return json_contents



## Creating a directory if it doesn't already exists
def create_directory_if_nonexistent(dir_path):
    """
    Creating a directory if it doesn't already exists

    :param dir_path: (string) path to the directory that will be checked
    :return None:
    """


    ## Concatenating dir path and dir name
    # dir_path = os.path.join(dir_path, dir_name)

    ## Creating directory
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)


    return



## Creating date string from current date back a specified number of days
def create_date_string(delta_days, day_adjust):
    """
    Creating date string from current date back a specified number of days

    :param delta_days: (int) number of days to back in time to build historic information
    :param day_adjust: (int) number of days added to the initial time (now)
    :return date_string: (str) date string covering time period in the format %Y-%m-%d_to_%Y-%m-%d
    """


    ## Final time, initial time and time delta

    ### Retrospective analyis
    if delta_days < 0:
        t1 = utc_tz.localize(datetime.utcnow()).astimezone(mexico_tz) + dateutil.relativedelta.relativedelta(days=day_adjust) ## Adding one day to ensure that the current day is included in the extraction.
        td = dateutil.relativedelta.relativedelta(days=delta_days)
        t0 = t1 + td

    ### Prospective analysis
    if delta_days > 0:
        t0 = utc_tz.localize(datetime.utcnow()).astimezone(mexico_tz) + dateutil.relativedelta.relativedelta(days=day_adjust) ## Adding one day to avoid counting the current day
        td = dateutil.relativedelta.relativedelta(days=delta_days)
        t1 = t0 + td


    ## Date string with suitable format for sql query
    date_string = datetime.strftime(t0, "%Y-%m-%d") + "_to_" + datetime.strftime(t1, "%Y-%m-%d")


    return date_string



## Generate date string (format: %Y-%m-%d) for a time period based on an initial month and a month delta
def generate_month_based_date_string(delta_months, current_month=utc_tz.localize(datetime.utcnow()).astimezone(mexico_tz).strftime('%Y-%m')):
    """
    Generate date string (format: %Y-%m-%d) for a time period based on an initial month and a month delta

    :param current_month: (string) date as a string (format: %Y-%m) that represents the end of the current month
    :param delta_months: (int) number of months considered for the time delta
    :return date_string: (str) date string covering time period in the format %Y-%m-%d_to_%Y-%m-%d
    """


    ## Base time considered for the time delta
    if delta_months == 0:
        t_base = datetime.strptime(current_month, '%Y-%m')
    else:
        t_base = datetime.strptime(current_month, '%Y-%m') + dateutil.relativedelta.relativedelta(months=1, day=1)

    ## Time after applying the time delta
    if delta_months == 0:
        t_adj = t_base + dateutil.relativedelta.relativedelta(months=1)
    else:
        t_adj = t_base + dateutil.relativedelta.relativedelta(months=delta_months)

    ## Creating date string with resulting times
    if t_base < t_adj:
        date_string = datetime.strftime(t_base, "%Y-%m-%d") + "_to_" + datetime.strftime(t_adj, "%Y-%m-%d")
    elif t_base > t_adj:
        date_string = datetime.strftime(t_adj, "%Y-%m-%d") + "_to_" + datetime.strftime(t_base, "%Y-%m-%d")


    return date_string



## Generate date string (format: %Y-%m-%d) for a time period based on an initial year and a year delta
def generate_year_based_date_string(delta_years, current_year=utc_tz.localize(datetime.utcnow()).astimezone(mexico_tz).strftime('%Y')):
    """
    Generate date string (format: %Y-%m-%d) for a time period based on an initial year and a year delta

    :param current_year: (string) date as a string (format: %Y) that represents the end of the current month
    :param delta_years: (int) number of years considered for the time delta
    :return date_string: (str) date string covering time period in the format %Y-%m-%d_to_%Y-%m-%d
    """


    ## Base time considered for the time delta
    if delta_years == 0:
        t_base = datetime.strptime(current_year, '%Y')
    else:
        t_base = datetime.strptime(current_year, '%Y') + dateutil.relativedelta.relativedelta(years=1, day=1)

    ## Time after applying the time delta
    if delta_years == 0:
        t_adj = t_base + dateutil.relativedelta.relativedelta(years=1)
    else:
        t_adj = t_base + dateutil.relativedelta.relativedelta(years=delta_years)

    ## Creating date string with resulting times
    if t_base < t_adj:
        date_string = datetime.strftime(t_base, "%Y-%m-%d") + "_to_" + datetime.strftime(t_adj, "%Y-%m-%d")
    elif t_base > t_adj:
        date_string = datetime.strftime(t_adj, "%Y-%m-%d") + "_to_" + datetime.strftime(t_base, "%Y-%m-%d")


    return date_string





"----------------------------------------------------------------------------------------------------------------------"
############################## Data schema based wrangling functions ###################################################
"----------------------------------------------------------------------------------------------------------------------"


"--------------- Data schema based unitary functions ---------------"


## Renaming columns based on a specified data schema
def rename_columns_with_data_schema(dfx, data_schema):
    """
    Renaming columns based on a specified data schema

    :param dfx (dataframe): df with the columns that will be cleaned
    :param data_schema (dictionary): data schema containing the clean column names
    :return dfx (dataframe): df after renaming the columns based on the data schema
    """


    ## Creating the data dictionary to rename the columns
    clean_col_names = {
        col: data_schema[col]["clean_col_name"]
        for col in data_schema
    }

    ## Renaming columns
    dfx.rename(columns=clean_col_names, inplace=True)


    return dfx



## Eliminating irrelevant columns based on specified data schema
def drop_irrelevant_columns_with_data_schema(dfx, data_schema):
    """

    :param dfx (dataframe): df containing irrelevant columns
    :param data_schema (dictionary): data schema containing distinction between relevant and irrelevant columns
    :return dfx (dataframe): df containing only relevant columns
    """


    ## Creating a list of the relevant columns
    rc = [
        data_schema[col]["clean_col_name"]
        for col in data_schema
        if data_schema[col]["relevant"]
    ]

    ## Dropping selected columns
    dfx = dfx.loc[:, rc]


    return dfx



## Formatting the columns' data types based on a specified data schema
def format_data_types_with_data_schema(dfx, data_schema):
    """
    Formatting the columns data types based on a specified data schema

    :param dfx (dataframe): df whose columns will be formatted according to the predefined data type
    :param data_schema (dictionary): data schema containing the columns data types
    :return dfx (dataframe): df with the columns formatted
    """


    ## Formatting each column based on it's datatype

    ### Strings
    rc = [
        data_schema[col]["clean_col_name"]
        for col in data_schema
        if data_schema[col]["relevant"]
           and data_schema[col]["data_type"] == "str"
    ]
    for col in rc:
        dfx[col] = dfx[col].astype("str")
        # dfx[col] = dfx[col].apply(lambda x: unidecode.unidecode(x.upper()))
        dfx[col] = dfx[col].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf8')


    ### Datetimes
    rc = [
        data_schema[col]["clean_col_name"]
        for col in data_schema
        if data_schema[col]["relevant"]
           and data_schema[col]["data_type"] == "datetime"
    ]
    for col in rc:
        dfx[col] = pd.to_datetime(dfx[col], errors="coerce")


    ### Integers
    rc = [
        data_schema[col]["clean_col_name"]
        for col in data_schema
        if data_schema[col]["relevant"]
           and data_schema[col]["data_type"] == "int"
    ]
    for col in rc:
        dfx[col] = pd.to_numeric(dfx[col], downcast="integer")


    ### Floats
    rc = [
        data_schema[col]["clean_col_name"]
        for col in data_schema
        if data_schema[col]["relevant"]
           and data_schema[col]["data_type"] == "float"
    ]
    for col in rc:
        dfx[col] = pd.to_numeric(dfx[col], downcast="float")
        dfx[col] = dfx[col].round(2)


    return dfx



## Mapping row values with information specified on the data schema
def map_row_values_with_data_schema(dfx, data_schema):
    """

    :param dfx (dataframe): df with rows as extracted from the data source
    :param data_schema (dictionary): data schema containing the value mapping
    :return dfx (dataframe): df with row values mapped
    """


    ## Creating dictionary with the clean column name and the mapping that will be used
    mapping_reference = {
        data_schema[col]["clean_col_name"]: data_schema[col]["values_map"]
        for col in data_schema
        if "values_map" in data_schema[col]
    }

    ## Mapping values according to reference
    for col in mapping_reference:
        dfx[col] = dfx[col].map(mapping_reference[col]).fillna(dfx[col])


    return dfx



"--------------- Data schema based compounded functions ---------------"


## Group of data wrangling functions that are based on the data schema
def data_wrangling_schema_functions(dfx, data_schema):
    """
    Group of data wrangling functions that are based on the data schema

    :param dfx: (dataframe) raw data previous to the wrangling process
    :param data_schema: (dictionary) data schema containing the parameters for the data wrangling
    :return dfx: (dataframe) data processed through the data wrangling functions
    """


    ## Renaming columns based on a specified data schema
    dfx = rename_columns_with_data_schema(dfx, data_schema)

    ## Eliminating irrelevant columns based on specified data schema
    dfx = drop_irrelevant_columns_with_data_schema(dfx, data_schema)

    ## Formatting the columns data types based on a specified data schema
    dfx = format_data_types_with_data_schema(dfx, data_schema)

    ## Mapping row values with information specified on the data schema
    dfx = map_row_values_with_data_schema(dfx, data_schema)


    return dfx





"----------------------------------------------------------------------------------------------------------------------"
############################## Useful general data wrangling functions #################################################
"----------------------------------------------------------------------------------------------------------------------"


"--------------- Useful general data wrangling unitary functions ---------------"


## Adding column with a distinction between the first and second half of the month (quincena)
def add_quincena_column(dfx, date_col_name):
    """
    Adding column with a distinction between the first and second half of the month (quincena)

    :param dfx: (dataframe) df without the quincena column added
    :param date_col_name: (string) name of the column that will be used as reference the create the new quincena column
    :return dfx: (dataframe) df with a column indicating the half of the month with the following format: %y-%m-qx (where 'x' stands for 1 or 2, depending on the month's half)
    """


    ## Creating support column to identify the half of the month
    dfx.insert(
        dfx.columns.to_list().index(date_col_name) + 1,
        'support_col_month_half',
        dfx[date_col_name].apply(lambda x: '1' if int(x.strftime('%d')) <= 15 else '2')
    )

    ## Creating quincena column
    dfx.insert(
        dfx.columns.to_list().index(date_col_name) + 1,
        'quincena',
        dfx[date_col_name].dt.strftime(date_format='%y-%m-q') + dfx['support_col_month_half']
    )

    ## Dropping support column
    dfx.drop(['support_col_month_half'], axis=1, inplace=True)


    return dfx





"----------------------------------------------------------------------------------------------------------------------"
"----------------------------------------------------------------------------------------------------------------------"
############### END OF FILE ############################################################################################
"----------------------------------------------------------------------------------------------------------------------"
"----------------------------------------------------------------------------------------------------------------------"
