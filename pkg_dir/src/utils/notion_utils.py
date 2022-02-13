## MODULE WITH UTIL FUNCTIONS - NOTION





"----------------------------------------------------------------------------------------------------------------------"
####################################################### Imports ########################################################
"----------------------------------------------------------------------------------------------------------------------"


## Standard library imports

import requests


## Third party imports

import pandas as pd


## Local application imports

from pkg_dir.config.config import (

    creds_file_path as crds_loc,

)

from pkg_dir.src.utils.general_utils import (

    read_yaml,

)





"----------------------------------------------------------------------------------------------------------------------"
####################################################### Functions ######################################################
"----------------------------------------------------------------------------------------------------------------------"


## Read notion database with api
def notion_api_call(db_api_url, db_id, headers):
    """
    Read notion database with api

    :param db_api_url (string): base url provided by Notion to make api calls
    :param db_id (string): unique id of the database that will be read
    :param headers (dictionary): dict with authorization and version info
    :return req (?): response after calling notions api
    """


    ## Configuring reading URL
    read_url = db_api_url + db_id + "/query"

    ## Requesting info via the API
    req = requests.request(
        "POST",
        read_url,
        headers=headers
    )

    ## Verifying API call status
    print("API interaction status code: ", req.status_code)


    return req



## Calling a Notion database as a json via Notion's API
def get_notion_db_json(db_id):
    """
    Calling a Notion database as a json via Notion's API

    :param db_id (string): unique id of the database that will be called
    :return db_json (json): json with the notion's db contents
    """


    ## Reading credentials from yaml file
    yaml_file = read_yaml(crds_loc)
    notion_version = yaml_file["notion_api"]["notion_version"]
    db_api_url = yaml_file["notion_api"]["db_api_url"]
    api_key = yaml_file["notion_api"]["api_key"]


    ## Building headers for the API call
    headers = {
        "Authorization": "Bearer " + api_key,
        "Notion-Version": notion_version
    }


    ## Calling notion's api
    req = notion_api_call(db_api_url, db_id, headers)

    ## Converting the api response to a json
    db_json = req.json()


    return db_json



## Crating a schema of the notion database that was read
def create_notion_db_schema(db_json, relevant_properties):
    """
    Crating a schema of the notion database that was read

    :param db_json (json): json object obtained by calling notion's api
    :param relevant_properties (list): list of string with the names of the relevant properties
    :return db_schema (dictionary): schema of the table that includes the properties' data type
    """


    ## Selecting a sample entry to go over all of it's properties
    sample_entry = db_json["results"][0]["properties"]


    ## Bulding dictionary (schema) of the relevant properties and their datatypes
    db_schema = {
        prop: {
            "data_type": sample_entry[prop]["type"]
        }
        for prop in sample_entry
        if prop in relevant_properties
    }

    # print(db_schema)


    return db_schema



## Building a the blueprint dictionary for the dataframe (orient=index)
def notion_db_blueprint_df(db_json, db_schema, index_prop):
    """
    Building a the blueprint dictionary for the dataframe (orient=index)

    :param db_json (json): json object obtained by calling notion's api
    :return db_schema (dictionary): schema of the table that includes the properties' data type
    :param index_prop (string): name of the property that will serve as the df's index
    :return df_dict (dict): dictionary that will be used to create a dataframe with the json contents
    """


    ## Empty dictionary that will store all the results
    df_dict = {}


    ## Iterating over every row in the dataframe
    for row in db_json["results"]:

        ## Defining the table's base attributes

        #### All properties contained in the notion db
        row_props = row["properties"]

        #### Name of the index; key attribute in the notion db
        row_name = row_props[index_prop]["title"][0]["plain_text"]

        #### Empty list to store all the row contents
        row_contents = []


        ## Iterating over every relevant property in the table
        for col in db_schema:

            ## Identifying the datatype of the property
            data_type = db_schema[col]["data_type"]


            ## Set of conditions to determine how the row will be treated

            #### Skipping the index row
            if data_type == "title":
                continue

            #### Searching for data in specific locations for special data types (1)
            elif data_type in ["select", "person", "created_by"]:
                try:
                    row_contents.append(row_props[col][data_type]["name"])
                except:
                    row_contents.append("No_data")

            #### Searching for data in specific locations for special data types (2)
            elif data_type in ["rich_text"]:
                try:
                    row_contents.append(row_props[col][data_type][0]["text"]["content"])
                except:
                    row_contents.append("No_data")

            #### Searching for data in specific locations for special data types (2)
            elif data_type in ["formula"]:

                try:

                    #### Applying conditions based on the type of formula result

                    if row_props[col][data_type]["type"] == "string":
                        row_contents.append(row_props[col][data_type]["string"])

                    elif row_props[col][data_type]["type"] == "number":
                        row_contents.append(row_props[col][data_type]["number"])

                except:
                    row_contents.append("No_data")

            #### General procedure to find data
            else:
                row_contents.append(row_props[col][db_schema[col]["data_type"]])


        ## Saving the row contents gathered
        df_dict[row_name] = row_contents


    return df_dict



## Obtaining a dataframe from a notion database
def notion_json_to_df(db_json, relevant_properties):
    """
    Obtaining a dataframe from a notion database

    :param db_json (json): json object obtained by calling notion's api
    :param relevant_properties (list): list of string with the names of the relevant properties
    :return df_n (dataframe): resulting dataframe crated based on the blueprint generated
    """


    ## General parameters needed to build the dataframe

    #### Database schema
    db_schema = create_notion_db_schema(db_json, relevant_properties)

    #### Property that will be used as the dataframe's index
    index_prop = [prop for prop in db_schema if db_schema[prop]["data_type"] == "title"][0]


    ## Building a the blueprint dictionary for the dataframe (orient=index)
    df_dict = notion_db_blueprint_df(db_json, db_schema, index_prop)


    ## Creating dataframe with the resulting blueprint dictionary

    #### Crating dataframe
    df_n = pd.DataFrame.from_dict(df_dict, orient="index")

    #### Inserting the table's index as a column at the end of the df
    df_n.insert(
        df_n.shape[1],
        index_prop,
        df_n.index
    )

    #### Resetting index
    df_n.reset_index(inplace=True, drop=True)

    #### Adjusting column names
    df_n.columns = [col_n for col_n in db_schema]


    return df_n



## Obtaining a Notion database as dataframe with the selected columns
def notion_db_to_df(db_id, relevant_properties):
    """
    Obtaining a Notion database as dataframe with the selected columns

    :param db_id (string): unique id to identify the notion database
    :param relevant_properties (list): list of string with the names of the relevant properties
    :return df_n (dataframe): resulting dataframe crated based on the blueprint generated
    """


    ## Calling a Notion database as a json via Notion's API
    db_json = get_notion_db_json(db_id)

    ## Obtaining a dataframe from a notion database
    df_n = notion_json_to_df(db_json, relevant_properties)


    return df_n





"----------------------------------------------------------------------------------------------------------------------"
"----------------------------------------------------------------------------------------------------------------------"
## END OF FILE ##
"----------------------------------------------------------------------------------------------------------------------"
"----------------------------------------------------------------------------------------------------------------------"