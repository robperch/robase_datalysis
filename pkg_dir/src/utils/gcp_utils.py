## UTIL FUNCTION TO FOR GOOGLE CLOUD SERVICES





"----------------------------------------------------------------------------------------------------------------------"
############################################# Imports ##################################################################
"----------------------------------------------------------------------------------------------------------------------"


"--- Standard library imports ---"


"--- Third party imports ---"
import pygsheets
from google.oauth2 import service_account
import pandas as pd

"--- Local application imports ---"






"----------------------------------------------------------------------------------------------------------------------"
############### General utils for gcp ##################################################################################
"----------------------------------------------------------------------------------------------------------------------"


## Connecting to google sheet containing data
def gcp_connect_gsheet_tab(creds_path, sheet_name, tab_name):
    """
    Connecting to google sheet containing data

    :param creds_path: (string) path leading to the gcp credentials that will authorize access to the sheet
    :param sheet_name: (string) name of the sheet where the data is
    :param tab_name: (string) name of the tab where the data is
    :return g_sheet: (gsheet object) pygsheets spreasheet object
    :return worktab: (gsheet object) pygsheets worksheet object
    """


    ## Setting up client to interact with google sheet
    gc = pygsheets.authorize(service_file=creds_path)

    ## Obtaining the google sheet object
    g_sheet = gc.open(sheet_name)

    ## Obtaining the worksheet object
    worktab = g_sheet.worksheet_by_title(tab_name)


    return g_sheet, worktab



## Extracting all data from a google sheet as a dataframe
def extract_gsheet_data(creds_path, sheet_name, tab_name):
    """
    Extracting all data from a google sheet as a dataframe

    :param creds_path: (string) path leading to the gcp credentials that will authorize access to the sheet
    :param sheet_name: (string) name of the sheet where the data is
    :param tab_name: (string) name of the tab where the data is
    :return dfx: (dataframe) df containing all raw data extracted from the google sheet
    """


    ## Connecting to google sheet containing data
    g_sheet, worktab = gcp_connect_gsheet_tab(creds_path, sheet_name, tab_name)

    ## Extracting all records and converting to dataframe
    dfx = pd.DataFrame(worktab.get_all_records())


    return dfx





"----------------------------------------------------------------------------------------------------------------------"
"----------------------------------------------------------------------------------------------------------------------"
############################################# END OF FILE ##############################################################
"----------------------------------------------------------------------------------------------------------------------"
"----------------------------------------------------------------------------------------------------------------------"
