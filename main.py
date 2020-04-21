import pandas as pd
import sys
import os
import gspread 
from oauth2client.service_account import ServiceAccountCredentials
import sqlalchemy
import sqlite3
import re
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, UniqueConstraint, PrimaryKeyConstraint
from df2gspread import df2gspread as d2g
from helper import *

if __name__ == "__main__":

    wks, credentials = create_wks()

    dbms = get_dbms()

    deliveries_df = grab_deliveries_gs(wks, 'PPE Distro Tracking')

    updated_deliveries = deliveries_to_sqlite(dbms, deliveries_df)

    update_total_count_to_gs(wks, updated_deliveries, credentials)

    update_deliveries_to_gs(wks, updated_deliveries, credentials)
