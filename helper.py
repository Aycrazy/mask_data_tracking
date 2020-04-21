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

#GLOBAL VARIABLELS


# Global Variables
SQLITE                  = 'sqlite'

# Table Names
DELIVERIES          = 'deliveries'
TOTAL       = 'total_items_delivered'
POC = 'points_of_contact'


#SQLITE CONNECTION FUNS

def create_wks():
    scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

    credentials = ServiceAccountCredentials.from_json_keyfile_name(
         'covid19tracker-273614-fa3cbba84de0.json', scope) # Your json file here

    gc = gspread.authorize(credentials)

    wks = gc.open_by_url("https://docs.google.com/spreadsheets/d/1nTiL-AuU6Jn2Ovr3iO_TDIJODZ0AoHzYzxGE-YPVGts/edit#gid=227207881")

    return wks, credentials


def get_engine():

    engine = create_engine('sqlite:///mcw_ppe_db.sqlite', echo=False)

    return engine


#DATABASE FUNS

class MyDatabase:
    # http://docs.sqlalchemy.org/en/latest/core/engines.html
    DB_ENGINE = {
        SQLITE: 'sqlite:///{DB}'
    }

    # Main DB Connection Ref Obj
    db_engine = None
    def __init__(self, dbtype, username='', password='', dbname=''):
        dbtype = dbtype.lower()
        if dbtype in self.DB_ENGINE.keys():
            engine_url = self.DB_ENGINE[dbtype].format(DB=dbname)
            self.db_engine = create_engine(engine_url)
            print(self.db_engine)
        else:
            print("DBType is not found in DB_ENGINE")
    
    def create_db_tables(self):
        metadata = MetaData()
        deliveries = Table(DELIVERIES, metadata,
                      Column('agency', String),
                      Column('date_distributed',String),
                      Column('number_distributed', Integer),
                      Column('distributor', String),
                      Column('box_num', String),
                      Column('kit_num', String),
                      PrimaryKeyConstraint('agency', 'date_distributed','box_num','kit_num', name = 'pk_agency_date')
                      )
        total = Table(TOTAL, metadata,
                        #Column('id', Integer, primary_key=True),
                        Column('agency', None, ForeignKey('deliveries.agency')),
                        Column('total_distributed', Integer, nullable=False),
                        UniqueConstraint('agency', 'total_distributed', sqlite_on_conflict='REPLACE')
                        )
        poc = Table(POC, metadata,
                    Column('agency_poc', String, primary_key=True),
                    Column('agency', None, ForeignKey('deliveries.agency')),
                    Column('agency_cell', String),
                    Column('box_num', String),
                    Column('distributor', None, ForeignKey('deliveries.distributor'))
                   )
        try:
            metadata.create_all(self.db_engine)
            print("Tables created")
        except Exception as e:
            print("Error occurred during Table creation!")
            print(e)
    # Insert, Update, Delete
    def execute_query(self, query=''):
        if query == '' : return
        print (query)
        with self.db_engine.connect() as connection:
            try:
                excute = connection.execute(query)
            except Exception as e:
                print(e)
            if 'select' in query:
                execute.fetchall()
                
    def print_all_data(self, table='', query=''):
        query = query if query != '' else "SELECT * FROM '{}';".format(table)
        print(query)
        with self.db_engine.connect() as connection:
            try:
                result = connection.execute(query)
            except Exception as e:
                print(e)
            else:
                for row in result:
                    print(row) # print(row[0], row[1], row[2])
                result.close()
        print("\n")

#'Agency','Number Distributed LAST','Date of Last Distribution','Who will distribute','Box #','Kit #'
def add_deliveries(dbms, df):
    df = df.rename(columns={'Who Distributes':'Who Distributes Next',\
                            'Who will distrubte?':'Who Distributes Next',\
                           'Who Will distribute?':'Who Distributes Next'})
    print(df.columns)
    for row in df.iterrows():
        insert_dict = {}
        
        if 'Date of Last Distribution' in df.columns:
            insert_dict['agency'] = row[1]['Agency']
            insert_dict['date_distributed'] = row[1]['Date of Last Distribution']
            insert_dict['number_distributed'] = row[1]['Number Distributed LAST']
            insert_dict['distributor'] = row[1]['Who Distributes Next']
            insert_dict['box'] = row[1]['Box #']
            insert_dict['kit'] = row[1]['Kit #']
        elif 'Timestamp' in df.columns:
            insert_dict['agency'] = row[1]['What agency did you drop the PPE off at?']
            insert_dict['date_distributed'] = row[1]['Date of Drop-Off']
            insert_dict['number_distributed'] = int(row[1]['Number of Units'])*700
            insert_dict['distributor'] = row[1]['Your Name']
            insert_dict['box'] = row[1]['Box Number']
            insert_dict['kit'] = row[1]['Kit Number']
            
        else:
            insert_dict['agency'] = row[1]['agency']
            insert_dict['date_distributed'] = row[1]['date_distributed']
            insert_dict['number_distributed'] = row[1]['number_distributed']
            insert_dict['distributor'] = row[1]['distributor']
            insert_dict['box'] = row[1]['box_num']
            insert_dict['kit'] = row[1]['kit_num']

        dbms.execute_query('INSERT INTO deliveries(agency,date_distributed,number_distributed, distributor, box_num,kit_num)\
                           VALUES ("{0}","{1}","{2}","{3}","{5}","{4}")'.\
                              format(insert_dict['agency'],insert_dict['date_distributed'],\
                                     insert_dict['number_distributed'], insert_dict['distributor'] ,insert_dict['box'],
                                    insert_dict['kit']))

def add_total(dbms, df):

    df = df.rename(columns={'Total':'Total Delivered'})
    for row in df.iterrows():
        insert_dict = {}
        if 'Agency' in df.columns:
            insert_dict['agency'] = row[1]['Agency']
            insert_dict['total_distributed'] = row[1]['Total Delivered']
        else:
            insert_dict['agency'] = row[1]['agency']
            insert_dict['total_distributed'] = row[1]['total_distributed']
            
        dbms.execute_query('INSERT INTO total_items_delivered(agency, total_distributed)\
                           VALUES ("{0}","{1}")'.\
                              format(insert_dict['agency'],insert_dict['total_distributed']))


def get_dbms():

    dbms = MyDatabase('sqlite', dbname='mcw_ppe_db.sqlite')

    return dbms

#UPDATE DATA IN GOOGLE SPREADSHEET

def grab_deliveries_gs(wks, sheet_name):

    deliveries = wks.worksheet(sheet_name)

    deliveries_data = deliveries.get_all_values()
    delivery_headers = deliveries_data.pop(0)
    deliveries_df = pd.DataFrame(deliveries_data, columns=delivery_headers)

    return deliveries_df


def deliveries_to_sqlite(dbms, deliveries_df):

    engine = get_engine()

    old_len = pd.read_sql_query("SELECT * FROM deliveries WHERE number_distributed > 0 AND \
                                   number_distributed IS NOT NULL", engine).shape[0]


    add_deliveries(dbms, deliveries_df)


    updated_deliveries = pd.read_sql_query("SELECT * FROM deliveries WHERE number_distributed > 0 AND \
                                   number_distributed IS NOT NULL", engine)

    new_len = updated_deliveries.shape[0]

    print('{0} new rows added'.format((new_len-old_len)))

    return updated_deliveries


def update_total_count_to_gs(wks, deliveries_qdf, credentials):
    
    spreadsheet_key = '1nTiL-AuU6Jn2Ovr3iO_TDIJODZ0AoHzYzxGE-YPVGts'
    wks_name = 'Total Delivered'
    d2g.upload(deliveries_qdf.groupby('agency').sum().reset_index(), spreadsheet_key, wks_name, credentials=credentials, row_names=True)


def update_deliveries_to_gs(wks, deliveries_qdf, credentials):

    spreadsheet_key = '1nTiL-AuU6Jn2Ovr3iO_TDIJODZ0AoHzYzxGE-YPVGts'
    wks_name = 'Deliveries Made'
    d2g.upload(deliveries_qdf , spreadsheet_key, wks_name, credentials=credentials, row_names=True)