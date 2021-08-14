import json
import os
from time import sleep
from datetime import datetime
import psycopg


class PostgreSQLHandler:
    def __init__(self):
        pg_db=os.getenv("PG_DB")
        pg_user=os.getenv("PG_USER")
        pg_pass=os.getenv("PG_PASS")
        try:
            print('Connecting...')
            self.__connection = psycopg.connect(dbname=pg_db,user=pg_user,password=pg_pass,autocommit=True)
            print('Connected !!')
        except:
            print('Unable to connect !!')
        self.__pg=self.__connection.cursor()

    def date_pg(self,date):
        collected=datetime.fromtimestamp(date)
        return collected
    ##### Inserting data #####

    def InsertGlucose(self,msg):
        glucose="""
           INSERT INTO blood_sugar (device_id, collect_time, mg_dl, mmol_l)
           VALUES (%s, %s, %s, %s)
           ON CONFLICT DO NOTHING;"""
        try:
            self.__pg.execute(glucose,(msg['gateway_id'],self.date_pg(msg['collect_time']),msg['mg_dl'],msg['mmol_dl']))
        except  (Exception) as err:
            print('ERROR: ',err)

    def InsertOxygen(self,msg):
        oxygen="""
           INSERT INTO oxygen_level (device_id, collect_time, pulse, spo2)
           VALUES (%s, %s, %s, %s)
           ON CONFLICT DO NOTHING;
           """
        try:
            self.__pg.execute(oxygen,(msg['gateway_id'],
                self.date_pg(msg['collect_time']),msg['pulse'],msg['spo2']))
        except  (Exception) as err:
            print('ERROR: ',err)

    def InsertBloodPressure(self,msg):
        bp="""
           INSERT INTO bmood_pressure (device_id, collect_time, diastolic, pulse, systolic)
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT DO NOTHING;
           """
        try:
            self.__pg.execute(bp,(msg['gateway_id'],
                self.date_pg(msg['collect_time']),msg['diastolic'],msg['pulse'],msg['systolic']))
        except  (Exception) as err:
            print('ERROR: ',err)

    def InsertTemperature(self,msg):
        temp= """
           INSERT INTO temperature (device_id, collect_time, temperature)
           VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;
           """
        try:
            self.__pg.execute(temp,(msg['gateway_id'],
        self.date_pg(msg['collect_time']),msg['temperature']))
        except  (Exception) as err:
            print('ERROR: ',err)

    def InsertWeight(self,msg):
        weight="""
           INSERT INTO weight (device_id, collect_time, bmi, bodyfat,weight)
           VALUES (%s, %s, %s, %s, %s)
           ON CONFLICT DO NOTHING;
           """
        try:
           results =  self.__pg.execute(weight,(msg['gateway_id'], 
            self.date_pg(msg['collect_time']),msg['bmi'],msg['bodyfat'],msg['weight']))
        except  (Exception) as err:
            print('ERROR: ',err)


    #### retreiving data ####


