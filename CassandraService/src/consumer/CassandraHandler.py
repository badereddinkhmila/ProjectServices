import json
import os
from time import sleep
from datetime import datetime
from dateutil.relativedelta import relativedelta
from cassandra.query import BatchStatement, SimpleStatement
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy, ConsistencyLevel
from cassandra import query


class CassandraHandler:
    
    def __init__(self):
        #servers=[os.getenv("CASSANDRA_NODE_1"),os.getenv("CASSANDRA_NODE_2"),os.getenv("CASSANDRA_NODE_3")]
        servers=[os.getenv("CASSANDRA_NODE_1")]
        profile = ExecutionProfile(
            load_balancing_policy=WhiteListRoundRobinPolicy(servers),
            retry_policy=DowngradingConsistencyRetryPolicy(),
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
            request_timeout=20,
        )
        #cluster=Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile})
        cluster=Cluster(servers)
        try:
            print('Connecting...')
            self.__cassandra = cluster.connect('avc_storage')
            print('Connected !!')
        except:
            print('Unable to connect !!')
            
    def get_months(self,start_date,end_date):
        date_format = "%Y-%m"
        months = []
        chopS = len(start_date.split()[-1]) - 8
        start = start_date[:-chopS]
        chopE = len(end_date.split()[-1]) - 8
        end = end_date[:-chopE]
        start=datetime.strptime(start,'%Y-%m-%d %H:%M:%S')
        end=datetime.strptime(end,'%Y-%m-%d %H:%M:%S')
        num_months = (end.year - start.year) * 12 + (end.month - start.month)
        for i in range (0,num_months+1):
            months.append((start + relativedelta(months=i)).strftime(date_format))
        return (months)
    
    def get_month(self,date):
        date=datetime.fromtimestamp(date)
        month_format = "%Y-%m"
        return date.strftime(month_format)
    
    def date_cassandra(self,date):
        collected=datetime.fromtimestamp(date)
        return collected
    ##### Inserting data #####

    def InsertGlucose(self,msg):
        glucose=self.__cassandra.prepare("""
           INSERT INTO glucose_by_month (gateway, month, collect_time, mg_dl, mmol_l)
           VALUES (?, ?, ?, ?, ?)
           IF NOT EXISTS""")
        try:
           results = self.__cassandra.execute(glucose,[msg['gateway_id'],self.get_month(msg['collect_time']),
           self.date_cassandra(msg['collect_time']),msg['mg_dl'],msg['mmol_dl']])
        except  Exception:
            print('The ERROR :',Exception)
    
    def InsertOxygen(self,msg):
        oxygen=self.__cassandra.prepare("""
           INSERT INTO oxygen_by_month (gateway, month, collect_time, pulse, spo2)
           VALUES (?, ?, ?, ?, ?)
           IF NOT EXISTS
           """)
        try:
           results = self.__cassandra.execute(oxygen,[msg['gateway_id'],self.get_month(msg['collect_time']),
                self.date_cassandra(msg['collect_time']),msg['pulse'],msg['spo2']])
        except  Exception:
            print('The ERROR :',Exception)
    
    def InsertBloodPressure(self,msg):
        bp=self.__cassandra.prepare("""
           INSERT INTO bmoodpressure_by_month (gateway, month, collect_time, diastolic, pulse, systolic)
           VALUES (?, ?, ?, ?, ?)
           IF NOT EXISTS
           """)
        try:
           results = self.__cassandra.execute(bp,[msg['gateway_id'],self.get_month(msg['collect_time']),
                self.date_cassandra(msg['collect_time']),msg['diastolic'],msg['pulse'],msg['systolic']])
        except  Exception:
            print('The ERROR :',Exception)    
    
    def InsertTemperature(self,msg):
        temp=self.__cassandra.prepare("""
           INSERT INTO temperature_by_month (gateway, month, collect_time, temperature)
           VALUES (?, ?, ?, ?)
           IF NOT EXISTS
           """)
        #try:
        results = self.__cassandra.execute(temp,[msg['gateway_id'],self.get_month(msg['collect_time']),
        self.date_cassandra(msg['collect_time']),msg['temperature']])
        #except  Exception:
        #    print('ERROR: ',Exception)
        print(msg['gateway_id'],self.get_month(msg['collect_time']),self.date_cassandra(msg['collect_time']),msg['temperature'])    
    
    def InsertWeight(self,msg):
        weight=self.__cassandra.prepare("""
           INSERT INTO weight_by_month (gateway, month, collect_time, bmi, bodyfat,weight)
           VALUES (?, ?, ?, ?, ?, ?)
           IF NOT EXISTS
           """)
        try:
           results = self.__cassandra.execute(weight,[msg['gateway_id'],self.get_month(msg['collect_time']),
                self.date_cassandra(msg['collect_time']),msg['bmi'],msg['bodyfat'],msg['weight']])
        except  Exception:
            print('The ERROR :',Exception)
    
    def InsertError(self,msg):
        error=self.__cassandra.prepare("""
           INSERT INTO error_values_by_day (gateway, topic, collect_time,error)
           VALUES (?, ?, ?, ?)
           IF NOT EXISTS
           """)
        try:
           results = self.__cassandra.execute(error,[msg['gateway_id'],self.get_month(msg['collect_time']),
                self.date_cassandra(msg['collect_time']),msg['error']])
        except  Exception:
            print('The ERROR :',Exception)

    #### retreiving data ####
    
    def GetOxygen(self,gateway_id,start_date,end_date,month):
        spo2=[]
        pulse=[]
        maxMin=[]
        response = self.__cassandra.execute(
            """select collect_time, spo2, pulse from avc_storage.oxygen_by_month 
            where gateway=%(gateway)s AND month IN %(month)s AND collect_time>=%(start)s AND collect_time<=%(end)s;""",
                {'gateway':gateway_id, 'month':query.ValueSequence(month), 'start':start_date,'end':end_date})
        max_min = self.__cassandra.execute(
            """select max(spo2) as max_v, min(spo2) as min_v from avc_storage.oxygen_by_month 
            where gateway=%(gateway)s AND month IN %(month)s AND collect_time>=%(start)s AND collect_time<=%(end)s;""",
                {'gateway':gateway_id, 'month':query.ValueSequence(month), 'start':start_date,'end':end_date})
        
        for row in response:   
            spo2.append([row[0].timestamp() * 1000, row[1]])        
            pulse.append([row[0].timestamp() * 1000, row[2]])
        for row in max_min:
            maxMin.append([row[0],row[1]])               
        my_resp={'spo2':spo2,'pulse':pulse,'max_min':maxMin}
        return my_resp
    
    def GetBloodPressure(self,gateway_id,start_date,end_date,month):
        systolic=[]
        diastolic=[]
        pulse=[]
        maxMin=[]
        response = self.__cassandra.execute(
            """select collect_time,systolic,diastolic,pulse from bloodpressure_by_month
            where gateway=%(gateway)s AND month IN %(month)s AND collect_time>=%(start)s AND collect_time<=%(end)s;""",
                {'gateway':gateway_id, 'month':query.ValueSequence(month), 'start':start_date,'end':end_date})
        max_min = self.__cassandra.execute(
            """select max(diastolic) as max_v, min(diastolic) as min_v from avc_storage.bloodpressure_by_month 
            where gateway=%(gateway)s AND month IN %(month)s AND collect_time>=%(start)s AND collect_time<=%(end)s;""",
                {'gateway':gateway_id, 'month':query.ValueSequence(month), 'start':start_date,'end':end_date})
        
        for row in response:
            systolic.append([row[0].timestamp() * 1000, row[1]])        
            diastolic.append([row[0].timestamp() * 1000, row[2]])     
            pulse.append([row[0].timestamp() * 1000, row[3]])
        for row in max_min:
            maxMin.append([row[0],row[1]])
        my_resp={'systolic':systolic,'diastolic':diastolic,'pulse':pulse,'max_min':maxMin}
        return my_resp    
    
    def GetGlucose(self,gateway_id,start_date,end_date,month):
        mmol_l=[]
        mg_dl=[]
        maxMin=[]
        response = self.__cassandra.execute(
            """select collect_time,mmol_l, mg_dl from glucose_by_month
             where gateway=%(gateway)s AND month IN %(month)s AND collect_time>=%(start)s AND collect_time<=%(end)s;""",
                {'gateway':gateway_id, 'month':query.ValueSequence(month), 'start':start_date,'end':end_date})
        max_min = self.__cassandra.execute(
            """select max(mg_dl) as max_v, min(mg_dl) as min_v from avc_storage.glucose_by_month 
            where gateway=%(gateway)s AND month IN %(month)s AND collect_time>=%(start)s AND collect_time<=%(end)s;""",
                {'gateway':gateway_id, 'month':query.ValueSequence(month), 'start':start_date,'end':end_date})
        
        for row in response:        
            mmol_l.append([row[0].timestamp() * 1000, row[1]])        
            mg_dl.append([row[0].timestamp() * 1000, row[2]])
        for row in max_min:
            maxMin.append([row[0],row[1]])     
        my_resp={'mmol/l':mmol_l,'mg/dl':mg_dl,'max_min':maxMin}
        return my_resp    
    
    def GetWeight(self,gateway_id,start_date,end_date,month):
        bmi=[]
        bodyfat=[]
        weight=[]
        maxMin=[]
        response = self.__cassandra.execute(
            """select collect_time,bmi,bodyfat,weight from weight_by_month 
            where gateway=%(gateway)s AND month IN %(month)s AND collect_time>=%(start)s AND collect_time<=%(end)s;""",
                {'gateway':gateway_id, 'month':query.ValueSequence(month), 'start':start_date,'end':end_date})
        max_min = self.__cassandra.execute(
            """select max(weight) as max_v, min(weight) as min_v from avc_storage.weight_by_month 
            where gateway=%(gateway)s AND month IN %(month)s AND collect_time>=%(start)s AND collect_time<=%(end)s;""",
                {'gateway':gateway_id, 'month':query.ValueSequence(month), 'start':start_date,'end':end_date})
        for row in response:
            bmi.append([row[0].timestamp() * 1000, row[1]])        
            weight.append([row[0].timestamp() * 1000, row[3]])     
            bodyfat.append([row[0].timestamp() * 1000, row[2]])
        for row in max_min:
            maxMin.append([row[0],row[1]])
        my_resp={'bmi':bmi,'bodyfat':bodyfat,'weight':weight,'max_min':maxMin}
        return my_resp
    
    def GetTemperature(self,gateway_id,start_date,end_date,month):
        temp=[]
        maxMin=[]
        response = self.__cassandra.execute(
            """select collect_time,temperature from temperature_by_month 
            where gateway=%(gateway)s AND month IN %(month)s AND collect_time>=%(start)s AND collect_time<=%(end)s;""",
                {'gateway':gateway_id, 'month':query.ValueSequence(month), 'start':start_date,'end':end_date})
        max_min = self.__cassandra.execute(
            """select max(temperature) as max_v, min(temperature) as min_v from avc_storage.temperature_by_month 
            where gateway=%(gateway)s AND month IN %(month)s AND collect_time>=%(start)s AND collect_time<=%(end)s;""",
                {'gateway':gateway_id, 'month':query.ValueSequence(month), 'start':start_date,'end':end_date})
        for row in response:
            temp.append([row[0].timestamp() * 1000, row[1]])
        for row in max_min:
            maxMin.append([row[0],row[1]])        
        return {'temperature':temp,'max_min':maxMin}    
        
    def ReadColdStorageData(self,gateway_id,start_date,end_date,context):
        #### for seperated data
        """switcher = {
        'temperature': GetTemperature(self,gateway_id,start_date,end_date),
        'bloodpressure': GetTemperature(self,gateway_id,start_date,end_date),
        'oxygen': GetTemperature(self,gateway_id,start_date,end_date),
        'glucose': GetTemperature(self,gateway_id,start_date,end_date),
        'weight': GetTemperature(self,gateway_id,start_date,end_date),
        }
        func = switcher.get(context, lambda: "Invalid context !!")"""
        #### for all data
        month=self.get_months(start_date,end_date)
        oxy=self.GetOxygen(gateway_id,start_date,end_date,month)
        bp=self.GetBloodPressure(gateway_id,start_date,end_date,month)
        glu=self.GetGlucose(gateway_id,start_date,end_date,month)
        weight=self.GetWeight(gateway_id,start_date,end_date,month)
        temp=self.GetTemperature(gateway_id,start_date,end_date,month)
        my_resp={'oxygen':oxy,'bp':bp,'glucose':glu,'temperature':temp,'weight':weight}
        return my_resp
    
