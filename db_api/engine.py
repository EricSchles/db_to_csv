import sqlalchemy
import pandas as pd
import shutil
import os
import time
import math
import pickle

class Client:
    def __init__(self,db,config_file):
        self.db = db
        self.con = db.connect()
        self.config_file = config_file
        
    def get_next(self,cur):
        try:
            fetch = cur.fetchone()    
        except sqlalchemy.exc.DatabaseError:
            return "restart"
        return fetch

    def get_all(self):
        tables = self.load_config()
        for table_name in tables.keys():
            sql_statement = "SELECT * FROM "+table_name
            self.get(sql_statement,tables[table_name],table_name,get_all=True)

    def save(self,counter,table_counter,table_name,df,list_of_existing_ids):
        pickle.dump(list_of_existing_ids,open("list_of_existing_ids.pickle","w"))
        df.to_csv("cached_"+table_name+"_table.csv")
        counter += 1
        if counter%100 == 0:
            shutil.copy("cached_"+table_name+"_table.csv",str(table_counter)+table_name+"_table.csv")
        if counter%1000 == 0:
            table_counter+=1
            print "New csv started!"
        return counter,table_counter

    def reset(self):
        pickle.dump([],open("list_of_existing_ids.pickle","w"))
        
    def get(self,sql_statement,table_columns,table_name,id_index,get_all=False):
        counter = 0
        table_counter = 1
        list_of_existing_ids = pickle.load(open("list_of_existing_ids.pickle","r"))
        df = pd.DataFrame()
        if not get_all:
            if not "order by" in sql_statement.lower():
                raise NameError("Failed - ORDER BY not in SQL statement and is required because database is not fault tolerant")        
        cur = self.con.execute(sql_statement)
        fetch = cur.fetchone()
        while fetch:
            tmp = {}
            vals = fetch
            if not vals[id_index] in list_of_existing_ids:
                list_of_existing_ids.append(vals[id_index])
                for ind,header in enumerate(table_columns):
                    tmp[header] = vals[ind]
                df = df.append(tmp,ignore_index=True)
                counter,table_counter = self.save(counter,table_counter,table_name,df,list_of_existing_ids)
                fetch = self.get_next(cur)
                if fetch == "restart":
                    return True
            else:
                counter += 1
                fetch = cur.fetchone()
        
    def is_nan(self,record):
        if type(record) != type(str()):
            return math.isnan(record)
        else:
            return False

    def load_config(self):
        df = pd.read_csv(self.config_file)
        size = 0
        counter = 0
        for i in df.index: size += 1
        table_name = ""
        dicter = {}
        while counter < size:
            record = df.ix[counter]
            if not self.is_nan(record["TABLE NAME"]):
                table_name = record["TABLE NAME"]
                dicter[table_name] = []
                counter += 1
            else:
                if not self.is_nan(record["FIELD NAME"]):
                    tmp_record = record["FIELD NAME"]
                    tmp_record = tmp_record.replace("(16)","")
                    cleaned_record = tmp_record.strip()
                    dicter[table_name].append(cleaned_record)
                    counter += 1
            if self.is_nan(record["TABLE NAME"]) and self.is_nan(record["FIELD NAME"]): counter += 1
            
        return dicter
