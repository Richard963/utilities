
from sqlalchemy import create_engine, inspect, engine, inspection
import pandas as pd
import os
import json
import logging
from dotenv import load_dotenv

class dbCNXN:
    def __init__(self) -> None:
        load_dotenv()
        self.engine = self.build_engine()
        return None

    def build_engine() -> engine:
        try:
            with open(os.path.join(os.getcwd(),'database.json'), "r") as read_file:
                config = json.load(read_file)
        except Exception as e: 
            logging.error("database.json failed to load")
            logging.error(e)
        
        username = os.getenv("DBUSER")
        password = os.getenv("DBPASSWD")

        default  = 'Dev' #Sort this out!

        url = (
            f"{config[default]['dialect']}+{config[default]['driver']}://"
            f"{username}:{password}@{config[default]['hostname']}:"
            f"{config[default]['port']}/{config[default]['database']}")
            
        try:
            engine = create_engine(url)
        except Exception as e:
            print(e)
        return engine

    def database_information(self)-> inspection:
        try:
            data = inspect(self.engine)
            data.get_schema_names()
        except Exception as e:
            print(e)
        return data

    def table_exists(self, table, schema=None) -> bool:
        try:
            data = inspect(self.engine)
            tables = data.get_table_names(schema)
        except Exception as e:
            print(e)
        
        if any(table in t for t in tables):
            return True
        else:
            return False

    def query(self,query:str) -> pd.DataFrame:
        data = pd.read_sql_query(query,self.engine)
        return data

    def write(self,df:pd.DataFrame,target_table:str, schema:str):
        df.to_sql(
            target_table,
            self.engine,
            schema=schema,
            if_exists='append', 
            chunksize=10000,
            index=False
            )
        return None

    def execute(self,query:str) -> None:
        try:
            engine1=self.engine
            cnxn=engine1.connect()
            cnxn.execute(query)
        except Exception as e:
            print(e)
        return None

class Table:
    def __init__(self, table_name:str, schema:str, DB:dbCNXN, types:dict={}) -> None:
        self.DB = DB
        self.schema = schema
        self.tblname = table_name
        self.dtypes = types
        if not DB.table_exists(self.tblname, schema=self.schema):
            self.create_table()
        pass

    def create_table(self) -> None:
        string=''
        for k,v in self.dtypes.items():
            string+=k+" "+str(v)+", "
        string = string[:-2]
        self.DB.execute(f"""
        CREATE TABLE {self.schema}.{self.tblname} (
            {string}
        )
        """)

    def drop_table(self) -> None:
        try:
            self.DB.execute(f"DROP TABLE {self.schema}.{self.tblname}")
        except Exception as e:
            print(e)

    def truncate_table(self) -> None:
        try:
            self.DB.execute(f"TRUNCATE TABLE {self.schema}.{self.tblname}")
        except Exception as e:
            print("Table could not be truncated: " + e)

    def write(self, df) -> None:
        self.DB.write(df,self.tblname, self.schema)

    def get_100(self) -> pd.DataFrame:
        return self.DB.query(f"SELECT * FROM {self.schema}.{self.tblname} LIMIT 100")

    def get(self) -> pd.DataFrame:
        return self.DB.query(f"SELECT * FROM {self.schema}.{self.tblname}")
