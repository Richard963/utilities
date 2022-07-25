
from sqlalchemy import create_engine, inspect, engine, inspection
import pandas as pd
import os
from dotenv import load_dotenv

def buildEngine(schema='') -> engine:
    dialect = 'mysql'
    driver = 'mysqldb'
    username = os.getenv("DBUSER")
    password = os.getenv("DBPASSWD")
    dbaddress = os.getenv("DBADDRESS")
    dbname = schema
    port = os.getenv("DBPORT")
    url = (f"{dialect}+{driver}://{username}:{password}"
            f"@{dbaddress}:{port}/{dbname}")
    try:
        engine = create_engine(url)
    except Exception as e:
        print(e)
    return engine


class dbCNXN:
    def __init__(self,schema='') -> None:
        load_dotenv()
        self.engine = buildEngine(schema)
        self.schema = schema
        return None

    def databaseInformation(self)-> inspection:
        try:
            data = inspect(self.engine)
            data.get_schema_names()
        except Exception as e:
            print(e)
        return data

    def table_exists(self, table) -> bool:
        try:
            data = inspect(self.engine)
            tables = data.get_table_names()
        except Exception as e:
            print(e)
        
        if any(table in t for t in tables):
            return True
        else:
            return False

    def query(self,query:str) -> pd.DataFrame:
        data = pd.read_sql_query(query,self.engine)
        return data

    def write(self,df:pd.DataFrame,target:str):
        df.to_sql(
            target,
            self.engine,
            schema=self.schema,
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
    def __init__(self, table_name:str, DB:dbCNXN, types:dict={}) -> None:
        self.DB = DB
        self.tblname = table_name
        self.dtypes = types
        if not DB.table_exists(self.tblname):
            self.create_table()
        pass

    def create_table(self) -> None:
        string=''
        for k,v in self.dtypes.items():
            string+=k+" "+str(v)+", "
        string = string[:-2]
        self.DB.execute(f"""
        CREATE TABLE {self.DB.schema}.{self.tblname} (
            {string}
        )
        """)

    def drop_table(self) -> None:
        try:
            self.DB.execute(f"DROP TABLE {self.DB.schema}.{self.tblname}")
        except Exception as e:
            print(e)

    def write(self, df) -> None:
        self.DB.write(df,self.tblname)

    def get_100(self) -> pd.DataFrame:
        return self.DB.query(f"SELECT * FROM {self.DB.schema}.{self.tblname} LIMIT 100")
