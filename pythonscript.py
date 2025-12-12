#python -m pip install pandas sqlalchemy pymysql
#python -m pip install --upgrade cryptography pymysql
#1.Importing of Necessary Libraries
import pandas as pd
from sqlalchemy import create_engine

#2. Defining necessary variables with values assigned
username = 'testusr'
password = "Inceptez!123"
host = '136.114.39.161'
db = 'stgdb_ketan'

#3. Creating the DB connection using connection url using create_engine function & pymysql is used for providing driver to connect
engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}:3306/{db}")

#4. Create a variable to represent the source location of the data..
#folder="C:\\dataset\\"#ensure to use 2 slashes for escape sequence
folder="C:\\Users\\moham\\Downloads\\2 Datawarehouse Learning-20250923T174518Z-1-001\\2 Datawarehouse Learning\\dataset\\"

#5. Mapping of the source file with the target table using python dictionary
table_file_dict = {
    "stg_transactions": folder+"transactions.csv",
    "stg_accounts": folder+"accounts.csv",
    "stg_payments": folder+"payments.csv",
    "stg_creditcard": folder+"creditcard.csv",
    "stg_loans": folder+"loans.csv",
    "stg_cust_profile": folder+"cust.csv",
    "stg_branches": folder+"branches.csv",
    "stg_employees": folder+"employee.csv"}

#6. Can you iterate or loop or run through all the dictionary items to load the respective tables with the respective files...
for table, file in table_file_dict.items():#this for loop runs how many time?  8 times (based on no of items in my dictionary)
    #first iteration of for loop will assign table='stg_transactions' , file=folder+'transactions.csv'
    #second iteration of for loop will assign table='stg_accounts' , file=folder+'accounts.csv'    
    df = pd.read_csv(file)#first iter i will read accounts.csv file and convert it into dataframe/memorytable
#    if table=='stg_branches':
#        df=df.query("branch_id=130")#If i want to do some filteration/transformation on the data
    
    df.to_sql(table, con=engine, index=False, if_exists="replace")#pls load the data from df/memorytable to actual db table
    
    print(f"Rows loaded in the table {table}")
    
#file="C:\\Users\\moham\\Downloads\\2 Datawarehouse Learning-20250923T174518Z-1-001\\2 Datawarehouse Learning\\dataset\\employee.csv"