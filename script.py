import sqlalchemy as sa
import urllib
import datetime
import re
import os
import shutil
import pandas as pd

server='WIN-SNQUCAIM13C'
database = 'KPI_4G'
username= 'script_report'
password = '49*pHdW*I4R2x'

params = urllib.parse.quote_plus('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)
print(params)

engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s"%params)
engine.connect()

def dfSanitize(argDF):
    argDF['End_Time'] = pd.to_datetime(argDF['End_Time'], infer_datetime_format=True)
    argDF['Start_Time'] = pd.to_datetime(argDF['Start_Time'], infer_datetime_format = True)

    filterCB = lambda arg: re.sub(r'%', '', arg).strip()
    for theColumn in argDF:
        if theColumn != 'Start_Time' and theColumn !='End_Time' and r'%' in str(argDF[theColumn].loc[0]):
            argDF[theColumn] = pd.to_numeric(argDF[theColumn].apply(filterCB))
    return argDF

def saveIntoDB(dataFrame, tableName, db_connection):
    try:
        dataFrame.to_sql(tableName, con=db_connection, if_exists='append', index=False)
    except ValueError as vX:
        print(vX)
    except Exception as ex:
        print(ex)
    else:
        print('The operation succeeded')

# Lectura de los CSV:
KPIsRoute='./test_kpis'
KPIsBackUpRoute='./backup'

for dirPath, dirNames, fileNames in os.walk(KPIsRoute):
    for fileName in fileNames:
        fileRoute = os.path.join(dirPath, fileName)
        KPIDataFrame = pd.read_csv(fileRoute).drop(columns=['Index'])

        for column in KPIDataFrame.columns:
            newColumnName = re.sub(r'\(.*\)','', column.replace(' ', '_')).strip()
            KPIDataFrame.rename(columns={column:newColumnName}, inplace=True)

        KPIDataFrame = dfSanitize(KPIDataFrame)
        # print(KPIDataFrame.head())

        # shutil.move(fileRoute, KPIsBackUpRoute)

        if 'RAN' in fileName and (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d") in fileName:
            print(fileName)
            saveIntoDB(KPIDataFrame, 'ran_kpis', engine)


# Cierre de conexion
engine.dispose()
# conn = pyodbc.connect('Driver={SQL Server};'
#                      'Server=WIN-SNQUCAIM13C;'
#                      'Database=KPI_WAREHOUSE;'
#                      'Trusted_Connection=yes;')

# cursor = conn.cursor()
# cursor.execute('SELECT * FROM Test')

# for i in cursor:
#    print(i)