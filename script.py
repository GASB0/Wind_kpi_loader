import sqlalchemy as sa
import urllib
import datetime
import re
import os
import shutil
import pandas as pd
import zipfile

server='WIN-SNQUCAIM13C'
database = 'KPI_4G'
username= 'script_report'
password = '49*pHdW*I4R2x'

params = urllib.parse.quote_plus('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)

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

def saveIntoDB(dataFrame, tableName, db_connection, ifExistsAction):
    try:
        dataFrame.to_sql(tableName, con=db_connection, if_exists=ifExistsAction, index=False)
    except ValueError as vX:
        print(vX)
    except Exception as ex:
        print(ex)
    else:
        print('The operation succeeded')

# Calculando los promedios diarios
def dailyAvgCalculator(argDF):
    auxDF = pd.DataFrame(columns = argDF.columns)
    auxDic = dict()
    for column in argDF.columns:
        if pd.api.types.is_numeric_dtype(argDF[column]):
            auxDic[column] =[argDF[column].mean()]

    diffCols = list(set(list(argDF.columns)) - set(list(pd.DataFrame.from_dict(auxDic).columns)))
    resDF = pd.concat([auxDF, pd.DataFrame.from_dict(auxDic)])

    for column in diffCols:
        resDF[column] = argDF[column].iloc[0]

    resDF['End_Time'] = resDF['Start_Time'] + datetime.timedelta(days=1)

    return resDF

# Lectura de los CSV:
KPIsRoute='K:/KPI_WAREHOUSE_FILES/'
KPIsBackUpRoute='K:/BACKUPS/CSV_files/'

# Extraccion de los zips
for dirPath, dirNames, fileNames in os.walk(KPIsRoute):
    for fileName in fileNames:
        if r'.zip' in fileName:
            print('Removing the file: ', fileName)
            fileRoute = os.path.join(dirPath, fileName)
            with zipfile.ZipFile(fileRoute) as zip_ref:
                zip_ref.extractall(KPIsRoute)
            
            # print(fileRoute)
            os.remove(fileRoute)
            

# Estableciendo las fechas importantes
today = datetime.datetime.now()
yesterday = datetime.datetime(today.year, today.month, today.day) - datetime.timedelta(days=1)
today = yesterday + datetime.timedelta(days=1)

# Leyendo y cargando los CSVs a la base de datos
for dirPath, dirNames, fileNames in os.walk(KPIsRoute):
    for fileName in fileNames:
        fileRoute = os.path.join(dirPath, fileName)
        KPIDataFrame = pd.read_csv(fileRoute).drop(columns=['Index'])

        for column in KPIDataFrame.columns:
            newColumnName = re.sub(r'\(.*\)','', column.replace(' ', '_')).strip()
            KPIDataFrame.rename(columns={column:newColumnName}, inplace=True)

        KPIDataFrame = dfSanitize(KPIDataFrame)

        shutil.move(fileRoute, KPIsBackUpRoute)

        if 'RAN' in fileName: # and (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d") in fileName:
            saveIntoDB(KPIDataFrame, 'ran_kpis', engine, 'append') # Saving the found file
            saveCB = lambda df: saveIntoDB(df, 'yesterdays_ran_resume', engine, 'replace')
            KPIDataFrame.groupby('Cell_Name').apply(dailyAvgCalculator).pipe(saveCB)

        if 'RCP' in fileName: # and (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d") in fileName:
            print(fileName)
            saveIntoDB(KPIDataFrame, 'rcp_kpis', engine, 'append')
            yesterdays_rcp_kpis = KPIDataFrame[(KPIDataFrame['Start_Time'] < today) & (KPIDataFrame['Start_Time'] >= yesterday)]
            saveIntoDB(yesterdays_rcp_kpis, 'yesterdays_rcp_kpis', engine, 'replace')

        if 'USPP' in fileName: # and (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d") in fileName:
            print(fileName)
            saveIntoDB(KPIDataFrame, 'uspp_kpis', engine, 'append')
            yesterdays_uspp_kpis = KPIDataFrame[(KPIDataFrame['Start_Time'] < today) & (KPIDataFrame['Start_Time'] >= yesterday)]
            saveIntoDB(yesterdays_uspp_kpis, 'yesterdays_uspp_kpis', engine, 'replace')

        if 'xGW' in fileName: # and (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d") in fileName:
            print(fileName)
            saveIntoDB(KPIDataFrame, 'xgw_kpis', engine, 'append')
            yesterdays_xgw_kpis = KPIDataFrame[(KPIDataFrame['Start_Time'] < today) & (KPIDataFrame['Start_Time'] >= yesterday)]
            saveIntoDB(yesterdays_xgw_kpis, 'yesterdays_xgw_kpis', engine, 'replace')

        if 'MME' in fileName: # and (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d") in fileName:
            print(fileName)
            saveIntoDB(KPIDataFrame, 'mme_kpis', engine, 'append')
            yesterdays_mme_kpis = KPIDataFrame[(KPIDataFrame['Start_Time'] < today) & (KPIDataFrame['Start_Time'] >= yesterday)]
            saveIntoDB(yesterdays_mme_kpis, 'yesterdays_mme_kpis', engine, 'replace')

# Cierre de conexion
engine.dispose()
