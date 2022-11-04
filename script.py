import sqlalchemy as sa
import urllib
import logging
import datetime
import re
import os
import shutil
import pandas as pd
import zipfile

# Logging settings
logging.basicConfig(filename='./messages.log', encoding='utf-8', level=logging.INFO)
logging.info(20*'*'+ 'Logging corresponding to '+ str(datetime.datetime.now()) + 20*'*')

# DB settings
server='WIN-SNQUCAIM13C'
database = 'KPI_4G'
username= 'script_report'
password = '49*pHdW*I4R2x'
params = urllib.parse.quote_plus('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)

try:
    engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s"%params)
    engine.connect()
    logging.info(f'Succesfully connected to {database}')
except Exception as ex:
    logging.error('Failed to connect to database. Check your db settings.')


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

def main():
    # Lectura de los CSV:
    KPIsRoute='K:/KPI_WAREHOUSE_FILES/'
    KPIsBackUpRoute='K:/BACKUPS/CSV_files/'

    # Extraccion de los zips
    try:
        if not os.path.exists(KPIsRoute):
            raise Exception('The path doesn\'t exist')

        print('Extracting zip files from KPI repository:') 
        filesToExtract = []
        for dirPath, dirNames, fileNames in os.walk(KPIsRoute):
            for fileName in fileNames:
                if r'.zip' in fileName:
                    print('Extracting:', fileName)
                    filesToExtract.append(fileName)
                    fileRoute = os.path.join(dirPath, fileName)
                    with zipfile.ZipFile(fileRoute) as zip_ref:
                        zip_ref.extractall(KPIsRoute)
                    
                    os.remove(fileRoute)

        print(f'{len(filesToExtract)} files have been extracted')
    except Exception as ex:
        logging.error('Something went wrong while reading the route to the KPI files: '+ KPIsRoute)
        logging.error(ex)
        return -1

    # Estableciendo las fechas importantes
    today = datetime.datetime.now()
    yesterday = datetime.datetime(today.year, today.month, today.day) - datetime.timedelta(days=1)
    today = yesterday + datetime.timedelta(days=1)

# Leyendo y cargando los CSVs a la base de datos
    try:
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
                    logging.info('ran_kpis saved successfully')
                    saveCB = lambda df: saveIntoDB(df, 'daily_ran_summary', engine, 'append')
                    KPIDataFrame.groupby('Cell_Name').apply(dailyAvgCalculator).pipe(saveCB)
                    logging.info('daily_ran_summary saved successfully')

                if 'RCP' in fileName: # and (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d") in fileName:
                    print(fileName)
                    saveIntoDB(KPIDataFrame, 'rcp_kpis', engine, 'append')
                    logging.info('rcp_kpis saved successfully')
                    yesterdays_rcp_kpis = KPIDataFrame[(KPIDataFrame['Start_Time'] < today) & (KPIDataFrame['Start_Time'] >= yesterday)]
                    saveIntoDB(yesterdays_rcp_kpis, 'yesterdays_rcp_kpis', engine, 'replace')
                    logging.info('yesterdays_rcp_kpis saved successfully')

                if 'USPP' in fileName: # and (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d") in fileName:
                    print(fileName)
                    saveIntoDB(KPIDataFrame, 'uspp_kpis', engine, 'append')
                    logging.info('uspp_kpis saved successfully')
                    yesterdays_uspp_kpis = KPIDataFrame[(KPIDataFrame['Start_Time'] < today) & (KPIDataFrame['Start_Time'] >= yesterday)]
                    saveIntoDB(yesterdays_uspp_kpis, 'yesterdays_uspp_kpis', engine, 'replace')
                    logging.info('yesterdays_uspp_kpis saved successfully')

                if 'xGW' in fileName: # and (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d") in fileName:
                    print(fileName)
                    saveIntoDB(KPIDataFrame, 'xgw_kpis', engine, 'append')
                    logging.info('xgw_kpis saved successfully')
                    yesterdays_xgw_kpis = KPIDataFrame[(KPIDataFrame['Start_Time'] < today) & (KPIDataFrame['Start_Time'] >= yesterday)]
                    saveIntoDB(yesterdays_xgw_kpis, 'yesterdays_xgw_kpis', engine, 'replace')
                    logging.info('yesterdays_xgw_kpis saved successfully')

                if 'MME' in fileName: # and (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d") in fileName:
                    print(fileName)
                    saveIntoDB(KPIDataFrame, 'mme_kpis', engine, 'append')
                    logging.info('mme_kpis saved successfully')
                    yesterdays_mme_kpis = KPIDataFrame[(KPIDataFrame['Start_Time'] < today) & (KPIDataFrame['Start_Time'] >= yesterday)]
                    saveIntoDB(yesterdays_mme_kpis, 'yesterdays_mme_kpis', engine, 'replace')
                    logging.info('yesterdays_mme_kpis saved successfully')

    # Cierre de conexion
    except Exception as ex:
        print(ex)
        logging.error(ex)
        return -1
    finally:
        engine.dispose()
    return 0

if __name__=='__main__':
    if main() != 0:
        print('Abnormal execution, refer to the logs for more information')
        logging.error('Something went wrong')
    else:
        print('Successful execution')
        logging.info('Successful execution')