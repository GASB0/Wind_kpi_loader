import sqlalchemy as sa
import urllib
import logging
import datetime
import re
import os
import pandas as pd
import zipfile
import time

# Logging settings
try:
    logPath = 'C:/Users/Administrator/Documents/Scripts/databaseLoader/messages.log'
    print(logPath)
    if not os.path.exists(logPath):
        raise ValueError('The specified path does not seem to exists')
    logging.basicConfig(filename=logPath, level=logging.INFO)
except BaseException as e:
    print('Errors while trying to configure the logging module, check it\'s configuration...')
    time.sleep(3)
    print(e)
    input('Press enter to exit')
    raise FileExistsError

# DB settings
server='WIN-SNQUCAIM13C'
database = 'KPI_4G'
username= 'script_report'
password = '49*pHdW*I4R2x'
params = urllib.parse.quote_plus('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)
engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s"%params)


# Misc functions
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

def fileExtractor(KPIsRoute)->bool:
    # Extraccion de los zips
    startTime = time.time()
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
                        
                    logging.info(f'The file {fileName} has been extracted successfuly')
                    os.remove(fileRoute)

        endTime = time.time()
        messageString = f'{len(filesToExtract)} files have been extracted in {endTime - startTime} seconds'
        print(messageString)
        logging.info(messageString)

    except Exception as ex:
        logging.error('Something went wrong while reading the route to the KPI files: '+ KPIsRoute)
        logging.error(ex)
        return False

    return True