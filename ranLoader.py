import datetime
import re
import os
import shutil
import pandas as pd
import time
import dbLoader

def main():
    # Engine connection
    try:
        dbLoader.engine.connect()
        dbLoader.logging.info(f'Succesfully connected to {dbLoader.database}')
    except:
        dbLoader.logging.error('Failed to connect to database. Check your db settings.')
        return 0

    # Lectura de los CSV:
    KPIsRoute='K:/KPI_WAREHOUSE_FILES/'
    KPIsBackUpRoute='K:/BACKUPS/CSV_files/'

    # Extraccion de los zips
    dbLoader.fileExtractor(KPIsRoute)

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

                KPIDataFrame = dbLoader.dfSanitize(KPIDataFrame)

                startTime = time.time()

                if 'RAN' in fileName:
                    shutil.move(fileRoute, KPIsBackUpRoute)
                    dbLoader.saveIntoDB(KPIDataFrame, 'ran_kpis', dbLoader.engine, 'append')
                    dbLoader.logging.info('ran_kpis saved successfully')
                    saveCB = lambda df: dbLoader.saveIntoDB(df, 'daily_ran_summary', dbLoader.engine, 'append')
                    KPIDataFrame.groupby('Cell_Name').apply(dbLoader.dailyAvgCalculator).pipe(saveCB)
                    dbLoader.logging.info('daily_ran_summary saved successfully')

    except BaseException as ex:
        print(ex)
        dbLoader.logging.error(ex)
        return -1
    # Cierre de conexion
    finally:
        dbLoader.engine.dispose()
    return 0

if __name__=='__main__':
    dbLoader.logging.info(20*'*'+ 'RAN logging corresponding to '+ str(datetime.datetime.now()) + 20*'*')
    if main() != 0:
        print('Abnormal execution, refer to the logs for more information')
        dbLoader.logging.error('Something went wrong')
    else:
        print('Successful execution')
        dbLoader.logging.info('Successful execution')