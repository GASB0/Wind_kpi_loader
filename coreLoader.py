
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

                shutil.move(fileRoute, KPIsBackUpRoute)

                startTime = time.time()

                if 'RCP' in fileName: # and (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d") in fileName:
                    print(fileName)
                    dbLoader.saveIntoDB(KPIDataFrame, 'rcp_kpis', dbLoader.engine, 'append')
                    dbLoader.logging.info('rcp_kpis saved successfully')
                    yesterdays_rcp_kpis = KPIDataFrame[(KPIDataFrame['Start_Time'] < today) & (KPIDataFrame['Start_Time'] >= yesterday)]
                    dbLoader.saveIntoDB(yesterdays_rcp_kpis, 'yesterdays_rcp_kpis', dbLoader.engine, 'replace')
                    dbLoader.logging.info('yesterdays_rcp_kpis saved successfully')

                if 'USPP' in fileName: # and (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d") in fileName:
                    print(fileName)
                    dbLoader.saveIntoDB(KPIDataFrame, 'uspp_kpis', dbLoader.engine, 'append')
                    dbLoader.logging.info('uspp_kpis saved successfully')
                    yesterdays_uspp_kpis = KPIDataFrame[(KPIDataFrame['Start_Time'] < today) & (KPIDataFrame['Start_Time'] >= yesterday)]
                    dbLoader.saveIntoDB(yesterdays_uspp_kpis, 'yesterdays_uspp_kpis', dbLoader.engine, 'replace')
                    dbLoader.logging.info('yesterdays_uspp_kpis saved successfully')

                if 'xGW' in fileName: # and (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d") in fileName:
                    print(fileName)
                    dbLoader.saveIntoDB(KPIDataFrame, 'xgw_kpis', dbLoader.engine, 'append')
                    dbLoader.logging.info('xgw_kpis saved successfully')
                    yesterdays_xgw_kpis = KPIDataFrame[(KPIDataFrame['Start_Time'] < today) & (KPIDataFrame['Start_Time'] >= yesterday)]
                    dbLoader.saveIntoDB(yesterdays_xgw_kpis, 'yesterdays_xgw_kpis', dbLoader.engine, 'replace')
                    dbLoader.logging.info('yesterdays_xgw_kpis saved successfully')

                if 'MME' in fileName: # and (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d") in fileName:
                    print(fileName)
                    dbLoader.saveIntoDB(KPIDataFrame, 'mme_kpis', dbLoader.engine, 'append')
                    dbLoader.logging.info('mme_kpis saved successfully')
                    yesterdays_mme_kpis = KPIDataFrame[(KPIDataFrame['Start_Time'] < today) & (KPIDataFrame['Start_Time'] >= yesterday)]
                    dbLoader.saveIntoDB(yesterdays_mme_kpis, 'yesterdays_mme_kpis', dbLoader.engine, 'replace')
                    dbLoader.logging.info('yesterdays_mme_kpis saved successfully')
    except BaseException as ex:
        print(ex)
        dbLoader.logging.error(ex)
        return -1
    # Cierre de conexion
    finally:
        dbLoader.engine.dispose()
    return 0


if __name__=='__main__':
    dbLoader.logging.info(20*'*'+ 'CORE logging corresponding to '+ str(datetime.datetime.now()) + 20*'*')
    if main() != 0:
        print('Abnormal execution, refer to the logs for more information')
        dbLoader.logging.error('Something went wrong')
    else:
        print('Successful execution')
        dbLoader.logging.info('Successful execution')