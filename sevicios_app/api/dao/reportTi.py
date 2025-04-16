import logging
import pyodbc
from sevicios_app.vo.ReportTiBugsRelevant import ReportTiBugsRelevant
from sevicios_app.vo.reportTiDeviceRepair import ReportTiDeviceRepair


logger = logging.getLogger('')

class ReportTi():

    def getConexion(self):
        try:
            direccion_servidor = '192.168.84.162'
            nombre_bd = 'PLANEACION'
            nombre_usuario = 'manh'
            password = 'Pa$$w0rdLDM'
            conexion = None

            conexion = pyodbc.connect('DRIVER={SQL Server};SERVER=' + direccion_servidor+';DATABASE='+nombre_bd+';UID='+nombre_usuario+';PWD=' + password)
            return conexion
        except Exception as exception:
            logger.error(f"Se presento un error al establecer la conexion: {exception}")
            raise exception

    def closeConexion(self, conexion):
        try:
            conexion.close()
        except Exception as exception:
            logger.error(f"Se presento una incidencia al cerrar la conexion: {exception}")
            raise exception
        
    def getBugsRelevant(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            relevantList=[]
            cursor.execute("select DESCRIPTION,STATUS,COMMENTS from TI_BUGS_RELEVANT")
            registros=cursor.fetchall()
            for registro in registros:
                bugs=ReportTiBugsRelevant(registro[0],registro[1],registro[2])
                relevantList.append(bugs)
            return relevantList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
                
    def getDeviceRepair(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            relevantList=[]
            cursor.execute("select TOTAL_DEVICES,LOCATION,STATUS,ESTIMATED_DATE,COMMENTS from TI_DEVICE_REPAIR")
            registros=cursor.fetchall()
            for registro in registros:
                bugs=ReportTiDeviceRepair(registro[0],registro[1],registro[2],registro[3],registro[4])
                relevantList.append(bugs)
            return relevantList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
                
    def getFuntionalDevices(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            relevantList=[]
            cursor.execute("select DEVICE,TOTAL,ANOTHER_LOCATION,REPAIR,FUNCTIONAL_DEVICES from TI_FUNCTIONAL_DEVICES")
            registros=cursor.fetchall()
            for registro in registros:
                bugs=ReportTiDeviceRepair(registro[0],registro[1],registro[2],registro[3],registro[4])
                relevantList.append(bugs)
            return relevantList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
                
    def getUsePercetage(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            relevantList=[]
            cursor.execute("select FIRST_SHIFT,SECOND_SHIFT,THIRD_SHIFT from TI_PERCETAGE_USE")
            registros=cursor.fetchall()
            for registro in registros:
                bugs=ReportTiBugsRelevant(registro[0],registro[1],registro[2])
                relevantList.append(bugs)
            return relevantList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)