import logging
import pyodbc
# from sevicios_app.vo.pedido import Pedido

logger = logging.getLogger('')

class WMSDaoQA():

    def getConexion(self):
        try:
            direccion_servidor = '192.168.84.162'
            nombre_bd = 'ILS'
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
        
    def registerContainerType(self,containerType,description, container_class, empty_weight, weight_um, length, width, height, active, weight_tolerance, maximum_weight):
        try:
            print(containerType,description, container_class, empty_weight, weight_um, length, width, height, active, weight_tolerance, maximum_weight)
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute("INSERT INTO CONTAINER_TYPE(CONTAINER_TYPE, DESCRIPTION, CONTAINER_CLASS, EMPTY_WEIGHT, WEIGHT_UM, LENGTH, WIDTH, HEIGHT, ACTIVE, WEIGHT_TOLERANCE, MAXIMUM_WEIGHT, USE_AS_DEFAULT, DATE_TIME_STAMP) " +
                           "VALUES( ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , 'N', GETDATE())", containerType, description, container_class, empty_weight, weight_um, length, width, height, active, weight_tolerance, maximum_weight)
            conexion.commit()
            print("Container registrado")
            return True
        except Exception as exception:
            logger.error(f"Se presento una incidencia al ejecutar el proceso de envio de correos a tiendas tiendas: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
                
    def deleteContainerType(self,containerType):
        try:
            print(containerType)
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute("DELETE CONTAINER_TYPE WHERE CONTAINER_TYPE='6942083514743I'", containerType)
            conexion.commit()
            print("Container registrado")
            return True
        except Exception as exception:
            logger.error(f"Se presento una incidencia al ejecutar el proceso de envio de correos a tiendas tiendas: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)