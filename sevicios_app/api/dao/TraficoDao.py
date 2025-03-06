import logging
import pyodbc
from sevicios_app.vo.contenedorSalidaDetalle import ContenedorSalidaDetalle
from sevicios_app.vo.contenedorSalida import ContenedorSalida

logger = logging.getLogger('')

class TraficoDao():

    def getConexion(self):
        try:
            direccion_servidor = '192.168.84.107'
            nombre_bd = 'TRAFICO'
            nombre_usuario = 'sa'
            password = 'M1n150#!'
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
        
    def getContenedorSalidaById(self, numContenedorSalida):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            contenedorSalidaDetalleList=[]
            cursor.execute("SELECT CSD.FRACCION+CSD.NICO, PLD.SKU, PLD.CODIGO_BARRAS, SS.CODIGO_SAT, SS.DESCRIPCION, PLD.PIEZAS, CSD.UMC, PLD.PESO, CSD.PEDIMENTO, UM.DESCRIPCION, UM.CAT_SAT, TC.CLAVE_SAT "+
                           "FROM CONTENEDOR_SALIDA CS "+
                           "INNER JOIN CONTENEDOR_SALIDA_DETAIL CSD ON CSD.ID_CONTENEDOR_SALIDA = CS.ID_CONTENEDOR_SALIDA "+
                           "INNER JOIN PACKING_LIST PL ON PL.CONTENEDOR = CSD.CONTENEDOR "+
                           "INNER JOIN PACKING_LIST_DETAIL PLD ON PLD.ID_PACKING_LIST = PL.ID_PACKING_LIST AND CSD.SKU = PLD.CODIGO_BARRAS "+
                           "INNER JOIN SKU_SAP SS ON SS.SKU = PLD.SKU "+
                           "LEFT JOIN TIPO_CONTENEDOR TC ON TC.DESCRIPCION =PLD.TIPO_EMPAQUE "+
                           "LEFT JOIN UNIDAD_MEDIDA UM ON UM.CLAVE = CSD.UMC "+
                           "WHERE CS.CONTENEDOR_SALIDA=?", (numContenedorSalida))
            registros=cursor.fetchall()
            for registro in registros:
                contenedorSalidaDetalle=ContenedorSalidaDetalle(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11])
                contenedorSalidaDetalleList.append(contenedorSalidaDetalle)
            return contenedorSalidaDetalleList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getLastTwentyContenedoresSalida(self, idContenedorSalida):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            contenedorSalidaList=[]
            cadena=""
            if idContenedorSalida!=None:
                cadena= "where SA.CONTENEDOR_SALIDA like '%" + idContenedorSalida+"%' "
            cursor.execute("SELECT TOP 20 SA.CONTENEDOR_SALIDA "+
                           "FROM CONTENEDOR_SALIDA SA "+ cadena +
                           "ORDER BY SA.FECHA_CARGA DESC")
            registros=cursor.fetchall()
            for registro in registros:
                contenedorSalida=ContenedorSalida(registro[0])
                contenedorSalidaList.append(contenedorSalida)
            return contenedorSalidaList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
