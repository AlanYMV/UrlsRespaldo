import logging
import pyodbc
from sevicios_app.vo.planeacion import Planeacion
from sevicios_app.vo.datoDash import DatoDash
logger = logging.getLogger('')

class PlaneacionDao():

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

    def getPlaneacion(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            planeacionList=[]
            cursor.execute("SELECT SUBINDICE, UNIDAD, ORDEN, NUM_TIENDA, TIENDA, TIPO, OLA, ID, CANT_SOL, VOLUMEN, NUM_CONTENEDORES, DIA_CARGA, DIA_ARRIBO, HORA_ARRIBO, INICIO_DESCARGA, FIN_DESCARGA, FIN_PROCESO_ADMIN FROM PLANEACION")
            registros=cursor.fetchall()
            for registro in registros:
                print(f'{registro[11]} - {registro[12]} - {registro[13]} - {registro[14]} - {registro[15]} - {registro[16]}')
                planeacion=Planeacion(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11], registro[12], registro[13], registro[14], registro[15], registro[16])
                planeacionList.append(planeacion)
            return planeacionList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def updateSolicitudVacaciones(self, folio, estatus):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute("UPDATE Solicitud_vacaciones SET Motivo=? WHERE Folio=?", (estatus, folio))
            conexion.commit()
            return True
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def updateSolicitudPermiso(self, folio, estatus):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute("UPDATE Solicitudes_Permisos SET Status_solicitud=? WHERE Folio=?", (estatus, folio))
            conexion.commit()
            return True
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getDatosDash(self, mes, gastoDistribucion, venta, contenedoresEmbarcados, pedidosEmbarcados, rentaMensual, inventarioMensual, dias, ontime, fillRate, leadTime, dato1RatioEntradas, dato2RatioEntradas, dato1RatioSalidas, dato2RatioSalidas, ticketsReportados, piezasReportadas, rotacionStocks, stockBajaRotacion, stockSinRotacion):
        try:
            print('Llego al DAO')
            conexion=self.getConexion()
            cursor=conexion.cursor()
            datosDashList=[]
            numParametros=0
            consulta1 = "SELECT MES, GASTO_DISTRIBUCION, VENTA, CONENEDORES_EMBARCADOS, PEDIDOS_EMBARCADOS,RENTA_MENSUAL, INVENTARIO_MENSUAL, DIAS, ONTIME, "
            consulta2 = "Fill_Rate, Lead_Time, Dato1_Ratio_entradas, Dato2_Ratio_entradas, Dato1_Ratio_salidas, Dato2_Ratio_salidas, Tickets_reportados, Piezas_reportadas, "
            consulta3 = "Rotacion_stocks, Stock_baja_rotacion, Stock_sin_rotacion "
            consulta4 = "FROM DATOS_DASH"
            consulta= consulta1+consulta2+consulta3+consulta4
            if mes != '' and mes !=None:
                consulta=consulta+ " WHERE MES = '"+mes+"'"
                numParametros +=1
            if gastoDistribucion != ''  and gastoDistribucion !=None:
                if numParametros>0:
                    consulta=consulta+ " AND"
                else:
                    consulta=consulta+ " WHERE"
                consulta=consulta+ " GASTO_DISTRIBUCION = '"+gastoDistribucion+"'"
                numParametros +=1
            if venta != ''  and venta !=None:
                if numParametros>0:
                    consulta=consulta+ " AND"
                else:
                    consulta=consulta+ " WHERE"
                consulta=consulta+ " VENTA = '"+venta+"'"
                numParametros +=1
            if contenedoresEmbarcados != ''  and contenedoresEmbarcados !=None:
                if numParametros>0:
                    consulta=consulta+ " AND"
                else:
                    consulta=consulta+ " WHERE"
                consulta=consulta+ " CONENEDORES_EMBARCADOS = '"+contenedoresEmbarcados+"'"
                numParametros +=1
            if pedidosEmbarcados != ''  and pedidosEmbarcados !=None:
                if numParametros>0:
                    consulta=consulta+ " AND"
                else:
                    consulta=consulta+ " WHERE"
                consulta=consulta+ " PEDIDOS_EMBARCADOS = '"+pedidosEmbarcados+"'"
                numParametros +=1
            if rentaMensual != ''  and rentaMensual !=None:
                if numParametros>0:
                    consulta=consulta+ " AND"
                else:
                    consulta=consulta+ " WHERE"
                consulta=consulta+ " RENTA_MENSUAL = '"+rentaMensual+"'"
                numParametros +=1
            if inventarioMensual != ''  and inventarioMensual !=None:
                if numParametros>0:
                    consulta=consulta+ " AND"
                else:
                    consulta=consulta+ " WHERE"
                consulta=consulta+ " INVENTARIO_MENSUAL = '"+inventarioMensual+"'"
                numParametros +=1
            if dias != ''  and dias !=None:
                if numParametros>0:
                    consulta=consulta+ " AND"
                else:
                    consulta=consulta+ " WHERE"
                consulta=consulta+ " DIAS = '"+dias+"'"
                numParametros +=1
            if ontime != ''  and ontime !=None:
                if numParametros>0:
                    consulta=consulta+ " AND"
                else:
                    consulta=consulta+ " WHERE"
                consulta=consulta+ " ON_TIME = '"+ontime+"'"
                numParametros +=1
            if fillRate != ''  and fillRate !=None:
                if numParametros>0:
                    consulta=consulta+ " AND"
                else:
                    consulta=consulta+ " WHERE"
                consulta=consulta+ " FILL_RATE = '"+fillRate+"'"
                numParametros +=1
            if leadTime != ''  and leadTime !=None:
                if numParametros>0:
                    consulta=consulta+ " AND"
                else:
                    consulta=consulta+ " WHERE"
                consulta=consulta+ " LEAD_TIME = '"+leadTime+"'"
                numParametros +=1
            if dato1RatioEntradas != ''  and dato1RatioEntradas !=None:
                if numParametros>0:
                    consulta=consulta+ " AND"
                else:
                    consulta=consulta+ " WHERE"
                consulta=consulta+ " DATO1_RATIO_ENTRADAS = '"+dato1RatioEntradas+"'"
                numParametros +=1
            if dato2RatioEntradas != ''  and dato2RatioEntradas !=None:
                if numParametros>0:
                    consulta=consulta+ " AND"
                else:
                    consulta=consulta+ " WHERE"
                consulta=consulta+ " DATO2_RATIO_ENTRADAS = '"+dato2RatioEntradas+"'"
                numParametros +=1
            if dato1RatioSalidas != ''  and dato1RatioSalidas !=None:
                if numParametros>0:
                    consulta=consulta+ " AND"
                else:
                    consulta=consulta+ " WHERE"
                consulta=consulta+ " DATO1_RATIO_SALIDAS = '"+dato1RatioSalidas+"'"
                numParametros +=1
            if dato2RatioSalidas != ''  and dato2RatioSalidas !=None:
                if numParametros>0:
                    consulta=consulta+ " AND"
                else:
                    consulta=consulta+ " WHERE"
                consulta=consulta+ " DATO2_RATIO_SALIDAS = '"+dato2RatioSalidas+"'"
                numParametros +=1
            if ticketsReportados != ''  and ticketsReportados !=None:
                if numParametros>0:
                    consulta=consulta+ " AND"
                else:
                    consulta=consulta+ " WHERE"
                consulta=consulta+ " TICKETS_REPORTADOS = '"+ticketsReportados+"'"
                numParametros +=1
            if piezasReportadas != ''  and piezasReportadas !=None:
                if numParametros>0:
                    consulta=consulta+ " AND"
                else:
                    consulta=consulta+ " WHERE"
                consulta=consulta+ " PIEZAS_REPORTADAS = '"+piezasReportadas+"'"
                numParametros +=1
            if rotacionStocks != ''  and rotacionStocks !=None:
                if numParametros>0:
                    consulta=consulta+ " AND"
                else:
                    consulta=consulta+ " WHERE"
                consulta=consulta+ " ROTACION_STOCKS = '"+rotacionStocks+"'"
                numParametros +=1
            if stockBajaRotacion != ''  and stockBajaRotacion !=None:
                if numParametros>0:
                    consulta=consulta+ " AND"
                else:
                    consulta=consulta+ " WHERE"
                consulta=consulta+ " STOCK_BAJA_ROTACION = '"+stockBajaRotacion+"'"
                numParametros +=1
            if stockSinRotacion != ''  and stockSinRotacion !=None:
                if numParametros>0:
                    consulta=consulta+ " AND"
                else:
                    consulta=consulta+ " WHERE"
                consulta=consulta+ " STOCK_SON_ROTACION = '"+stockSinRotacion+"'"
                numParametros +=1

            cursor.execute(consulta)
            registros=cursor.fetchall()
            for registro in registros:
                datoDash=DatoDash(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11], registro[12], registro[13], registro[14], registro[15], registro[16], registro[17], registro[18], registro[19])
                datosDashList.append(datoDash)
            return datosDashList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
