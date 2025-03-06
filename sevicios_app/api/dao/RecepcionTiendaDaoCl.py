import logging
import pyodbc
from sevicios_app.vo.detallePedidoTiendaCl import DetallePedidoTiendaCl
from sevicios_app.vo.pedidoTienda import PedidoTienda
from sevicios_app.vo.solicitudTraslado import SolicitudTraslado
from sevicios_app.vo.tiendaPendiente import TiendaPendiente
from sevicios_app.vo.tablaTiendaPendiente import TablaTiendaPendiente
from sevicios_app.vo.tiendaPendienteFecha import TiendaPendienteFecha
from sevicios_app.vo.pedidoPorCerrar import PedidoPorCerrar
from sevicios_app.vo.reciboTienda import ReciboTienda
from sevicios_app.vo.tienda import Tienda
from sevicios_app.vo.confirmationPending import ConfirmationPending
from sevicios_app.vo.auditoriaTiendaCl import AuditoriaTiendaCl
from sevicios_app.vo.orderAudi import OrderAudi
from sevicios_app.vo.subfamilyOrder import SubFamilyOrder

logger = logging.getLogger('')

class RecepcionTiendaDaoCl():

    def getConexion(self):
        try:
            direccion_servidor = '192.168.84.179'
            nombre_bd = 'recepciontiendacl'
            nombre_usuario = 'sa'
            password = 'Mini$o2021!#'
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
        
    def executeActualizarTablasTiendas(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute("EXEC SP_REPORTE_TIENDAS_DETALLE_PEDIDO")
            conexion.commit()
            cursor.execute("EXEC SP_REPORTE_TIENDAS_STATUS_PEDIDO")
            conexion.commit()
            cursor.execute("EXEC SP_REPORTE_TIENDAS_TRASLADO")
            conexion.commit()
            cursor.execute("EXEC SP_REPORTE_TIENDAS_PENDIENTES")
            conexion.commit()
            return True
        except Exception as exception:
            logger.error(f"Se presento una incidencia al ejecutar el proceso de reporte de tiendas: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
                
    def getDetallePedido(self, tienda, carga, pedido, contenedor, articulo, estatusContenedor):
        try:
            print('Entro DAO')
            conexion=self.getConexion()
            cursor=conexion.cursor()
            detallePedidoList=[]
            numParametros=0
            consulta="SELECT STS_SOL_NAME, ALMACENNOMBRE, SOLICITUDNOTRANSPORTE, SOLICITUDID, CONTENEDORID, ARTICULOCVE, ARTICULODESC, STS_CONT_NAME, ARTICULOQTY, QC from DetallePedidos where"
            if tienda != '':
                consulta=consulta+ " ALMACENNOMBRE LIKE '%"+tienda+"%'"
                numParametros +=1
            if carga != '':
                if numParametros>0:
                    consulta=consulta+ " AND"
                consulta=consulta+ " SOLICITUDNOTRANSPORTE LIKE '%"+carga+"%'"
                numParametros +=1
            if pedido != '':
                if numParametros>0:
                    consulta=consulta+ " AND"
                consulta=consulta+ " SOLICITUDID LIKE '%"+pedido+"%'"
                numParametros +=1
            if contenedor != '':
                if numParametros>0:
                    consulta=consulta+ " AND"
                consulta=consulta+ " CONTENEDORID LIKE '%"+contenedor+"%'"
                numParametros +=1
            if articulo != '':
                if numParametros>0:
                    consulta=consulta+ " AND"
                consulta=consulta+ " ARTICULOCVE LIKE '%"+articulo+"%'"
            if estatusContenedor != '':
                if numParametros>0:
                    consulta=consulta+ " AND"
                consulta=consulta+ " STS_CONT_NAME = '"+estatusContenedor+"'"
            print(consulta)             
            cursor.execute(consulta)
            registros=cursor.fetchall()
            for registro in registros:
                detallePedidoTienda=DetallePedidoTiendaCl(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9])
                detallePedidoList.append(detallePedidoTienda)
            return detallePedidoList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getPedido(self, tienda, carga, pedido):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            pedidoList=[]
            numParametros=0
            consulta="SELECT ANIO, ESTATUS, MES, FECHA_CEDIS, ID_CARGA, TIENDA, PEDIDO, TIPO_PEDIDO, SOLICITUD_TRASLADO, CONTENEDORES_PENDIENTES, CONTENEDORES_RECIBIDOS, ITEMS_PENDIENTES, PIEZAS_PENDIENTES, ITEMS_RECIBIDOS, PIEZAS_RECIBIDAS FROM STATUS_PEDIDO WHERE"
            if tienda != '':
                consulta=consulta+ " TIENDA LIKE '%"+tienda+"%'"
                numParametros +=1
            if carga != '':
                if numParametros>0:
                    consulta=consulta+ " AND"
                consulta=consulta+ " ID_CARGA LIKE '%"+carga+"%'"
                numParametros +=1
            if pedido != '':
                if numParametros>0:
                    consulta=consulta+ " AND"
                consulta=consulta+ " PEDIDO LIKE '%"+pedido+"%'"
            logger.error(consulta)    
            cursor.execute(consulta)
            registros=cursor.fetchall()
            for registro in registros:
                pedidoTienda=PedidoTienda(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11], registro[12], registro[13], registro[14])
                pedidoList.append(pedidoTienda)
            return pedidoList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getSolicitudTraslado(self, documento, origenSolicitud, destinoSolicitud, origenTraslado, destinoTraslado):
        try:
            logger.error("acceso dao")
            conexion=self.getConexion()
            cursor=conexion.cursor()
            solicitudTrasladoList=[]
            numParametros=0
            consulta="SELECT DOCUMENTO_SOLICITUD, STATUS, ORIGEN_SOLICITUD, DESTINO_SOLICITUD, ARTICULOS_SOLICITUD, CANTIDAD_SOLICITUD, COMENTARIOS, FECHA_SOLICITUD, FECHA_VENCIMIENTO, ORIGEN_TRASLADO, DESTINO_TRASLADO, ARTICULOS_TRASLADO, CANTIDAD_TRASLADO FROM TRASLADO_SAP WHERE"
            if documento != '':
                consulta=consulta+ " DOCUMENTO_SOLICITUD LIKE '%"+documento+"%'"
                numParametros +=1
            if origenSolicitud != '':
                if numParametros>0:
                    consulta=consulta+ " AND"
                consulta=consulta+ " ORIGEN_SOLICITUD LIKE '%"+origenSolicitud+"%'"
                numParametros +=1
            if destinoSolicitud != '':
                if numParametros>0:
                    consulta=consulta+ " AND"
                consulta=consulta+ " DESTINO_SOLICITUD LIKE '%"+destinoSolicitud+"%'"
                numParametros +=1
            if origenTraslado != '':
                if numParametros>0:
                    consulta=consulta+ " AND"
                consulta=consulta+ " ORIGEN_TRASLADO LIKE '%"+origenTraslado+"%'"
                numParametros +=1
            if destinoTraslado != '':
                if numParametros>0:
                    consulta=consulta+ " AND"
                consulta=consulta+ " DESTINO_TRASLADO LIKE '%"+destinoTraslado+"%'"
            cursor.execute(consulta)
            registros=cursor.fetchall()
            for registro in registros:
                solicitudTraslado=SolicitudTraslado(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11], registro[12])
                solicitudTrasladoList.append(solicitudTraslado)
            return solicitudTrasladoList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getTiendasPendientes(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            tiendaPendienteList=[]
            numParamteros=0
            cursor.execute("SELECT TIPE.SOLICITUD_ESTATUS, TIPE.CARGA, TIPE.PEDIDO, TIPE.NOMBRE_ALMACEN, TIPE.FECHA_EMBARQUE, TIPE.FECHA_PLANEADA, TIPE.TRANSITO, TIPE.CROSS_DOCK, TIPE.FECHA_ENTREGA "+
                           "FROM(select TP.SOLICITUD_ESTATUS, TP.CARGA, TP.PEDIDO, TP.NOMBRE_ALMACEN, TP.FECHA_EMBARQUE, TP.FECHA_PLANEADA, TP.TRANSITO, TP.CROSS_DOCK, ISNULL((SELECT FECHA_REAL FROM FECHA_TRAFICO FT WHERE FT.CARGA=TP.CARGA AND FT.PEDIDO=TP.PEDIDO), TP.FECHA_PLANEADA) FECHA_ENTREGA from TIENDA_PENDIENTE TP) TIPE "+
                           "WHERE TIPE.SOLICITUD_ESTATUS='EN TRANSITO' AND TIPE.FECHA_ENTREGA<=GETDATE() "+
                           "ORDER BY TIPE.FECHA_ENTREGA")
            registros=cursor.fetchall()
            for registro in registros:
                tiendaPendiente=TiendaPendiente(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8])
                tiendaPendienteList.append(tiendaPendiente)
            return tiendaPendienteList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def executeEnvioCorreos(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute("EXEC SP_ENVIO_CORREO_TIENDAS_PENDIENTES")
            conexion.commit()
            return True
        except Exception as exception:
            logger.error(f"Se presento una incidencia al ejecutar el proceso de envio de correos a tiendas tiendas: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getTablaTiendasPendientes(self, fecha):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            tablastiendaPendienteList=[]
            numParamteros=0
            cursor.execute("SELECT TIPE.NOMBRE_ALMACEN, MIN(TIPE.FECHA_ENTREGA) FECHA_ENTREGA,  COUNT(TIPE.PEDIDO) NUM_PEDIDOS, SUM(TIPE.TRANSITO+TIPE.CROSS_DOCK) PIEZAS, SUM(TIPE.SolicitudTotalContenedores) CONTENEDORES "+
                           "FROM(select TP.SOLICITUD_ESTATUS, TP.CARGA, TP.PEDIDO, TP.NOMBRE_ALMACEN, TP.FECHA_EMBARQUE, TP.FECHA_PLANEADA, TP.TRANSITO, TP.CROSS_DOCK, ISNULL((SELECT FECHA_REAL FROM FECHA_TRAFICO FT WHERE FT.CARGA=TP.CARGA AND FT.PEDIDO=TP.PEDIDO), TP.FECHA_PLANEADA) FECHA_ENTREGA, SO.SolicitudTotalContenedores "+
                           "from TIENDA_PENDIENTE TP "+
                           "inner join Solicitud SO ON SO.SolicitudNoTransporte=TP.CARGA AND SO.SolicitudID=TP.PEDIDO) TIPE "+ 
                           "WHERE TIPE.SOLICITUD_ESTATUS='EN TRANSITO' AND format(TIPE.FECHA_ENTREGA, 'yyyy-MM-dd')<= ? "+
                           "GROUP BY TIPE.NOMBRE_ALMACEN ORDER BY FECHA_ENTREGA", (fecha))
            registros=cursor.fetchall()
            for registro in registros:
                tablaTiendaPendiente=TablaTiendaPendiente(registro[0], registro[1], registro[2], registro[3], registro[4])
                tablastiendaPendienteList.append(tablaTiendaPendiente)
            return tablastiendaPendienteList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getTiendasPendientesFecha(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            tiendasPendientesFechaList=[]
            cursor.execute("SELECT TIPE.CARGA, TIPE.PEDIDO, TIPE.NOMBRE_ALMACEN, TIPE.FECHA_EMBARQUE, TIPE.FECHA_ENTREGA "+
                           "FROM(select TP.SOLICITUD_ESTATUS, TP.CARGA, TP.PEDIDO, TP.NOMBRE_ALMACEN, TP.FECHA_EMBARQUE, TP.FECHA_PLANEADA, TP.TRANSITO, TP.CROSS_DOCK, ISNULL((SELECT FECHA_REAL FROM FECHA_TRAFICO FT WHERE FT.CARGA=TP.CARGA AND FT.PEDIDO=TP.PEDIDO), TP.FECHA_PLANEADA) FECHA_ENTREGA from TIENDA_PENDIENTE TP) TIPE "+
                           "LEFT JOIN FECHA_TRAFICO FTR ON FTR.CARGA=TIPE.CARGA AND FTR.PEDIDO=TIPE.PEDIDO "+
                           "WHERE TIPE.SOLICITUD_ESTATUS='EN TRANSITO' AND FORMAT(TIPE.FECHA_ENTREGA, 'yyyy-MM-dd')<format(GETDATE(), 'yyyy-MM-dd') "+
                           "AND (FORMAT(FTR.FECHA_REGISTRO, 'yyyy-MM-dd')!=FORMAT(GETDATE(), 'yyyy-MM-dd') OR FTR.FECHA_REGISTRO IS NULL) "+
                           "ORDER BY TIPE.FECHA_ENTREGA, TIPE.NOMBRE_ALMACEN, TIPE.PEDIDO")
            registros=cursor.fetchall()
            for registro in registros:
                tiendaPendienteFecha=TiendaPendienteFecha(registro[0], registro[1], registro[2], registro[3], registro[4])
                tiendasPendientesFechaList.append(tiendaPendienteFecha)
            return tiendasPendientesFechaList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def insertFechaTrafico(self, fecha, carga, pedido):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute("DELETE FECHA_TRAFICO WHERE CARGA=? AND PEDIDO=?", (carga, pedido))
            cursor.execute("INSERT INTO FECHA_TRAFICO (CARGA, PEDIDO, FECHA_REAL, FECHA_REGISTRO) VALUES(?, ?, ?, GETDATE())", (carga, pedido, fecha))
            conexion.commit()
            return True
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getPedidosPorCerrar(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            pedidopPorCerrarList=[]
            cursor.execute("SELECT SP.PEDIDO, SP.ESTATUS, SP.TIENDA, SP.ID_CARGA, TS.DOCUMENTO_SOLICITUD, TS.STATUS "+
                           "FROM STATUS_PEDIDO SP INNER JOIN TRASLADO_SAP TS ON TS.DOCUMENTO_SOLICITUD=SP.SOLICITUD_TRASLADO "+
                           "WHERE TS.STATUS='C' AND SP.ESTATUS!='CERRADA'")
            registros=cursor.fetchall()
            for registro in registros:
                pedidoPorCerrar=PedidoPorCerrar(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5])
                pedidopPorCerrarList.append(pedidoPorCerrar)
            return pedidopPorCerrarList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def cerrarPedidoPendientes(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute("UPDATE Solicitud SET SolicitudStatus = 4 WHERE SolicitudID IN (SELECT SP.PEDIDO "+
                           "FROM STATUS_PEDIDO SP INNER JOIN TRASLADO_SAP TS ON TS.DOCUMENTO_SOLICITUD=SP.SOLICITUD_TRASLADO "+
                           "WHERE TS.STATUS='C' AND SP.ESTATUS!='CERRADA')")

            cursor.execute("UPDATE SolicitudContenedor SET ContenedorStatus = 4 WHERE SolicitudID IN (SELECT SP.PEDIDO "+
                           "FROM STATUS_PEDIDO SP INNER JOIN TRASLADO_SAP TS ON TS.DOCUMENTO_SOLICITUD=SP.SOLICITUD_TRASLADO "+
                           "WHERE TS.STATUS='C' AND SP.ESTATUS!='CERRADA')")

            cursor.execute("UPDATE SolicitudRutas SET status = 4 WHERE SolicitudID IN (SELECT SP.PEDIDO "+
                           "FROM STATUS_PEDIDO SP INNER JOIN TRASLADO_SAP TS ON TS.DOCUMENTO_SOLICITUD=SP.SOLICITUD_TRASLADO "+
                           "WHERE TS.STATUS='C' AND SP.ESTATUS!='CERRADA')")
            conexion.commit()
            return True
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
    
    def getPedidosSinTr(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            pedidosSinTrList=[]
            numero=0
            cursor.execute("SELECT PEDIDO FROM STATUS_PEDIDO WHERE SOLICITUD_TRASLADO='' and ESTATUS!='CERRADA'")
            registros=cursor.fetchall()
            for registro in registros:
                logger.error(registro[0])
                pedidosSinTrList.append(registro[0])
            return pedidosSinTrList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getTiendasPendientesAll(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            tiendaPendienteList=[]
            numParamteros=0
            cursor.execute("SELECT TIPE.SOLICITUD_ESTATUS, TIPE.CARGA, TIPE.PEDIDO, TIPE.NOMBRE_ALMACEN, TIPE.FECHA_EMBARQUE, TIPE.FECHA_PLANEADA, TIPE.TRANSITO, TIPE.CROSS_DOCK, TIPE.FECHA_ENTREGA "+
                           "FROM(select TP.SOLICITUD_ESTATUS, TP.CARGA, TP.PEDIDO, TP.NOMBRE_ALMACEN, TP.FECHA_EMBARQUE, TP.FECHA_PLANEADA, TP.TRANSITO, TP.CROSS_DOCK, ISNULL((SELECT FECHA_REAL FROM FECHA_TRAFICO FT WHERE FT.CARGA=TP.CARGA AND FT.PEDIDO=TP.PEDIDO), TP.FECHA_PLANEADA) FECHA_ENTREGA from TIENDA_PENDIENTE TP) TIPE "+
                           "ORDER BY TIPE.FECHA_ENTREGA")
            registros=cursor.fetchall()
            for registro in registros:
                tiendaPendiente=TiendaPendiente(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8])
                tiendaPendienteList.append(tiendaPendiente)
            return tiendaPendienteList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
                
    def getTiendasPendientesCorreo(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            tiendaPendienteList=[]
            numParamteros=0
            cursor.execute("SELECT TIPE.SOLICITUD_ESTATUS, TIPE.CARGA, TIPE.PEDIDO, TIPE.NOMBRE_ALMACEN, TIPE.FECHA_EMBARQUE, TIPE.FECHA_PLANEADA, TIPE.TRANSITO, TIPE.CROSS_DOCK, TIPE.FECHA_ENTREGA "+
                           "FROM(select TP.SOLICITUD_ESTATUS, TP.CARGA, TP.PEDIDO, TP.NOMBRE_ALMACEN, TP.FECHA_EMBARQUE, TP.FECHA_PLANEADA, TP.TRANSITO, TP.CROSS_DOCK, ISNULL((SELECT FECHA_REAL FROM FECHA_TRAFICO FT WHERE FT.CARGA=TP.CARGA AND FT.PEDIDO=TP.PEDIDO), TP.FECHA_PLANEADA) FECHA_ENTREGA from TIENDA_PENDIENTE TP) TIPE "+
                           "where TIPE.SOLICITUD_ESTATUS IN ('EN TRANSITO', 'REC PARCIAL') "+
                           "AND FORMAT(TIPE.FECHA_ENTREGA, 'yyyy-MM-dd')<FORMAT(GETDATE(), 'yyyy-MM-dd') "+
                           "ORDER BY TIPE.SOLICITUD_ESTATUS DESC, TIPE.FECHA_ENTREGA")
            registros=cursor.fetchall()
            for registro in registros:
                tiendaPendiente=TiendaPendiente(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8])
                tiendaPendienteList.append(tiendaPendiente)
            return tiendaPendienteList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getRecibosTienda(self, fechaInicio, fechaFin):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            recibosTiendaList=[]
            numParamteros=0
            cursor.execute("select SolicitudNoTransporte 'CARGA', SolicitudID 'PEDIDO', TransportistaFHLlegada 'LLEGADA DEL TRANSPORTISTA:', TransportistaHoraIni 'INICIO DE ESCANEO', TransportistaHoraFin 'FIN DE ESCANEO', TransportistaFHFin  'CIERRE DE CAMION' "+
                           "from SolicitudTransportista "+
                           "where TransportistaOrigen = 'TIENDA' "+
                           "AND CONVERT(DATE, TransportistaFHLlegada ,103) between '"+fechaInicio+"' and '"+fechaFin+"' "+
                           "order by TransportistaFHLlegada DESC, SolicitudID, SolicitudNoTransporte")
            registros=cursor.fetchall()
            for registro in registros:
                reciboTienda=ReciboTienda(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5])
                recibosTiendaList.append(reciboTienda)
            return recibosTiendaList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getRecibosTienda(self, fechaInicio, fechaFin):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            recibosTiendaList=[]
            numParamteros=0
            cursor.execute("select SolicitudNoTransporte 'CARGA', SolicitudID 'PEDIDO', TransportistaFHLlegada 'LLEGADA DEL TRANSPORTISTA:', TransportistaHoraIni 'INICIO DE ESCANEO', TransportistaHoraFin 'FIN DE ESCANEO', TransportistaFHFin  'CIERRE DE CAMION' "+
                           "from SolicitudTransportista "+
                           "where TransportistaOrigen = 'TIENDA' "+
                           "AND CONVERT(DATE, TransportistaFHLlegada ,103) between '"+fechaInicio+"' and '"+fechaFin+"' "+
                           "order by TransportistaFHLlegada DESC, SolicitudID, SolicitudNoTransporte")
            registros=cursor.fetchall()
            for registro in registros:
                reciboTienda=ReciboTienda(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5])
                recibosTiendaList.append(reciboTienda)
            return recibosTiendaList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getRecibosTienda(self, fechaInicio, fechaFin):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            recibosTiendaList=[]
            numParamteros=0
            cursor.execute("select SolicitudNoTransporte 'CARGA', SolicitudID 'PEDIDO', TransportistaFHLlegada 'LLEGADA DEL TRANSPORTISTA:', TransportistaHoraIni 'INICIO DE ESCANEO', TransportistaHoraFin 'FIN DE ESCANEO', TransportistaFHFin  'CIERRE DE CAMION' "+
                           "from SolicitudTransportista "+
                           "where TransportistaOrigen = 'TIENDA' "+
                           "AND CONVERT(DATE, TransportistaFHLlegada ,103) between '"+fechaInicio+"' and '"+fechaFin+"' "+
                           "order by TransportistaFHLlegada DESC, SolicitudID, SolicitudNoTransporte")
            registros=cursor.fetchall()
            for registro in registros:
                reciboTienda=ReciboTienda(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5])
                recibosTiendaList.append(reciboTienda)
            return recibosTiendaList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getListadoTiendas(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            tiendasList=[]
            cursor.execute("select AlmacenCve, AlmacenNombre from Almacen where AlmacenCve like 'CL0%-DI' and AlmacenEstatus = 'N'")
            registros=cursor.fetchall()
            tiendasList.append(Tienda('Todas las tiendas','')) #List all shops
            for registro in registros:
                tienda=Tienda(registro[0], registro[1])
                tiendasList.append(tienda)
            return tiendasList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getAuditoriaTienda(self, tienda, fechaInicio, fechaFin):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            auditoriaTiendaList=[]
            if tienda.startswith('Todas'):
                cursor.execute(" WITH registros AS (SELECT soli.SolicitudNoTransporte, SUM(soli.SolicitudTotalContenedores) TotalContenedores,  " +
                                "   (SELECT COUNT(*) FROM AuditoriaContenedor audi WHERE audi.AuditoriaSolicitudId=soli.SolicitudId  " +
                                "   AND audi.AuditoriaSolicitudTransNo=soli.SolicitudNoTransporte AND audi.AuditoriaContenedorStatus!=1 ) ContenedoresAuditados, " +
                                "   MAX(soli.SolicitudWarehouseTo) SolicitudWarehouseTo, MAX(convert(nvarchar(MAX),soli.fechaRecepcion,23)) fechaRecepcion " +
                                "   FROM (SELECT sol.SolicitudID, sol.SolicitudNoTransporte, SUM(sol.SolicitudTotalContenedores) SolicitudTotalContenedores, sol.SolicitudWarehouseTo,  " +
                                "   (SELECT TOP 1 TransportistaFecha FROM SolicitudTransportista st WHERE st.SolicitudID=sol.SolicitudID AND st.SolicitudNoTransporte=sol.SolicitudNoTransporte AND st.TransportistaOrigen='TIENDA') fechaRecepcion " +
                                "   FROM Solicitud sol WHERE sol.SolicitudStatus=4 GROUP BY sol.SolicitudID, sol.SolicitudNoTransporte, sol.SolicitudWarehouseTo ) soli " +
                                "   WHERE FORMAT(soli.fechaRecepcion, 'yyyy-MM-dd') >= ? " +
                                "   AND FORMAT(soli.fechaRecepcion, 'yyyy-MM-dd') <= ? GROUP BY soli.SolicitudNoTransporte, soli.SolicitudId ) " +
                                " SELECT SolicitudWarehouseTo, fechaRecepcion, SUM(TotalContenedores) AS TotalContenedores, SUM(ContenedoresAuditados) AS ContenedoresAuditados " +
                                " FROM registros GROUP BY    SolicitudWarehouseTo,   fechaRecepcion ORDER BY fechaRecepcion DESC ",  (fechaInicio, fechaFin))
            else:
                cursor.execute(" WITH registros AS (SELECT soli.SolicitudNoTransporte, SUM(soli.SolicitudTotalContenedores) TotalContenedores,  " +
                                "   ( SELECT COUNT(*) FROM AuditoriaContenedor audi WHERE audi.AuditoriaSolicitudId=soli.SolicitudId  " +
                                "   AND audi.AuditoriaSolicitudTransNo=soli.SolicitudNoTransporte AND audi.AuditoriaContenedorStatus!=1 ) ContenedoresAuditados, " +
                                "   MAX(soli.SolicitudWarehouseTo) SolicitudWarehouseTo, MAX(convert(nvarchar(MAX),soli.fechaRecepcion,23)) fechaRecepcion " +
                                "   FROM ( SELECT sol.SolicitudID, sol.SolicitudNoTransporte, SUM(sol.SolicitudTotalContenedores) SolicitudTotalContenedores, sol.SolicitudWarehouseTo,  " +
                                "   (SELECT TOP 1 TransportistaFecha FROM SolicitudTransportista st WHERE st.SolicitudID=sol.SolicitudID AND st.SolicitudNoTransporte=sol.SolicitudNoTransporte AND st.TransportistaOrigen='TIENDA') fechaRecepcion " +
                                "   FROM Solicitud sol WHERE sol.SolicitudWarehouseTo= ?  " +
                                "   AND sol.SolicitudStatus=4 GROUP BY sol.SolicitudID, sol.SolicitudNoTransporte, sol.SolicitudWarehouseTo ) soli " +
                                "   WHERE FORMAT(soli.fechaRecepcion, 'yyyy-MM-dd') >= ? " +
                                "   AND FORMAT(soli.fechaRecepcion, 'yyyy-MM-dd') <= ? GROUP BY soli.SolicitudNoTransporte, soli.SolicitudId ) " +
                                " SELECT SolicitudWarehouseTo, fechaRecepcion, SUM(TotalContenedores) AS TotalContenedores, SUM(ContenedoresAuditados) AS ContenedoresAuditados " +
                                " FROM registros GROUP BY    SolicitudWarehouseTo,   fechaRecepcion ORDER BY fechaRecepcion DESC ", (tienda, fechaInicio, fechaFin))
            registros=cursor.fetchall()
            for registro in registros:
                if registro[2] !=0:
                    # print(registro[4]) 
                    porcentaje = str(round(registro[3]/registro[2]*100,2)) + " %"
                    auditoriaTienda=AuditoriaTiendaCl(registro[0], registro[1], registro[2], registro[3],  porcentaje)
                    auditoriaTiendaList.append(auditoriaTienda)
                else:
                    auditoriaTienda=AuditoriaTiendaCl(registro[0], registro[1], registro[2], registro[3], 'NA')
                    auditoriaTiendaList.append(auditoriaTienda)
            return auditoriaTiendaList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getConfirmPending(self):
            try:
                conexion=self.getConexion()
                cursor=conexion.cursor()
                confirmList=[]
                cursor.execute("select Carga,Pedido,NumContenedores,convert(nvarchar(MAX),Fecha,20) fecha from Recepcion where Confirmado=0")
                registros=cursor.fetchall()
                for registro in registros:
                    confirm=ConfirmationPending(registro[0], registro[1], registro[2], registro[3])
                    confirmList.append(confirm)
                return confirmList
            except Exception as exception:
                logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
                raise exception
            finally:
                if conexion!= None:
                    self.closeConexion(conexion)


    def getOrderAudi(self,tienda, fecha):
            try:
                conexion=self.getConexion()
                cursor=conexion.cursor()
                orderList=[]
                cursor.execute("select  soli.SolicitudID " +
                                "from (select sol.SolicitudID, sol.SolicitudNoTransporte,sol.SolicitudTotalContenedores, sol.SolicitudWarehouseTo, (select top 1 TransportistaFecha from SolicitudTransportista st where st.SolicitudID=sol.SolicitudID and st.SolicitudNoTransporte=sol.SolicitudNoTransporte and st.TransportistaOrigen='TIENDA') fechaRecepcion  " +
                                "from Solicitud sol where sol.SolicitudStatus=4) soli  " +
                                "where (select AlmacenCve from Almacen where AlmacenCve like '%DI' and AlmacenEstatus = 'N' and soli.SolicitudWarehouseTo = AlmacenCve) = soli.SolicitudWarehouseTo  " +
                                "and soli.SolicitudWarehouseTo = ? " +
                                # "and soli.SolicitudNoTransporte = ? " +
                                "and format(soli.fechaRecepcion, 'yyyy-MM-dd') = ?  " +
                                "order by soli.SolicitudNoTransporte desc ",tienda, fecha)
                registros=cursor.fetchall()
                for registro in registros:
                    orderAudi=OrderAudi(registro[0])
                    orderList.append(orderAudi)
                return orderList
            except Exception as exception:
                logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
                raise exception
            finally:
                if conexion!= None:
                    self.closeConexion(conexion)

                    
    def getSubFamilyOrders(self, tienda,fecha):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            subfamilyList=[]
            query = ("Select  s.SolicitudWarehouseTo , ship.user_def2,COUNT(ship.container_id) totalContenedores,COUNT(audi.AuditoriaContenedorID) contenedoresAuditados " +
                    " from Solicitud s join SolicitudContenedor soc on s.SolicitudID = soc.SolicitudID and s.SolicitudNoTransporte = soc.SolicitudNoTransporte " +
                    " left join  " +
                    " (select distinct sc.user_def2, sc.parent_container_id as container_id from [192.168.84.34].[ILS].[dbo].[SHIPPING_CONTAINER] sc where sc.parent_container_id is not null " +
                    " union all  " +
                    " select distinct ssc.user_def2, ssc.parent_container_id as container_id from [192.168.84.34].[ILS].[dbo].[AR_SHIPPING_CONTAINER] ssc where ssc.parent_container_id is not null) ship on ship.container_id COLLATE Modern_Spanish_CI_AS = soc.ContenedorId COLLATE Modern_Spanish_CI_AS " +
                    " left join AuditoriaContenedor audi  on audi.AuditoriaContenedorID=soc.ContenedorId AND audi.AuditoriaContenedorStatus!=1 " +
                    " where s.SolicitudWarehouseTo = ? " +
                    " and (SELECT TOP 1 format(st.TransportistaFecha,'yyyy-MM-dd') FROM SolicitudTransportista st  " +
                    " WHERE st.SolicitudID = s.SolicitudID  AND st.SolicitudNoTransporte = s.SolicitudNoTransporte AND st.TransportistaOrigen = 'TIENDA') = ? " +
                    " group by s.SolicitudWarehouseTo,ship.user_def2  ")
            
            cursor.execute(query,tienda,fecha )
            print(query)
            registros=cursor.fetchall()
            for registro in registros:
                if registro[2] !=0:
                    porcentaje = str(round(registro[3]/registro[2]*100,2)) + " %"
                    subfamilyOrder=SubFamilyOrder(registro[0], registro[1], registro[2], registro[3],porcentaje)
                    subfamilyList.append(subfamilyOrder)
                else:
                    subfamilyOrder=SubFamilyOrder(registro[0], registro[1], registro[2], registro[3], 'NA')
                    subfamilyList.append(subfamilyOrder)
            return subfamilyList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)