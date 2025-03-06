import logging
import pyodbc
from sevicios_app.vo.inventarioWms import InventarioWms
from sevicios_app.vo.reciboSap import ReciboSap
from sevicios_app.vo.pedidoSap import PedidoSap
from sevicios_app.vo.cuadraje import Cuadraje
from sevicios_app.vo.pendienteSemana import PendienteSemana
from sevicios_app.vo.inventarioTienda import InventarioTienda
from sevicios_app.vo.reciboDetalle import ReciboDetalle

logger = logging.getLogger('')

class MonitoreoDao():

    def getConexion(self):
        try:
            direccion_servidor = '192.168.84.107'
            nombre_bd = 'MINISO_MONITOREO'
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
        
    def getInventarioWms(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            inventarioWmsList=[]
            cursor.execute("SELECT "+
                           "ISNULL(NA.NOMBRE_ALMACEN, SI.WhsCode), "+
                           "Solicitado, OnHand, Comprometido, Disponible, SKU_SOL, SKU_OHD, SKU_CMP, CONVERT(date,DATEADD(DD,1,[Fecha Actualizacion]),103) AS 'Fecha Actualizacion' "+
                           "FROM SAP_INVCEDIS SI (nolock) "+
                           "LEFT JOIN NOMBRE_ALMACEN NA ON NA.CLAVE_ALMACEN=SI.WhsCode "+
                           "ORDER BY OnHand DESC")
            registros=cursor.fetchall()
            for registro in registros:
                inventarioWms=InventarioWms(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8])
#                print(f"{registro[0]} {registro[1]} {registro[2]} {registro[3]} {registro[4]} {registro[5]} {registro[6]} {registro[7]} {registro[8]}")
                inventarioWmsList.append(inventarioWms)
            return inventarioWmsList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getReciboSap(self, recibos):
        try:
            logger.error("Entro al proceso getReciboSap")
            conexion=self.getConexion()
            cursor=conexion.cursor()
            recibosList=[]
            consulta="SELECT DocSDT, CLOSE_QTY_SDT, CLOSE_QTY_TR, OPEN_QTY_SDT, OPEN_QTY_TR, TOTAL_QTY_SDT, TOTAL_QTY_TR, WMS, DIF, CLOSE_DATE, STS_WMS, VAL FROM SAP_RECEIPT WHERE DocSDT in ("+recibos+")  ORDER BY DocSDT"
            logger.error(consulta)
            cursor.execute(consulta)
            registros=cursor.fetchall()
            for registro in registros:
                reciboSap=ReciboSap(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11])
                recibosList.append(reciboSap)
            return recibosList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getReciboSapByValor(self, valor):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            recibosList=[]
            consulta="SELECT DocSDT, CLOSE_QTY_SDT, CLOSE_QTY_TR, OPEN_QTY_SDT, OPEN_QTY_TR, TOTAL_QTY_SDT, TOTAL_QTY_TR, WMS, DIF, CLOSE_DATE, STS_WMS, VAL FROM SAP_RECEIPT WHERE VAL = '"+valor+"' ORDER BY DocSDT"
            cursor.execute(consulta)
            registros=cursor.fetchall()
            for registro in registros:
                reciboSap=ReciboSap(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11])
                recibosList.append(reciboSap)
            return recibosList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getPedidoSap(self, pedidos):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            pedidosList=[]
            consulta="SELECT DocSDT, CLOSE_QTY_SDT, CLOSE_QTY_TR, OPEN_QTY_SDT, OPEN_QTY_TR, TOTAL_QTY_SDT, TOTAL_QTY_TR, WMS_CLOSE, DIF, SHIP_DATE, STS_WMS, VAL, OPEN_SAP FROM SAP_SHIPMENT WHERE DocSDT in ("+pedidos+")  ORDER BY DocSDT"
            cursor.execute(consulta)
            registros=cursor.fetchall()
            for registro in registros:
                pedidoSap=PedidoSap(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11], registro[12])
                pedidosList.append(pedidoSap)
            return pedidosList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getPedidoSapByValor(self, valor, limite):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            pedidosList=[]
            top=""
            if limite:
                top=" top 100"
            consulta="SELECT"+top+" DocSDT, CLOSE_QTY_SDT, CLOSE_QTY_TR, OPEN_QTY_SDT, OPEN_QTY_TR, TOTAL_QTY_SDT, TOTAL_QTY_TR, WMS_CLOSE, DIF, SHIP_DATE, STS_WMS, VAL, OPEN_SAP FROM SAP_SHIPMENT WHERE VAL='"+valor+"' ORDER BY DocSDT"
            cursor.execute(consulta)
            registros=cursor.fetchall()
            for registro in registros:
                pedidoSap=PedidoSap(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11], registro[12])
                pedidosList.append(pedidoSap)
            return pedidosList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getPedidoSapAbiertos(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            pedidosList=[]
            consulta="SELECT DocSDT, CLOSE_QTY_SDT, CLOSE_QTY_TR, OPEN_QTY_SDT, OPEN_QTY_TR, TOTAL_QTY_SDT, TOTAL_QTY_TR, WMS_CLOSE, DIF, SHIP_DATE, STS_WMS, VAL, OPEN_SAP FROM SAP_SHIPMENT WHERE OPEN_SAP='O' ORDER BY DocSDT"
            cursor.execute(consulta)
            registros=cursor.fetchall()
            for registro in registros:
                pedidoSap=PedidoSap(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11], registro[12])
                pedidosList.append(pedidoSap)
            return pedidosList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
                
    def executeCuadraje(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute("EXEC CARGA_CUADRAJE_WMS_OPEN_SHIPMENT")
            conexion.commit()
            cursor.execute("EXEC CARGA_CUADRAJE_WMS_RECEIPT")
            conexion.commit()
            cursor.execute("EXEC CARGA_CUADRAJE_WMS_SHIPMENT")
            conexion.commit()
            cursor.execute("EXEC CARGA_CUADRAJE_SAP_RECEIPT")
            conexion.commit()
            cursor.execute("EXEC CARGA_CUADRAJE_SAP_SHIPMENT")
            conexion.commit()

            return True
        except Exception as exception:
            logger.error(f"Se presento una incidencia al ejecutar el proceso de envio de correos a tiendas tiendas: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getDatosCuadraje(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cuadrajesList=[]

            recibosTotal=0
            recibosOk='0'
            recibosQty='0'
            recibosCloseErp='0'
            recibosRev='0'
            recibosTotalNum=0
            recibosOkNum='0'
            recibosQtyNum='0'
            recibosCloseErpNum='0'
            recibosRevNum='0'
            pedidosTotal=0
            pedidosOk='0'
            pedidosQty='0'
            pedidosCloseErp='0'
            pedidosRev='0'
            pedidosTotalNum=0
            pedidosOkNum='0'
            pedidosQtyNum='0'
            pedidosCloseErpNum='0'
            pedidosRevNum='0'
            pedidosAbiertos='0'
            pedidosAbiertosNum='0'

            cursor.execute("SELECT VAL, count(*) NUM_REGISTROS, sum(TOTAL_QTY_SDT)-sum(TOTAL_QTY_TR) DIF FROM SAP_RECEIPT GROUP BY VAL")
            registros=cursor.fetchall()
            for registro in registros:
                if registro[0]=='QTY':
                    recibosQty=registro[1]
                    recibosQtyNum=registro[2]
                    recibosTotal=recibosTotal+registro[1]
                    recibosTotalNum=recibosTotalNum+registro[2]
                if registro[0]=='CLOSE_ERP':
                    recibosCloseErp=registro[1]
                    recibosCloseErpNum=registro[2]
                    recibosTotal=recibosTotal+registro[1]
                    recibosTotalNum=recibosTotalNum+registro[2]
                if registro[0]=='OK':
                    recibosOk=registro[1]
                    recibosOkNum=registro[2]
                    recibosTotal=recibosTotal+registro[1]
                    recibosTotalNum=recibosTotalNum+registro[2]
                if registro[0]=='REV':
                    recibosRev=registro[1]
                    recibosRevNum=registro[2]
                    recibosTotal=recibosTotal+registro[1]
                    recibosTotalNum=recibosTotalNum+registro[2]
            
            cursor.execute("SELECT VAL, count(*)NUM_REGISTROS, SUM(DIF) TOTAL_DIF FROM SAP_SHIPMENT GROUP BY VAL")
            registros=cursor.fetchall()
            for registro in registros:
                if registro[0]=='QTY':
                    pedidosQty=registro[1]
                    pedidosQtyNum=registro[2]
                    pedidosTotal=pedidosTotal+registro[1]
                    pedidosTotalNum=pedidosTotalNum+registro[2]
                if registro[0]=='CLOSE_ERP':
                    pedidosCloseErp=registro[1]
                    pedidosCloseErpNum=registro[2]
                    pedidosTotal=pedidosTotal+registro[1]
                    pedidosTotalNum=pedidosTotalNum+registro[2]
                if registro[0]=='OK':
                    pedidosOk=registro[1]
                    pedidosOkNum=registro[2]
                    pedidosTotal=pedidosTotal+registro[1]
                    pedidosTotalNum=pedidosTotalNum+registro[2]
                if registro[0]=='REV':
                    pedidosRev=registro[1]
                    pedidosRevNum=registro[2]
                    pedidosTotal=pedidosTotal+registro[1]
                    pedidosTotalNum=pedidosTotalNum+registro[2]

            cursor.execute("SELECT count(*), ISNULL(SUM(TOTAL_QTY_SDT), 0) FROM SAP_SHIPMENT WHERE OPEN_SAP='O'")
            registros=cursor.fetchall()
            for registro in registros:
                pedidosAbiertos=registro[0]
                pedidosAbiertosNum=registro[1]
            cuadraje=Cuadraje(recibosTotal, recibosOk, recibosQty, recibosCloseErp, recibosRev, recibosTotalNum, recibosOkNum, recibosQtyNum, recibosCloseErpNum, recibosRevNum, pedidosTotal, pedidosOk, pedidosQty, pedidosCloseErp, pedidosRev, pedidosTotalNum, pedidosOkNum, pedidosQtyNum, pedidosCloseErpNum, pedidosRevNum, pedidosAbiertos, pedidosAbiertosNum)
            cuadrajesList.append(cuadraje)
            return cuadrajesList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getPendienteSemana(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            pendientesSemanaList=[]
            cursor.execute("select format(SH.MES_ANIO, 'MMM-yy') FECHA, SH.SHIP_DATE, COUNT(SH.SHIP_DATE) NUM_REGISTROS, "+
                           "(select sum(SSH.DIF) from SAP_SHIPMENT SSH WHERE SSH.SHIP_DATE=SH.SHIP_DATE AND SSH.VAL='REV') PIEZAS, "+
                           "(select ISNULL(sum(SSH.DIF),0) from SAP_SHIPMENT SSH WHERE SSH.SHIP_DATE=SH.SHIP_DATE AND STS_WMS='700') REETIQUETADO "+
                           "from SAP_SHIPMENT SH WHERE SH.SHIP_DATE!='' "+
                           "GROUP BY MES_ANIO, SH.SHIP_DATE ORDER BY MES_ANIO")
            registros=cursor.fetchall()
            for registro in registros:
                pendienteSemana=PendienteSemana(registro[0], registro[1], registro[2], registro[3], registro[4])
                pendientesSemanaList.append(pendienteSemana)
            return pendientesSemanaList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def insertSapReceiptVal(self, idRecibo):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute("INSERT INTO SAP_RECEIPT_VAL (DocSDT, FECHA, VAL) VALUES("+idRecibo+", GETDATE(), 'OK')")
            conexion.commit()
            return True
        except Exception as exception:
            logger.error(f"Se presento una incidencia al ejecutar el proceso de envio de correos a tiendas tiendas: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def insertSapShipmentValCl(self, idShipment): 
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute("INSERT INTO SAP_SHIPMENT_VAL_CL (DocSDT, FECHA, VAL) VALUES("+idShipment+", GETDATE(), 'OK')")
            conexion.commit()
            return True
        except Exception as exception:
            logger.error(f"Se presento una incidencia al ejecutar el proceso de envio de correos a tiendas tiendas: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def executeInvetarioCedis(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute("EXEC CargaSAP_INVCEDIS")
            conexion.commit()
            return True
        except Exception as exception:
            logger.error(f"Se presento una incidencia al ejecutar el proceso de envio de correos a tiendas tiendas: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
                
    def executeUpdateDiferencias(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute("EXEC REPROCESO_OBTENER_DOCUMENTOS")
            conexion.commit()
            cursor.execute("EXEC REPROCESO_CUADRAJE_WMS_OPEN_SHIPMENT")
            conexion.commit()
            cursor.execute("EXEC REPROCESO_CUADRAJE_WMS_RECEIPT")
            conexion.commit()
            cursor.execute("EXEC REPROCESO_CUADRAJE_WMS_SHIPMENT")
            conexion.commit()
            cursor.execute("EXEC REPROCESO_CUADRAJE_SAP_RECEIPT")
            conexion.commit()
            cursor.execute("EXEC REPROCESO_CUADRAJE_SAP_SHIPMENT")
            conexion.commit()

            return True
        except Exception as exception:
            logger.error(f"Se presento una incidencia al ejecutar el proceso de envio de correos a tiendas tiendas: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getInventarioClWms(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            inventarioWmsList=[]
            cursor.execute("SELECT "+
                           "ISNULL(NA.NOMBRE_ALMACEN, INV.WhsCode), "+
                           "Solicitado, OnHand, Comprometido, Disponible, SKU_SOL, SKU_OHD, SKU_CMP, CONVERT(date,DATEADD(DD,1,[Fecha Actualizacion]),103) AS 'Fecha Actualizacion' "+
                           "FROM SAP_INVCEDIS_MCL INV (nolock) "+
                           "LEFT JOIN NOMBRE_ALMACEN_CL NA ON NA.CLAVE_ALMACEN=INV.WhsCode "+
                           "ORDER BY OnHand DESC")
            registros=cursor.fetchall()
            for registro in registros:
                inventarioWms=InventarioWms(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8])
#                print(f"{registro[0]} {registro[1]} {registro[2]} {registro[3]} {registro[4]} {registro[5]} {registro[6]} {registro[7]} {registro[8]}")
                inventarioWmsList.append(inventarioWms)
            return inventarioWmsList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
                
    def executeInvetarioClCedis(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute("EXEC CargaSAP_INVCEDIS_MCL")
            conexion.commit()
            return True
        except Exception as exception:
            logger.error(f"Se presento una incidencia al ejecutar el proceso de envio de correos a tiendas tiendas: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getReciboSapCl(self, recibos):
        try:
            logger.error("Entro al proceso getReciboSap")
            conexion=self.getConexion()
            cursor=conexion.cursor()
            recibosList=[]
            consulta="SELECT DocSDT, CLOSE_QTY_SDT, CLOSE_QTY_TR, OPEN_QTY_SDT, OPEN_QTY_TR, TOTAL_QTY_SDT, TOTAL_QTY_TR, WMS, DIF, CLOSE_DATE, STS_WMS, VAL FROM SAP_RECEIPT_CL WHERE DocSDT in ("+recibos+")  ORDER BY DocSDT"
            logger.error(consulta)
            cursor.execute(consulta)
            registros=cursor.fetchall()
            for registro in registros:
                reciboSap=ReciboSap(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11])
                recibosList.append(reciboSap)
            return recibosList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
                
    def getReciboSapByValorCl(self, valor):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            recibosList=[]
            consulta="SELECT DocSDT, CLOSE_QTY_SDT, CLOSE_QTY_TR, OPEN_QTY_SDT, OPEN_QTY_TR, TOTAL_QTY_SDT, TOTAL_QTY_TR, WMS, DIF, CLOSE_DATE, STS_WMS, VAL FROM SAP_RECEIPT_CL WHERE VAL = '"+valor+"' ORDER BY DocSDT"
            cursor.execute(consulta)
            registros=cursor.fetchall()
            for registro in registros:
                reciboSap=ReciboSap(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11])
                recibosList.append(reciboSap)
            return recibosList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getPedidoSapCl(self, pedidos):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            pedidosList=[]
            consulta="SELECT DocSDT, CLOSE_QTY_SDT, CLOSE_QTY_TR, OPEN_QTY_SDT, OPEN_QTY_TR, TOTAL_QTY_SDT, TOTAL_QTY_TR, WMS_CLOSE, DIF, SHIP_DATE, STS_WMS, VAL, OPEN_SAP FROM SAP_SHIPMENT_CL WHERE DocSDT in ("+pedidos+")  ORDER BY DocSDT"
            cursor.execute(consulta)
            registros=cursor.fetchall()
            for registro in registros:
                pedidoSap=PedidoSap(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11], registro[12])
                pedidosList.append(pedidoSap)
            return pedidosList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
                
    def getPedidoSapAbiertosCl(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            pedidosList=[]
            consulta="SELECT DocSDT, CLOSE_QTY_SDT, CLOSE_QTY_TR, OPEN_QTY_SDT, OPEN_QTY_TR, TOTAL_QTY_SDT, TOTAL_QTY_TR, WMS_CLOSE, DIF, SHIP_DATE, STS_WMS, VAL, OPEN_SAP FROM SAP_SHIPMENT_CL WHERE OPEN_SAP='O' ORDER BY DocSDT"
            cursor.execute(consulta)
            registros=cursor.fetchall()
            for registro in registros:
                pedidoSap=PedidoSap(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11], registro[12])
                pedidosList.append(pedidoSap)
            return pedidosList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
                
    def getPedidoSapByValorCl(self, valor):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            pedidosList=[]
            consulta="SELECT DocSDT, CLOSE_QTY_SDT, CLOSE_QTY_TR, OPEN_QTY_SDT, OPEN_QTY_TR, TOTAL_QTY_SDT, TOTAL_QTY_TR, WMS_CLOSE, DIF, SHIP_DATE, STS_WMS, VAL, OPEN_SAP FROM SAP_SHIPMENT_CL WHERE VAL='"+valor+"' ORDER BY DocSDT"
            cursor.execute(consulta)
            registros=cursor.fetchall()
            for registro in registros:
                pedidoSap=PedidoSap(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11], registro[12])
                pedidosList.append(pedidoSap)
            return pedidosList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def executeCuadrajeCl(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute("EXEC CARGA_CUADRAJE_WMS_OPEN_SHIPMENT_CL")
            conexion.commit()
            cursor.execute("EXEC CARGA_CUADRAJE_WMS_RECEIPT_CL")
            conexion.commit()
            cursor.execute("EXEC CARGA_CUADRAJE_WMS_SHIPMENT_CL")
            conexion.commit()
            cursor.execute("EXEC CARGA_CUADRAJE_SAP_RECEIPT_CL")
            conexion.commit()
            cursor.execute("EXEC CARGA_CUADRAJE_SAP_SHIPMENT_CL")
            conexion.commit()

            return True
        except Exception as exception:
            logger.error(f"Se presento una incidencia al ejecutar el proceso de envio de correos a tiendas tiendas: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getDatosCuadrajeCl(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cuadrajesList=[]

            recibosTotal=0
            recibosOk='0'
            recibosQty='0'
            recibosCloseErp='0'
            recibosRev='0'
            recibosTotalNum=0
            recibosOkNum='0'
            recibosQtyNum='0'
            recibosCloseErpNum='0'
            recibosRevNum='0'
            pedidosTotal=0
            pedidosOk='0'
            pedidosQty='0'
            pedidosCloseErp='0'
            pedidosRev='0'
            pedidosTotalNum=0
            pedidosOkNum='0'
            pedidosQtyNum='0'
            pedidosCloseErpNum='0'
            pedidosRevNum='0'
            pedidosAbiertos='0'
            pedidosAbiertosNum='0'

            cursor.execute("SELECT VAL, count(*) NUM_REGISTROS, sum(TOTAL_QTY_SDT)-sum(TOTAL_QTY_TR) DIF FROM SAP_RECEIPT_CL GROUP BY VAL")
            registros=cursor.fetchall()
            for registro in registros:
                if registro[0]=='QTY':
                    recibosQty=registro[1]
                    recibosQtyNum=registro[2]
                    recibosTotal=recibosTotal+registro[1]
                    recibosTotalNum=recibosTotalNum+registro[2]
                if registro[0]=='CLOSE_ERP':
                    recibosCloseErp=registro[1]
                    recibosCloseErpNum=registro[2]
                    recibosTotal=recibosTotal+registro[1]
                    recibosTotalNum=recibosTotalNum+registro[2]
                if registro[0]=='OK':
                    recibosOk=registro[1]
                    recibosOkNum=registro[2]
                    recibosTotal=recibosTotal+registro[1]
                    recibosTotalNum=recibosTotalNum+registro[2]
                if registro[0]=='REV':
                    recibosRev=registro[1]
                    recibosRevNum=registro[2]
                    recibosTotal=recibosTotal+registro[1]
                    recibosTotalNum=recibosTotalNum+registro[2]
            
            cursor.execute("SELECT VAL, count(*)NUM_REGISTROS, SUM(DIF) TOTAL_DIF FROM SAP_SHIPMENT_CL GROUP BY VAL")
            registros=cursor.fetchall()
            for registro in registros:
                if registro[0]=='QTY':
                    pedidosQty=registro[1]
                    pedidosQtyNum=registro[2]
                    pedidosTotal=pedidosTotal+registro[1]
                    pedidosTotalNum=pedidosTotalNum+registro[2]
                if registro[0]=='CLOSE_ERP':
                    pedidosCloseErp=registro[1]
                    pedidosCloseErpNum=registro[2]
                    pedidosTotal=pedidosTotal+registro[1]
                    pedidosTotalNum=pedidosTotalNum+registro[2]
                if registro[0]=='OK':
                    pedidosOk=registro[1]
                    pedidosOkNum=registro[2]
                    pedidosTotal=pedidosTotal+registro[1]
                    pedidosTotalNum=pedidosTotalNum+registro[2]
                if registro[0]=='REV':
                    pedidosRev=registro[1]
                    pedidosRevNum=registro[2]
                    pedidosTotal=pedidosTotal+registro[1]
                    pedidosTotalNum=pedidosTotalNum+registro[2]

            cursor.execute("SELECT count(*), ISNULL(SUM(TOTAL_QTY_SDT), 0) FROM SAP_SHIPMENT_CL WHERE OPEN_SAP='O'")
            registros=cursor.fetchall()
            for registro in registros:
                pedidosAbiertos=registro[0]
                pedidosAbiertosNum=registro[1]
            cuadraje=Cuadraje(recibosTotal, recibosOk, recibosQty, recibosCloseErp, recibosRev, recibosTotalNum, recibosOkNum, recibosQtyNum, recibosCloseErpNum, recibosRevNum, pedidosTotal, pedidosOk, pedidosQty, pedidosCloseErp, pedidosRev, pedidosTotalNum, pedidosOkNum, pedidosQtyNum, pedidosCloseErpNum, pedidosRevNum, pedidosAbiertos, pedidosAbiertosNum)
            cuadrajesList.append(cuadraje)
            return cuadrajesList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getPendienteSemanaCl(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            pendientesSemanaList=[]
            cursor.execute("select format(SH.MES_ANIO, 'MMM-yy') FECHA, SH.SHIP_DATE, COUNT(SH.SHIP_DATE) NUM_REGISTROS, "+
                           "(select sum(SSH.DIF) from SAP_SHIPMENT_CL SSH WHERE SSH.SHIP_DATE=SH.SHIP_DATE AND SSH.VAL='REV') PIEZAS, "+
                           "(select ISNULL(sum(SSH.DIF),0) from SAP_SHIPMENT_CL SSH WHERE SSH.SHIP_DATE=SH.SHIP_DATE AND STS_WMS='700') REETIQUETADO "+
                           "from SAP_SHIPMENT_CL SH WHERE SH.SHIP_DATE!='' "+
                           "GROUP BY MES_ANIO, SH.SHIP_DATE ORDER BY MES_ANIO")
            registros=cursor.fetchall()
            for registro in registros:
                pendienteSemana=PendienteSemana(registro[0], registro[1], registro[2], registro[3], registro[4])
                pendientesSemanaList.append(pendienteSemana)
            return pendientesSemanaList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def insertSapReceiptValCl(self, idRecibo):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute("INSERT INTO SAP_RECEIPT_VAL_CL (DocSDT, FECHA, VAL) VALUES("+idRecibo+", GETDATE(), 'OK')")
            conexion.commit()
            return True
        except Exception as exception:
            logger.error(f"Se presento una incidencia al ejecutar el proceso de envio de correos a tiendas tiendas: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def executeUpdateDiferenciasCl(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute("EXEC REPROCESO_OBTENER_DOCUMENTOS_CL")
            conexion.commit()
            cursor.execute("EXEC REPROCESO_CUADRAJE_WMS_OPEN_SHIPMENT_CL")
            conexion.commit()
            cursor.execute("EXEC REPROCESO_CUADRAJE_WMS_RECEIPT_CL")
            conexion.commit()
            cursor.execute("EXEC REPROCESO_CUADRAJE_WMS_SHIPMENT_CL")
            conexion.commit()
            cursor.execute("EXEC REPROCESO_CUADRAJE_SAP_RECEIPT_CL")
            conexion.commit()
            cursor.execute("EXEC REPROCESO_CUADRAJE_SAP_SHIPMENT_CL")
            conexion.commit()

            return True
        except Exception as exception:
            logger.error(f"Se presento una incidencia al ejecutar el proceso de envio de correos a tiendas tiendas: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getInventarioTienda(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            inventarioTiendaList=[]
            consulta="SELECT ALMACEN, ITEM, DESCRIPCION, STOCK, COMPROMETIDO, SOLICITADO, ULTIMO_PRECIO FROM SAP_INVENTARIO_TIENDA ORDER BY ALMACEN, ITEM"
            cursor.execute(consulta)
            registros=cursor.fetchall()
            for registro in registros:
                inventarioTienda=InventarioTienda(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6])
                inventarioTiendaList.append(inventarioTienda)
            return inventarioTiendaList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getCuadrajeReciboDetalle(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            reciboDetalleList=[]
            cursor.execute("SELECT ISNULL(WRD.RECIBO, SRD.RECIBO) RECIBO, ISNULL(WRD.ITEM, SRD.ITEM) ITEM, ISNULL(WRD.CANTIDAD, 0) CANTIDAD_WMS, ISNULL(SRD.CANTIDAD, 0) CANTIDAD_SAP "+
                           "FROM WMS_RECEIPT_DETAIL WRD "+
                           "FULL OUTER JOIN SAP_RECEIPT_DETAIL SRD ON SRD.RECIBO = WRD.RECIBO AND SRD.ITEM = WRD.ITEM "+
                           "ORDER BY RECIBO, ITEM")
            registros=cursor.fetchall()
            for registro in registros:
                reciboDetalle=ReciboDetalle(registro[0], registro[1], registro[2], registro[3])
                reciboDetalleList.append(reciboDetalle)
            return reciboDetalleList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getCuadrajeReciboDetalleCl(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            reciboDetalleList=[]
            cursor.execute("SELECT ISNULL(WRD.RECIBO, SRD.RECIBO) RECIBO, ISNULL(WRD.ITEM, SRD.ITEM) ITEM, ISNULL(WRD.CANTIDAD, 0) CANTIDAD_WMS, ISNULL(SRD.CANTIDAD, 0) CANTIDAD_SAP "+
                           "FROM WMS_RECEIPT_DETAIL_CL WRD "+
                           "FULL OUTER JOIN SAP_RECEIPT_DETAIL_CL SRD ON SRD.RECIBO = WRD.RECIBO AND SRD.ITEM = WRD.ITEM "+
                           "ORDER BY RECIBO, ITEM")
            registros=cursor.fetchall()
            for registro in registros:
                reciboDetalle=ReciboDetalle(registro[0], registro[1], registro[2], registro[3])
                reciboDetalleList.append(reciboDetalle)
            return reciboDetalleList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getInventarioWmsCol(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            inventarioWmsList=[]
            cursor.execute("SELECT "+
                           "ISNULL(NA.NOMBRE_ALMACEN, SI.WhsCode), "+
                           "Solicitado, OnHand, Comprometido, Disponible, SKU_SOL, SKU_OHD, SKU_CMP, CONVERT(date,DATEADD(DD,1,[Fecha Actualizacion]),103) AS 'Fecha Actualizacion' "+
                           "FROM SAP_INVCEDIS_COL SI (nolock) "+
                           "LEFT JOIN NOMBRE_ALMACEN_COL NA ON NA.CLAVE_ALMACEN=SI.WhsCode "+
                           "ORDER BY OnHand DESC")
            registros=cursor.fetchall()
            for registro in registros:
                inventarioWms=InventarioWms(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8])
                inventarioWmsList.append(inventarioWms)
            return inventarioWmsList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def executeInvetarioCedisCol(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute("EXEC CargaSAP_INVCEDIS_COL")
            conexion.commit()
            return True
        except Exception as exception:
            logger.error(f"Se presento una incidencia al ejecutar el proceso de envio de correos a tiendas tiendas: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    
    def getDatosCuadrajeCol(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cuadrajesList=[]

            recibosTotal=0
            recibosOk='0'
            recibosQty='0'
            recibosCloseErp='0'
            recibosRev='0'
            recibosTotalNum=0
            recibosOkNum='0'
            recibosQtyNum='0'
            recibosCloseErpNum='0'
            recibosRevNum='0'
            pedidosTotal=0
            pedidosOk='0'
            pedidosQty='0'
            pedidosCloseErp='0'
            pedidosRev='0'
            pedidosTotalNum=0
            pedidosOkNum='0'
            pedidosQtyNum='0'
            pedidosCloseErpNum='0'
            pedidosRevNum='0'
            pedidosAbiertos='0'
            pedidosAbiertosNum='0'

            cursor.execute("SELECT VAL, count(*) NUM_REGISTROS, sum(TOTAL_QTY_SDT)-sum(TOTAL_QTY_TR) DIF FROM SAP_RECEIPT_COL GROUP BY VAL")
            registros=cursor.fetchall()
            for registro in registros:
                if registro[0]=='QTY':
                    recibosQty=registro[1]
                    recibosQtyNum=registro[2]
                    recibosTotal=recibosTotal+registro[1]
                    recibosTotalNum=recibosTotalNum+registro[2]
                if registro[0]=='CLOSE_ERP':
                    recibosCloseErp=registro[1]
                    recibosCloseErpNum=registro[2]
                    recibosTotal=recibosTotal+registro[1]
                    recibosTotalNum=recibosTotalNum+registro[2]
                if registro[0]=='OK':
                    recibosOk=registro[1]
                    recibosOkNum=registro[2]
                    recibosTotal=recibosTotal+registro[1]
                    recibosTotalNum=recibosTotalNum+registro[2]
                if registro[0]=='REV':
                    recibosRev=registro[1]
                    recibosRevNum=registro[2]
                    recibosTotal=recibosTotal+registro[1]
                    recibosTotalNum=recibosTotalNum+registro[2]
            
            cursor.execute("SELECT VAL, count(*)NUM_REGISTROS, SUM(DIF) TOTAL_DIF FROM SAP_SHIPMENT_COL GROUP BY VAL")
            registros=cursor.fetchall()
            for registro in registros:
                if registro[0]=='QTY':
                    pedidosQty=registro[1]
                    pedidosQtyNum=registro[2]
                    pedidosTotal=pedidosTotal+registro[1]
                    pedidosTotalNum=pedidosTotalNum+registro[2]
                if registro[0]=='CLOSE_ERP':
                    pedidosCloseErp=registro[1]
                    pedidosCloseErpNum=registro[2]
                    pedidosTotal=pedidosTotal+registro[1]
                    pedidosTotalNum=pedidosTotalNum+registro[2]
                if registro[0]=='OK':
                    pedidosOk=registro[1]
                    pedidosOkNum=registro[2]
                    pedidosTotal=pedidosTotal+registro[1]
                    pedidosTotalNum=pedidosTotalNum+registro[2]
                if registro[0]=='REV':
                    pedidosRev=registro[1]
                    pedidosRevNum=registro[2]
                    pedidosTotal=pedidosTotal+registro[1]
                    pedidosTotalNum=pedidosTotalNum+registro[2]

            cursor.execute("SELECT count(*), ISNULL(SUM(TOTAL_QTY_SDT), 0) FROM SAP_SHIPMENT_COL WHERE OPEN_SAP='O'")
            registros=cursor.fetchall()
            for registro in registros:
                pedidosAbiertos=registro[0]
                pedidosAbiertosNum=registro[1]
            cuadraje=Cuadraje(recibosTotal, recibosOk, recibosQty, recibosCloseErp, recibosRev, recibosTotalNum, recibosOkNum, recibosQtyNum, recibosCloseErpNum, recibosRevNum, pedidosTotal, pedidosOk, pedidosQty, pedidosCloseErp, pedidosRev, pedidosTotalNum, pedidosOkNum, pedidosQtyNum, pedidosCloseErpNum, pedidosRevNum, pedidosAbiertos, pedidosAbiertosNum)
            cuadrajesList.append(cuadraje)
            return cuadrajesList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)


    def getPendienteSemanaCol(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            pendientesSemanaList=[]
            cursor.execute("select format(SH.MES_ANIO, 'MMM-yy') FECHA, SH.SHIP_DATE, COUNT(SH.SHIP_DATE) NUM_REGISTROS, "+
                           "(select sum(SSH.DIF) from SAP_SHIPMENT_COL SSH WHERE SSH.SHIP_DATE=SH.SHIP_DATE AND SSH.VAL='REV') PIEZAS, "+
                           "(select ISNULL(sum(SSH.DIF),0) from SAP_SHIPMENT_COL SSH WHERE SSH.SHIP_DATE=SH.SHIP_DATE AND STS_WMS='700') REETIQUETADO "+
                           "from SAP_SHIPMENT_COL SH WHERE SH.SHIP_DATE!='' "+
                           "GROUP BY MES_ANIO, SH.SHIP_DATE ORDER BY MES_ANIO")
            registros=cursor.fetchall()
            for registro in registros:
                pendienteSemana=PendienteSemana(registro[0], registro[1], registro[2], registro[3], registro[4])
                pendientesSemanaList.append(pendienteSemana)
            return pendientesSemanaList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)


    def executeCuadrajeCol(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute("EXEC CARGA_CUADRAJE_WMS_OPEN_SHIPMENT_COL")
            conexion.commit()
            cursor.execute("EXEC CARGA_CUADRAJE_WMS_RECEIPT_COL")
            conexion.commit()
            cursor.execute("EXEC CARGA_CUADRAJE_WMS_SHIPMENT_COL")
            conexion.commit()
            cursor.execute("EXEC CARGA_CUADRAJE_SAP_RECEIPT_COL")
            conexion.commit()
            cursor.execute("EXEC CARGA_CUADRAJE_SAP_SHIPMENT_COL")
            conexion.commit()

            return True
        except Exception as exception:
            logger.error(f"Se presento una incidencia al ejecutar el proceso de envio de correos a tiendas tiendas: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

            
    def executeUpdateDiferenciasCol(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute("EXEC REPROCESO_OBTENER_DOCUMENTOS_COL")
            conexion.commit()
            cursor.execute("EXEC REPROCESO_CUADRAJE_WMS_OPEN_SHIPMENT_COL")
            conexion.commit()
            cursor.execute("EXEC REPROCESO_CUADRAJE_WMS_RECEIPT_COL")
            conexion.commit()
            cursor.execute("EXEC REPROCESO_CUADRAJE_WMS_SHIPMENT_COL")
            conexion.commit()
            cursor.execute("EXEC REPROCESO_CUADRAJE_SAP_RECEIPT_COL")
            conexion.commit()
            cursor.execute("EXEC REPROCESO_CUADRAJE_SAP_SHIPMENT_COL")
            conexion.commit()

            return True
        except Exception as exception:
            logger.error(f"Se presento una incidencia al ejecutar el proceso de envio de correos a tiendas tiendas: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)


    def getReciboSapCol(self, recibos):
        try:
            logger.error("Entro al proceso getReciboSap Col")
            conexion=self.getConexion()
            cursor=conexion.cursor()
            recibosList=[]
            consulta="SELECT DocSDT, CLOSE_QTY_SDT, CLOSE_QTY_TR, OPEN_QTY_SDT, OPEN_QTY_TR, TOTAL_QTY_SDT, TOTAL_QTY_TR, WMS, DIF, CLOSE_DATE, STS_WMS, VAL FROM SAP_RECEIPT_COL WHERE DocSDT in ("+recibos+")  ORDER BY DocSDT"
            logger.error(consulta)
            cursor.execute(consulta)
            registros=cursor.fetchall()
            for registro in registros:
                reciboSap=ReciboSap(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11])
                recibosList.append(reciboSap)
            return recibosList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)


    def getReciboSapByValorCol(self, valor):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            recibosList=[]
            consulta="SELECT DocSDT, CLOSE_QTY_SDT, CLOSE_QTY_TR, OPEN_QTY_SDT, OPEN_QTY_TR, TOTAL_QTY_SDT, TOTAL_QTY_TR, WMS, DIF, CLOSE_DATE, STS_WMS, VAL FROM SAP_RECEIPT_COL WHERE VAL = '"+valor+"' ORDER BY DocSDT"
            cursor.execute(consulta)
            registros=cursor.fetchall()
            for registro in registros:
                reciboSap=ReciboSap(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11])
                recibosList.append(reciboSap)
            return recibosList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)


    def getPedidoSapCol(self, pedidos):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            pedidosList=[]
            consulta="SELECT DocSDT, CLOSE_QTY_SDT, CLOSE_QTY_TR, OPEN_QTY_SDT, OPEN_QTY_TR, TOTAL_QTY_SDT, TOTAL_QTY_TR, WMS_CLOSE, DIF, SHIP_DATE, STS_WMS, VAL, OPEN_SAP FROM SAP_SHIPMENT_COL WHERE DocSDT in ("+pedidos+")  ORDER BY DocSDT"
            cursor.execute(consulta)
            registros=cursor.fetchall()
            for registro in registros:
                pedidoSap=PedidoSap(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11], registro[12])
                pedidosList.append(pedidoSap)
            return pedidosList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getPedidoSapAbiertosCol(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            pedidosList=[]
            consulta="SELECT DocSDT, CLOSE_QTY_SDT, CLOSE_QTY_TR, OPEN_QTY_SDT, OPEN_QTY_TR, TOTAL_QTY_SDT, TOTAL_QTY_TR, WMS_CLOSE, DIF, SHIP_DATE, STS_WMS, VAL, OPEN_SAP FROM SAP_SHIPMENT_COL WHERE OPEN_SAP='O' ORDER BY DocSDT"
            cursor.execute(consulta)
            registros=cursor.fetchall()
            for registro in registros:
                pedidoSap=PedidoSap(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11], registro[12])
                pedidosList.append(pedidoSap)
            return pedidosList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getPedidoSapByValorCol(self, valor, limite):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            pedidosList=[]
            top=""
            if limite:
                top=" top 100"
            consulta="SELECT"+top+" DocSDT, CLOSE_QTY_SDT, CLOSE_QTY_TR, OPEN_QTY_SDT, OPEN_QTY_TR, TOTAL_QTY_SDT, TOTAL_QTY_TR, WMS_CLOSE, DIF, SHIP_DATE, STS_WMS, VAL, OPEN_SAP FROM SAP_SHIPMENT_COL WHERE VAL='"+valor+"' ORDER BY DocSDT"
            cursor.execute(consulta)
            registros=cursor.fetchall()
            for registro in registros:
                pedidoSap=PedidoSap(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11], registro[12])
                pedidosList.append(pedidoSap)
            return pedidosList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)


    def insertSapReceiptValCol(self, idRecibo):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute("INSERT INTO SAP_RECEIPT_VAL_COL (DocSDT, FECHA, VAL) VALUES("+idRecibo+", GETDATE(), 'OK')")
            conexion.commit()
            return True
        except Exception as exception:
            logger.error(f"Se presento una incidencia al ejecutar el proceso de envio de correos a tiendas tiendas: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)


    def insertSapShipmentValCol(self, idShipment): 
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute("INSERT INTO SAP_SHIPMENT_VAL_COL (DocSDT, FECHA, VAL) VALUES("+idShipment+", GETDATE(), 'OK')")
            conexion.commit()
            return True
        except Exception as exception:
            logger.error(f"Se presento una incidencia al ejecutar el proceso de envio de correos a tiendas tiendas: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)