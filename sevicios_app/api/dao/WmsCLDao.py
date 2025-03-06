import logging
import pyodbc
from sevicios_app.vo.pedido import Pedido
from sevicios_app.vo.carga import Carga
from sevicios_app.vo.detalleCarga import DetalleCarga
from sevicios_app.vo.reciboPendiente import ReciboPendiente
from sevicios_app.vo.split import Split
from sevicios_app.vo.contenedorEpq import ContenedorEpq
from sevicios_app.vo.ubicacionVacia import UbiacionVacia
from sevicios_app.vo.contenedorOla import ContenedorOla
from sevicios_app.vo.olaPiezaCont import OlaPiezaCont
from sevicios_app.vo.detalleContenedorOla import DetalleContenedorOla
from sevicios_app.vo.lineaOla import LineaOla
from sevicios_app.vo.inventario import Inventario
from sevicios_app.vo.tareaReaSurtAbierta import TareaReaSurtAbierta
from sevicios_app.vo.contenedor import Contenedor
from sevicios_app.vo.transaccionPickPut import TransaccionPickPut
from sevicios_app.vo.wave import Wave
from sevicios_app.vo.splitCl import SplitCl
from sevicios_app.vo.unitMesure import UnitMesure
from sevicios_app.vo.porcentaje import Porcentaje
from sevicios_app.vo.contenedorQc import ContenedorQc
from sevicios_app.vo.itemcontenedorqc import ItemContenedorQc
from sevicios_app.vo.LocationItem import LocationItem

logger = logging.getLogger('')

class WMSCLDao():

    def getConexion(self):
        try:
            direccion_servidor = '192.168.84.34'
            nombre_bd = 'ILS'
            nombre_usuario = 'manh'
            password = 'Pa$$w0rdLDMCL'
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
        
    def getTransaccionesPickPut(self, oneHundred):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            transaccionesList=[]
            top = ""
            if oneHundred==True:
                top = "top 100"
            cursor.execute("SELECT "+top+" TH.ITEM, (SELECT DESCRIPTION FROM GENERIC_CONFIG_DETAIL WHERE RECORD_TYPE = 'HIST TR TY' AND IDENTIFIER = th.TRANSACTION_TYPE) 'TRANSACTION', "+
                           "TH.USER_NAME, th.REFERENCE_ID, DATEADD(HH,-3,th.DATE_TIME_STAMP) DATE_STAMP, th.WORK_TYPE, th.USER_STAMP, th.LOCATION, th.QUANTITY, "+
                           "BEFORE_IN_TRANSIT_QTY, AFTER_IN_TRANSIT_QTY, BEFORE_ON_HAND_QTY, AFTER_ON_HAND_QTY, BEFORE_ALLOC_QTY, AFTER_ALLOC_QTY, BEFORE_SUSPENSE_QTY, AFTER_SUSPENSE_QTY "+
                           "FROM TRANSACTION_HISTORY th (NOLOCK) "+
                           "where th.TRANSACTION_TYPE=120 and format(th.ACTIVITY_DATE_TIME, 'yyyy-MM-dd')= format(GETDATE(), 'yyyy-MM-dd') "+
                           "order by th.DATE_TIME_STAMP desc")
            registros=cursor.fetchall()
            for registro in registros:
                transaccionPickPut=TransaccionPickPut(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11], registro[12], registro[13], registro[14], registro[15], registro[16])
                transaccionesList.append(transaccionPickPut)
            return transaccionesList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
        
    def getLineasOla(self, oneHundred, ola):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            lineaOlaList=[]
            top=""
            if oneHundred:
                  top="top 100 "
            cursor.execute("select "+top+"LAUNCH_NUM, SHIPMENT_ID, ITEM, ITEM_DESC, TOTAL_QTY, STATUS1 "+
                           "from SHIPMENT_DETAIL where LAUNCH_NUM='"+ola+"' "+
                           "and STATUS1 <> 900 and total_qty != 0 and STATUS1 >= 300")
            registros=cursor.fetchall()
            for registro in registros:
                lineaOla=LineaOla(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5])
                lineaOlaList.append(lineaOla)
            return lineaOlaList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
    
    def getOlaPiezasContenedores(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            olaPiezaContList=[]
            cursor.execute("SELECT QTY.LAUNCH_NUM, QTY.TOTAL_QUANTITY, isnull(CONT.CONTENEDORES, 0) CONTENEDORES "+
                           "FROM "+
                           "(select SH.LAUNCH_NUM, "+
                           "sum(SD.TOTAL_QTY) TOTAL_QUANTITY "+
                           "from SHIPMENT_HEADER SH "+
                           "INNER JOIN SHIPMENT_DETAIL SD ON SD.INTERNAL_SHIPMENT_NUM= SH.INTERNAL_SHIPMENT_NUM "+
                           "GROUP BY SH.LAUNCH_NUM) QTY "+
                           "LEFT JOIN "+
                           "(select SH.LAUNCH_NUM, "+
                           "COUNT(SC.CONTAINER_ID) CONTENEDORES "+
                           "from SHIPMENT_HEADER SH "+
                           "INNER JOIN SHIPPING_CONTAINER SC ON SC.INTERNAL_SHIPMENT_NUM=SH.INTERNAL_SHIPMENT_NUM AND SC.CONTAINER_ID IS NOT NULL AND SC.USER_DEF3!='ELIMINAR CONT' AND SC.CONTAINER_TYPE!='ERROR' "+
                           "GROUP BY SH.LAUNCH_NUM) CONT ON CONT.LAUNCH_NUM=QTY.LAUNCH_NUM "+
                           "order by QTY.LAUNCH_NUM")
            registros=cursor.fetchall()
            for registro in registros:
                olaPiezaCont=OlaPiezaCont(registro[0], registro[1], registro[2])
                olaPiezaContList.append(olaPiezaCont)
            return olaPiezaContList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getWaveAnalysis(self, oneHundred, wave):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            waveList=[]
            top=""
            if oneHundred:
                top="top 100 "
            cursor.execute("SELECT "+top+"sh.ITEM, it.description, it.STORAGE_TEMPLATE, sh.SHIPMENT_ID, sh.LAUNCH_NUM "+
                           ", (SELECT SF.STATUS_NAME FROM FUNCTIONAL_AREA_STATUS_FLOW SF (NOLOCK) "+ 
                           "WHERE SF.status = sh.STATUS1 AND SF.FUNCTIONAL_AREA = 'Outbound' ) STATUS "+
                           ", sh.REQUESTED_QTY "+
                           ", (SELECT SUM(SAR.ALLOCATED_QTY) FROM SHIPMENT_ALLOC_REQUEST SAR (NOLOCK) "+ 
                           "WHERE SAR.SHIPMENT_ID = sh.SHIPMENT_ID AND SAR.ITEM = sh.ITEM)ALLOCATED_QTY "+
                           ",SUM(mt.ON_HAND_QTY) - SUM(mt.ALLOCATED_QTY) - SUM(mt.SUSPENSE_QTY) AS AV "+
                           ",SUM(mt.ON_HAND_QTY) OH, SUM(mt.ALLOCATED_QTY) AL, SUM(mt.IN_TRANSIT_QTY) IT, SUM(mt.SUSPENSE_QTY) SU "+
                           ", sh.CUSTOMER, sh.ITEM_CATEGORY1, DATEADD(HH,-6, HE.CREATION_DATE_TIME_STAMP) CREATION_DATE_TIME_STAMP, HE.SCHEDULED_SHIP_DATE "+
                           ", it.DIVISION "+
                           ", 'CONV' = CASE "+ 
                           "WHEN it.STORAGE_TEMPLATE = 'PZA-INR-CJA' THEN "+ 
                           "(SELECT un.CONVERSION_QTY FROM ITEM_UNIT_OF_MEASURE un (NOLOCK) WHERE un.ITEM = it.ITEM and un.QUANTITY_UM = 'INR') "+
                           "ELSE 1 END "+
                           "FROM "+ 
                           "SHIPMENT_DETAIL sh (NOLOCK) "+
                           "inner join SHIPMENT_HEADER HE (NOLOCK) on sh.SHIPMENT_ID = HE.SHIPMENT_ID "+ 
                           "left outer join METADATA_INSIGHT_INVENTORY_VIEW MT (NOLOCK) on sh.ITEM = MT.ITEM "+ 
                           "and MT.ACTIVE = 'Y' and MT.INVENTORY_STS = 'Disponible' "+
                           "left join ITEM it (NOLOCK) on sh.ITEM = it.ITEM "+
                           "WHERE "+ 
                           "HE.LEADING_STS <= 300 "+
                           "and HE.LAUNCH_NUM=? "+
                           "GROUP BY sh.ITEM, it.STORAGE_TEMPLATE, sh.SHIPMENT_ID,	sh.LAUNCH_NUM, sh.REQUESTED_QTY, sh.ERP_ORDER_LINE_NUM "+
                           ", sh.CUSTOMER, sh.ITEM_CATEGORY1, HE.CREATION_DATE_TIME_STAMP, HE.SCHEDULED_SHIP_DATE, it.DIVISION "+
                           ", sh.STATUS1 "+
                           ", it.description, it.ITEM "+
                           "ORDER BY sh.SHIPMENT_ID, sh.ERP_ORDER_LINE_NUM", (wave))
            registros=cursor.fetchall()
            for registro in registros:
                wave=Wave(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11], registro[12], registro[13], registro[14], registro[15], registro[16], registro[17], registro[18])
                waveList.append(wave)
            return waveList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getSplit(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            splitList=[]
            cursor.execute("select sh.SHIPMENT_ID, format(sh.CREATION_DATE_TIME_STAMP, 'yyyy-MM-dd') fecha_creacion, count(*) "+
                           "from SHIPMENT_HEADER sh "+
                           "inner join shipping_container sc on sc.INTERNAL_SHIPMENT_NUM=sh.INTERNAL_SHIPMENT_NUM "+
                           "where CREATION_PROCESS_STAMP='SplitShipmentBelowStatus' and TRAILING_STS<900 "+
                           "and sc.CONTAINER_ID is not null "+
                           "group by sh.SHIPMENT_ID, format(sh.CREATION_DATE_TIME_STAMP, 'yyyy-MM-dd') "+
                           "order by fecha_creacion, sh.SHIPMENT_ID")
            registros=cursor.fetchall()
            for registro in registros:
                split=SplitCl(registro[0], registro[1], registro[2])
                splitList.append(split)
            return splitList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getUnitMesureLocation(self, location, item):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            coinciden=''
            ubicaciones=''
            descripcion=''
            inner=''
            caja=''
            sku=''
            unitMesure=None
            cursor.execute("INSERT INTO MNSO_LOG_AUDITORIA(UBICACION, ITEM, FECHA) VALUES (?, ?, GETDATE())", (location, item))
            cursor.commit()
            cursor.execute("SELECT IT.DESCRIPTION, "+ 
                           "(select IUOM.CONVERSION_QTY from ITEM_UNIT_OF_MEASURE IUOM where IUOM.ITEM=IT.ITEM AND IUOM.QUANTITY_UM='INR') PiezasInner, "+
                           "(select IUOM.CONVERSION_QTY from ITEM_UNIT_OF_MEASURE IUOM where IUOM.ITEM=IT.ITEM AND IUOM.QUANTITY_UM='CJA') PiezasCja "+
                           "FROM LOCATION_INVENTORY LI "+
                           "INNER JOIN ITEM IT ON IT.ITEM=LI.ITEM "+
                           "INNER JOIN ITEM_CROSS_REFERENCE ICR ON ICR.ITEM=LI.ITEM "+
                           "WHERE ICR.X_REF_ITEM=? "+
                           "AND LI.LOCATION =? ", (item, location))
            fila = cursor.fetchone()
            if fila:
                coinciden='Y'
                descripcion=fila[0]
                inner=fila[1]
                caja=fila[2]
                ubicaciones=location
            else:
                cursor.execute("SELECT IT.DESCRIPTION, "+
                               "(select IUOM.CONVERSION_QTY from ITEM_UNIT_OF_MEASURE IUOM where IUOM.ITEM=IT.ITEM AND IUOM.QUANTITY_UM='INR') PiezasInner, "+
                               "(select IUOM.CONVERSION_QTY from ITEM_UNIT_OF_MEASURE IUOM where IUOM.ITEM=IT.ITEM AND IUOM.QUANTITY_UM='CJA') PiezasCja, "+
                               "IT.ITEM " +
                               "FROM ITEM IT "+
                               "INNER JOIN ITEM_CROSS_REFERENCE ICR ON ICR.ITEM=IT.ITEM "+
                               "WHERE ICR.X_REF_ITEM= ?", (item))
                registro = cursor.fetchone()
                if registro:
                    coinciden='N'
                    descripcion=registro[0]
                    inner=registro[1]
                    caja=registro[2]
                    sku=registro[3]
                else:
                    coinciden='NI'
                if sku!='':
                    cursor.execute("select LOCATION from LOCATION_INVENTORY where ITEM=? and location like 'R%'", (sku))
                    registros=cursor.fetchall()
                    for registro in registros:
                        if ubicaciones=='':
                            ubicaciones=registro[0] 
                        else:
                            ubicaciones = ubicaciones + ', '+registro[0]
            return UnitMesure(descripcion, inner, caja, ubicaciones, coinciden)
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
    
    def getTareasReaSurtAbiertas(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            tareasList=[]
            cursor.execute("SELECT WI.WORK_UNIT, WI.INSTRUCTION_TYPE, WI.WORK_TYPE, WI.USER_DEF1, WI.USER_DEF6, WI.CONDITION, WI.ITEM, WI.ITEM_DESC, WI.REFERENCE_ID, WI.FROM_LOC, WI.FROM_QTY, "+
                           "WI.TO_LOC, WI.TO_QTY, WI.LAUNCH_NUM, WI.INTERNAL_INSTRUCTION_NUM, WI.CONVERTED_QTY, WI.CONTAINER_ID, CT.CONTAINER_TYPE, FORMAT(WI.AGING_DATE_TIME, 'dd/MM/yyyy hh:mm:ss'), "+
                           "FORMAT(WI.START_DATE_TIME, 'dd/MM/yyyy hh:mm:ss') "+  
                           "FROM WORK_INSTRUCTION WI LEFT JOIN (SELECT * FROM SHIPPING_CONTAINER WHERE CONTAINER_TYPE <> '-' AND CONTAINER_ID IS NOT NULL) CT ON WI.CONTAINER_ID = CT.CONTAINER_ID "+ 
                           "WHERE (WORK_TYPE = 'Reab de Reserva a Picking' or WORK_TYPE LIKE 'Surt%') AND INSTRUCTION_TYPE = 'Detail' AND CONDITION ='OPEN'")
            registros=cursor.fetchall()
            for registro in registros:
                tareaReaSurtAbierta=TareaReaSurtAbierta(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11], registro[12], registro[13], registro[14], registro[15], registro[16], registro[17], registro[18], registro[19])
                tareasList.append(tareaReaSurtAbierta)
            return tareasList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getContenedores(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            contenedoresList=[]
            cursor.execute("SELECT SCH.CONTAINER_ID, SH.SHIPMENT_ID, SCH.CONTAINER_TYPE, SH.LEADING_STS, SC.ITEM, SC.QUANTITY, SH.CUSTOMER, SC.LAUNCH_NUM, format(dateadd(HOUR, -6,sc.DATE_TIME_STAMP), 'yyyy-MM-dd') "+
                           "FROM SHIPPING_CONTAINER SC "+ 
                           "INNER JOIN SHIPMENT_HEADER SH ON SH.INTERNAL_SHIPMENT_NUM=SC.INTERNAL_SHIPMENT_NUM "+
                           "INNER JOIN (SELECT CONTAINER_ID, CONTAINER_TYPE FROM SHIPPING_CONTAINER WHERE CONTAINER_ID IS NOT NULL) SCH ON SCH.CONTAINER_ID=SC.PARENT_CONTAINER_ID "+
                           "WHERE SC.QUANTITY>0 AND SH.LEADING_STS<999")
            registros=cursor.fetchall()
            for registro in registros:
                contenedor=Contenedor(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8])
                contenedoresList.append(contenedor)
            return contenedoresList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
                
    def getRecibosPendientes(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            reciboPendienteList=[]
            cursor.execute("select rh.RECEIPT_ID, rd.ITEM, rd.ITEM_DESC, rd.TOTAL_QTY, rd.OPEN_QTY from RECEIPT_HEADER rh inner join RECEIPT_DETAIL rd on rd.RECEIPT_ID=rh.RECEIPT_ID where CLOSE_DATE is null and TRAILING_STS <900 and rd.OPEN_QTY>0")
            registros=cursor.fetchall()
            for registro in registros:
                reciboPendiente=ReciboPendiente(registro[0], registro[1], registro[2], registro[3], registro[4])
                reciboPendienteList.append(reciboPendiente)
            return reciboPendienteList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getUbicacionesVaciasReservaTop(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            ubicacionesVaciasList=[]
            cursor.execute("select top 100 location, LOCATION_STS, ACTIVE "+
                           "from location "+
                           "where LOCATION_STS='Empty' "+
                           "AND  ACTIVE='Y' "+
                           "and location_type='RESERVA'")
            registros=cursor.fetchall()
            for registro in registros:
                ubiacionVacia=UbiacionVacia(registro[0], registro[1], registro[2])
                ubicacionesVaciasList.append(ubiacionVacia)
            return ubicacionesVaciasList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
    
    def getUbicacionesVaciasAll(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            ubicacionesVaciasList=[]
            cursor.execute("select location, LOCATION_STS, ACTIVE "+
                           "from location "+
                           "where LOCATION_STS='Empty' "+
                           "AND  ACTIVE='Y' "+
                           "and location_type='RESERVA'")
            registros=cursor.fetchall()
            for registro in registros:
                ubiacionVacia=UbiacionVacia(registro[0], registro[1], registro[2])
                ubicacionesVaciasList.append(ubiacionVacia)
            return ubicacionesVaciasList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
                
    def getEstatusContenedores(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            contenedorOlaList=[]
            cursor.execute("SELECT INF.LAUNCH_NUM, 'SEM'+CAST((SELECT DATEPART(ISO_WEEK, LS.LAUNCH_DATE_TIME_STARTED)from LAUNCH_STATISTICS LS where LS.INTERNAL_LAUNCH_NUM =INF.LAUNCH_NUM) AS VARCHAR) SEMANA, "+
                           "INF.PEDIDOS, INF.CONTENEDORES, INF.[PICKING PENDING], INF.[IN PICKING], INF.[PACKING PENDING], INF.[IN PACKING], "+
                           "INF.[STAGING PENDING], INF.[LOADING PENDING], INF.[SHIP CONFIRM PENDING], INF.[LOAD CONFIRM PENDING], INF.CLOSED, "+
                           "INF.Carton, INF.Bolsa "+
                           "FROM (select SH.LAUNCH_NUM, COUNT(DISTINCT SH.SHIPMENT_ID) PEDIDOS, "+
                           "COUNT(SC.CONTAINER_ID) CONTENEDORES, "+
                           "count(case when SC.status=300 then 1 end) 'PICKING PENDING', "+
                           "count(case when SC.status=301 then 1 end) 'IN PICKING', "+
                           "count(case when SC.status=400 then 1 end) 'PACKING PENDING', "+
                           "count(case when SC.status=401 then 1 end) 'IN PACKING', "+
                           "count(case when SC.status=600 then 1 end) 'STAGING PENDING', "+
                           "count(case when SC.status=650 then 1 end) 'LOADING PENDING', "+
                           "count(case when SC.status=700 then 1 end) 'SHIP CONFIRM PENDING', "+
                           "count(case when SC.status=800 then 1 end) 'LOAD CONFIRM PENDING', "+
                           "count(case when SC.status=900 then 1 end) 'CLOSED', "+
                           "count(case when SC.CONTAINER_CLASS='Carton' then 1 end) 'Carton', "+
                           "count(case when SC.CONTAINER_CLASS='Bolsa' then 1 end) 'Bolsa' "+
                           "from SHIPMENT_HEADER SH "+
                           "INNER JOIN (select CONTAINER_ID, STATUS, CONTAINER_CLASS, INTERNAL_SHIPMENT_NUM, CONTAINER_TYPE, ISNULL(USER_DEF3, 'NULL') USER_DEF3 from SHIPPING_CONTAINER WHERE CONTAINER_ID IS NOT NULL) SC "+
                           "ON SC.INTERNAL_SHIPMENT_NUM=SH.INTERNAL_SHIPMENT_NUM AND SC.USER_DEF3!='ELIMINAR CONT' AND SC.CONTAINER_TYPE!='ERROR' "+
                           "GROUP BY SH.LAUNCH_NUM) INF "+
                           "ORDER BY SEMANA, INF.LAUNCH_NUM")
            registros=cursor.fetchall()
            for registro in registros:
                contenedorOla=ContenedorOla(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11], registro[12], registro[13], registro[14])
                contenedorOlaList.append(contenedorOla)
            return contenedorOlaList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getDetalleEstatusContenedores(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            detalleContenedorOlaList=[]
            cursor.execute("select SH.LAUNCH_NUM, SH.SHIPMENT_ID, SC.CONTAINER_ID, SC.status, "+
                           "CASE "+
                           "WHEN SC.status=300 THEN 'PICKING PENDING' "+
                           "WHEN SC.status=301 THEN 'IN PICKING' "+
                           "WHEN SC.status=400 THEN 'PACKING PENDING' "+
                           "WHEN SC.status=401 THEN 'IN PACKING' "+
                           "WHEN SC.status=600 THEN 'STAGING PENDING' "+
                           "WHEN SC.status=650 THEN 'LOADING PENDING' "+
                           "WHEN SC.status=700 THEN 'SHIP CONFIRM PENDING' "+
                           "WHEN SC.status=800 THEN 'LOAD CONFIRM PENDING' "+ 
                           "WHEN SC.status=900 THEN 'CLOSED' "+
                           "END AS ESTATUS, "+
                           "CASE "+ 
                           "WHEN SC.CONTAINER_CLASS='Carton' THEN 'CARTON' "+
                           "WHEN SC.CONTAINER_CLASS='Bolsa' THEN 'BOLSA' "+ 
                           "END AS TIPO "+
                           "from SHIPMENT_HEADER SH "+
                           "INNER JOIN (SELECT CONTAINER_ID, CONTAINER_CLASS, status, INTERNAL_SHIPMENT_NUM, ISNULL(USER_DEF3, 'NULL') USER_DEF3, CONTAINER_TYPE FROM SHIPPING_CONTAINER WHERE CONTAINER_ID IS NOT NULL) SC ON SC.INTERNAL_SHIPMENT_NUM=SH.INTERNAL_SHIPMENT_NUM "+
                           "AND SC.USER_DEF3!='ELIMINAR CONT' AND SC.CONTAINER_TYPE!='ERROR' "+
                           "ORDER BY SC.status, SH.SHIPMENT_ID, TIPO, SC.CONTAINER_ID")
            registros=cursor.fetchall()
            for registro in registros:
                detalleContenedorOla=DetalleContenedorOla(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5])
                detalleContenedorOlaList.append(detalleContenedorOla)
            return detalleContenedorOlaList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getInventario(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            inventarioList=[]
            cursor.execute("select li.LOCATION, li.PERMANENT, lo.ACTIVE, lo.LOCATING_ZONE, li.ITEM, li.ITEM_DESC, li.INVENTORY_STS, (li.ON_HAND_QTY+li.IN_TRANSIT_QTY-li.ALLOCATED_QTY-li.SUSPENSE_QTY), li.ON_HAND_QTY, li.IN_TRANSIT_QTY, li.ALLOCATED_QTY, li.SUSPENSE_QTY, it.ITEM_CATEGORY1, it.ITEM_CATEGORY2, it.ITEM_CATEGORY3 "+
                           "from LOCATION_INVENTORY li "+
                           "inner join location lo on lo.LOCATION=li.LOCATION "+
                           "inner join item it on it.ITEM=li.ITEM "+
                           "where li.LOCATION like 'R-%' "+
                           "order by li.LOCATION")
            registros=cursor.fetchall()
            for registro in registros:
                inventario=Inventario(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], registro[11], registro[12], registro[13], registro[14])
                inventarioList.append(inventario)
            return inventarioList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
                
    def getPorcentajeSKUsPrioritarios(self, container):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute("select sc.PARENT_CONTAINER_ID container, isnull(sum(case when sp.item is not null then sc.QUANTITY end), 0)/sum(sc.QUANTITY)*100 porcentaje "+
                           "from shipping_container sc "+
                           "left join  (select skuprioritarioitem COLLATE Latin1_General_CI_AS item from [192.168.84.179].recepciontiendacl.dbo.SKUPrioritario) sp on sp.item=sc.ITEM "+
                           "where sc.PARENT_CONTAINER_ID=? group by sc.PARENT_CONTAINER_ID", (container))
            fila = cursor.fetchone()
            if fila:
                return Porcentaje(fila.container, fila.porcentaje)
            return Porcentaje(None, None)
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getContainerQc(self): #ContenedorQc
            try:
                conexion=self.getConexion()
                cursor=conexion.cursor()
                containerList=[]
                cursor.execute("select CONTAINER_ID, WEIGHT, USER_DEF1, TOTAL_FREIGHT_CHARGE, BASE_FREIGHT_CHARGE, FREIGHT_DISCOUNT, ACCESSORIAL_CHARGE, QC_ASSIGNMENT_ID, QC_STATUS, " +
                               "(select convert(nvarchar(MAX), DATEADD(HH,-6, activity_date_time), 20) from TRANSACTION_HISTORY th where th.CONTAINER_ID =SC.CONTAINER_ID and TRANSACTION_TYPE=210) fecha "+ #Modify -6 hour
                                "from SHIPPING_CONTAINER SC " +
                                "where SC.FREIGHT_DISCOUNT>0 and SC.CONTAINER_ID is not null and SC.QC_ASSIGNMENT_ID is not null")
                registros=cursor.fetchall()
                for registro in registros:
                    container=ContenedorQc(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8],registro[9])
                    containerList.append(container)
                return containerList
            except Exception as exception:
                logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
                raise exception
            finally:
                if conexion!= None:
                    self.closeConexion(conexion)

    def getItemContainerQc(self): #ItemContenedoresQc
            try:
                conexion=self.getConexion()
                cursor=conexion.cursor()
                itemContenedorQcList=[]
                cursor.execute("select sc.ITEM, count(*) NumOcur, (select count(*) from shipping_container sct where sct.ITEM=sc.ITEM and sct.status>=650 ) TotalCont " +
                                "from SHIPPING_CONTAINER sc " +
                                "inner join SHIPPING_CONTAINER scq on scq.CONTAINER_ID=sc.PARENT_CONTAINER_ID and scq.QC_ASSIGNMENT_ID is not null " +
                                "where sc.status>=650 "+
                                "group by sc.ITEM order by NumOcur desc")
                registros=cursor.fetchall()
                for registro in registros:
                    itemContenedorQc=ItemContenedorQc(registro[0], registro[1], registro[2])
                    itemContenedorQcList.append(itemContenedorQc)
                return itemContenedorQcList
            except Exception as exception:
                logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
                raise exception
            finally:
                if conexion!= None:
                    self.closeConexion(conexion)

#Container insert, delete and Update dimension item

    def validate_columns(self, dataframe, action, expected_columns):
        columns = dataframe.columns
        missing_columns = [col for col in expected_columns[action] if col not in columns]
        if missing_columns:
            return f"Faltan las siguientes columnas: {', '.join(missing_columns)}"
        return None

    def check_existing_containers(self, container_types):
        conexion = self.getConexion()
        cursor = conexion.cursor()
        placeholders = ', '.join(['?'] * len(container_types))
        sql_check = f"SELECT CONTAINER_TYPE FROM CONTAINER_TYPE WHERE CONTAINER_TYPE IN ({placeholders})"
        cursor.execute(sql_check, container_types)
        results = set(row[0] for row in cursor.fetchall())
        self.closeConexion(conexion)
        return results
    
    def check_existing_items(self, sku_list, um_list):
        conexion = self.getConexion()
        cursor = conexion.cursor()
        sql_check = """
        SELECT ITEM, QUANTITY_UM 
        FROM ITEM_UNIT_OF_MEASURE 
        WHERE ITEM = ? AND QUANTITY_UM = ?
        """
        existing_items = set()
        for sku, um in zip(sku_list, um_list):
            cursor.execute(sql_check, (sku, um))
            for row in cursor.fetchall():
                existing_items.add((row[0], row[1]))
        self.closeConexion(conexion)
        return existing_items
        

    def insert_containers(self, values_to_insert):
        conexion = self.getConexion()
        cursor = conexion.cursor()
        sql_insert = """
        INSERT INTO CONTAINER_TYPE(CONTAINER_TYPE, DESCRIPTION, CONTAINER_CLASS, EMPTY_WEIGHT, WEIGHT_UM, LENGTH, WIDTH, HEIGHT, ACTIVE, WEIGHT_TOLERANCE, MAXIMUM_WEIGHT, USE_AS_DEFAULT, DATE_TIME_STAMP)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'N', GETDATE())
        """
        try:
            total_params_per_record = 12  
            max_params = 2100  
            sub_batch_size = max_params // total_params_per_record  

            for i in range(0, len(values_to_insert), sub_batch_size):
                sub_batch = values_to_insert[i:i + sub_batch_size]
                cursor.executemany(sql_insert, sub_batch)
            conexion.commit()
            print("Batch insertado correctamente.")
            return True
        except Exception as e:
            logger.error(f"Error al insertar en bloque: {e}")
            conexion.rollback()
            return str(e)
        finally:
            self.closeConexion(conexion) 


    def delete_containers(self, container_types):
        conexion = self.getConexion()
        cursor = conexion.cursor()
        try:
            sql_delete = f"DELETE FROM CONTAINER_TYPE WHERE CONTAINER_TYPE in {tuple(container_types)}"
            cursor.execute(sql_delete)
            conexion.commit()
            logger.info("Eliminaciones realizadas con éxito")
            return True
        except Exception as e:
            logger.error(f"Error al eliminar en bloque: {e}")
            conexion.rollback()
            return str(e)
        finally:
            self.closeConexion(conexion)

    def actualizar_unidad_de_medida(self, data):
        conexion = self.getConexion()
        cursor = conexion.cursor()
        sql_update = """
        UPDATE ITEM_UNIT_OF_MEASURE
        SET LENGTH = ?, WIDTH = ?, HEIGHT = ?, WEIGHT = ?
        WHERE ITEM = ? AND QUANTITY_UM = ?
        """
        try:
            values_to_update = [
                (row['LONGITUD'], row['ANCHURA'], row['ALTURA'], row['PESO'], row['SKU'], row['UM'])
                for row in data if (row['SKU'], row['UM']) in self.check_existing_items([row['SKU']], [row['UM']])
            ]
            if not values_to_update:
                return "No se encontraron elementos para actualizar."
                
            cursor.executemany(sql_update, values_to_update)
            conexion.commit()
            logger.info("Actualizaciones realizadas con éxito")
            return True
        except Exception as e:
            logger.error(f"Error al actualizar en bloque: {e}")
            conexion.rollback()
            return str(e)
        finally:
            self.closeConexion(conexion)