import logging
import pyodbc
from sevicios_app.vo.inventarioDetalleErpWms import InventarioDetalleErpWms
from sevicios_app.vo.inventarioItem import InventarioItem
from sevicios_app.vo.inventarioTop import InventarioTop
from sevicios_app.vo.inventarioWmsErp import InventarioWmsErp

logger = logging.getLogger('')

class ScaleIntColDao():

    def getConexion(self):
        try:
            direccion_servidor = '192.168.84.107'
            nombre_bd = 'SCALEINTCOLOMBIAV2'
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
        
    def getWmsErp(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            comparativoWmsErpList=[]
            cursor.execute("SELECT  " + 
                            "YY.WAREHOUSE  " + 
                            ",SUM(YY.WMS_ONHAND) AS WMS_ONHAND  " + 
                            ",SUM(YY.ERP_ONHAND) AS ERP_ONHAND  " + 
                            ",SUM(YY.DIF_ONHAND) AS DIFERENCIA   " + 
                            ",SUM(YY.DIF_OH_ABS) AS 'DIFERENCIA ABS'  " + 
                            ",SUM(YY.WMS_INTRANSIT) AS WMS_INTRANSIT  " + 
                            ",(SELECT COUNT (XX.ITEM) FROM INVENTARIO XX (NOLOCK) WHERE XX.ERP_ALMACEN = SUBSTRING(WAREHOUSE,1,6)   " + 
                            "AND (XX.WMS_DISPONIBLE + XX.WMS_SUSPENDIDO + XX.WMS_TRANSITO) != 0 AND CONVERT(DATE, XX.FECHA) = CONVERT(DATE,GETDATE())) AS '#ITEMS_WMS'  " + 
                            ",(SELECT COUNT (XX.ITEM) FROM INVENTARIO XX (NOLOCK) WHERE XX.ERP_ALMACEN = SUBSTRING(WAREHOUSE,1,6)   " + 
                            "AND (XX.ERP_DISPONIBLE) != 0 AND CONVERT(DATE, XX.FECHA) >= CONVERT(DATE,GETDATE())) AS '#ITEMS_ERP'  " + 
                            ",(SELECT COUNT (XX.ITEM) FROM INVENTARIO XX (NOLOCK) WHERE XX.ERP_ALMACEN = SUBSTRING(WAREHOUSE,1,6)   " + 
                            "AND IIF(abs(XX.DIFERENCIA_DISPONIBLE)>0,(abs(XX.DIFERENCIA_DISPONIBLE) - XX.WMS_TRANSITO),abs(XX.DIFERENCIA_DISPONIBLE)) != 0   " + 
                            "AND CONVERT(DATE, XX.FECHA) >= CONVERT(DATE,GETDATE())) AS '#ITEMS_DIF'  " + 
                            "FROM (  " + 
                            "SELECT  " + 
                            "DATEADD(HH,3,FECHA) as DATE_TIME  " + 
                            ",[ITEM]  " + 
                            ",'CEN-PT - PRODUCTO TERMINADO' WAREHOUSE  " + 
                            ",[WMS_SUSPENDIDO] WMS_SUSPENSE  " + 
                            ",[WMS_TRANSITO] WMS_INTRANSIT  " + 
                            ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], [WMS_DISPONIBLE], [WMS_DISPONIBLE] - [WMS_TRANSITO]) WMS_ONHAND  " + 
                            ",[ERP_DISPONIBLE] ERP_ONHAND  " + 
                            ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], [DIFERENCIA_DISPONIBLE], [DIFERENCIA_DISPONIBLE] - [WMS_TRANSITO]) DIF_ONHAND  " + 
                            ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], ABS([DIFERENCIA_DISPONIBLE]), ABS([DIFERENCIA_DISPONIBLE] - [WMS_TRANSITO])) DIF_OH_ABS  " + 
                            "FROM [SCALEINTCOLOMBIAV2].[dbo].[INVENTARIO] (NOLOCK)  " + 
                            "WHERE CONVERT(DATE, FECHA) >= CONVERT(DATE,GETDATE())  " + 
                            "AND (WMS_DISPONIBLE + WMS_SUSPENDIDO + WMS_TRANSITO + ERP_DISPONIBLE) != 0  " + 
                            "AND ERP_ALMACEN IN ('CEN-PT')  " + 
                            "UNION ALL  " + 
                            "SELECT  " + 
                            "DATEADD(HH,3,FECHA) as DATE_TIME  " + 
                            ",[ITEM]  " + 
                            ",'CEN-OU - OUTLET' WAREHOUSE  " + 
                            ",[WMS_SUSPENDIDO] WMS_SUSPENSE  " + 
                            ",[WMS_TRANSITO] WMS_INTRANSIT  " + 
                            ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], [WMS_DISPONIBLE], [WMS_DISPONIBLE] - [WMS_TRANSITO]) WMS_ONHAND  " + 
                            ",[ERP_DISPONIBLE] ERP_ONHAND  " + 
                            ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], [DIFERENCIA_DISPONIBLE], [DIFERENCIA_DISPONIBLE] - [WMS_TRANSITO]) DIF_ONHAND  " + 
                            ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], ABS([DIFERENCIA_DISPONIBLE]), ABS([DIFERENCIA_DISPONIBLE] - [WMS_TRANSITO])) DIF_OH_ABS  " + 
                            "FROM [SCALEINTCOLOMBIAV2].[dbo].[INVENTARIO] (NOLOCK)  " + 
                            "WHERE CONVERT(DATE, FECHA) >= CONVERT(DATE,GETDATE())  " + 
                            "AND (WMS_DISPONIBLE + WMS_SUSPENDIDO + WMS_TRANSITO + ERP_DISPONIBLE) != 0  " + 
                            "AND ERP_ALMACEN IN ('CEN-OU')  " + 
                            "UNION ALL  " + 
                            "SELECT  " + 
                            "DATEADD(HH,3,FECHA) as DATE_TIME  " + 
                            ",[ITEM]  " + 
                            ",'CEN-MP - MERMA PROVEEDOR' WAREHOUSE  " + 
                            ",[WMS_SUSPENDIDO] WMS_SUSPENSE  " + 
                            ",[WMS_TRANSITO] WMS_INTRANSIT  " + 
                            ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], [WMS_DISPONIBLE], [WMS_DISPONIBLE] - [WMS_TRANSITO]) WMS_ONHAND  " + 
                            ",[ERP_DISPONIBLE] ERP_ONHAND  " + 
                            ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], [DIFERENCIA_DISPONIBLE], [DIFERENCIA_DISPONIBLE] - [WMS_TRANSITO]) DIF_ONHAND  " + 
                            ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], ABS([DIFERENCIA_DISPONIBLE]), ABS([DIFERENCIA_DISPONIBLE] - [WMS_TRANSITO])) DIF_OH_ABS  " + 
                            "FROM [SCALEINTCOLOMBIAV2].[dbo].[INVENTARIO] (NOLOCK)  " + 
                            "WHERE CONVERT(DATE, FECHA) >= CONVERT(DATE,GETDATE())  " + 
                            "AND (WMS_DISPONIBLE + WMS_SUSPENDIDO + WMS_TRANSITO + ERP_DISPONIBLE) != 0  " + 
                            "AND ERP_ALMACEN IN ('CEN-MP')  " + 
                            "UNION ALL  " + 
                            "SELECT  " + 
                            "DATEADD(HH,3,FECHA) as DATE_TIME  " + 
                            ",[ITEM]  " + 
                            ",'CEN-MA - MERMA ALMACEN' WAREHOUSE  " + 
                            ",[WMS_SUSPENDIDO] WMS_SUSPENSE  " + 
                            ",[WMS_TRANSITO] WMS_INTRANSIT  " + 
                            ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], [WMS_DISPONIBLE], [WMS_DISPONIBLE] - [WMS_TRANSITO]) WMS_ONHAND  " + 
                            ",[ERP_DISPONIBLE] ERP_ONHAND  " + 
                            ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], [DIFERENCIA_DISPONIBLE], [DIFERENCIA_DISPONIBLE] - [WMS_TRANSITO]) DIF_ONHAND  " + 
                            ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], ABS([DIFERENCIA_DISPONIBLE]), ABS([DIFERENCIA_DISPONIBLE] - [WMS_TRANSITO])) DIF_OH_ABS  " + 
                            "FROM [SCALEINTCOLOMBIAV2].[dbo].[INVENTARIO] (NOLOCK)  " + 
                            "WHERE CONVERT(DATE, FECHA) >= CONVERT(DATE,GETDATE())  " + 
                            "AND (WMS_DISPONIBLE + WMS_SUSPENDIDO + WMS_TRANSITO + ERP_DISPONIBLE) != 0  " + 
                            "AND ERP_ALMACEN IN ('CEN-MA')  " + 
                            "UNION ALL  " + 
                            "SELECT  " + 
                            "DATEADD(HH,3,FECHA) as DATE_TIME  " + 
                            ",[ITEM]  " + 
                            ",'CEN-DS - DESTRUCCION' WAREHOUSE  " + 
                            ",[WMS_SUSPENDIDO] WMS_SUSPENSE  " + 
                            ",[WMS_TRANSITO] WMS_INTRANSIT  " + 
                            ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], [WMS_DISPONIBLE], [WMS_DISPONIBLE] - [WMS_TRANSITO]) WMS_ONHAND  " + 
                            ",[ERP_DISPONIBLE] ERP_ONHAND  " + 
                            ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], [DIFERENCIA_DISPONIBLE], [DIFERENCIA_DISPONIBLE] - [WMS_TRANSITO]) DIF_ONHAND  " + 
                            ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], ABS([DIFERENCIA_DISPONIBLE]), ABS([DIFERENCIA_DISPONIBLE] - [WMS_TRANSITO])) DIF_OH_ABS  " + 
                            "FROM [SCALEINTCOLOMBIAV2].[dbo].[INVENTARIO] (NOLOCK)  " + 
                            "WHERE CONVERT(DATE, FECHA) >= CONVERT(DATE,GETDATE())  " + 
                            "AND (WMS_DISPONIBLE + WMS_SUSPENDIDO + WMS_TRANSITO + ERP_DISPONIBLE) != 0  " + 
                            "AND ERP_ALMACEN IN ('CEN-DS')  " + 
                            "UNION ALL  " + 
                            "SELECT  " + 
                            "DATEADD(HH,3,FECHA) as DATE_TIME  " + 
                            ",[ITEM]  " + 
                            ",'CEN-CU - CUARENTENA' WAREHOUSE  " + 
                            ",[WMS_SUSPENDIDO] WMS_SUSPENSE  " + 
                            ",[WMS_TRANSITO] WMS_INTRANSIT  " + 
                            ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], [WMS_DISPONIBLE], [WMS_DISPONIBLE] - [WMS_TRANSITO]) WMS_ONHAND  " + 
                            ",[ERP_DISPONIBLE] ERP_ONHAND  " + 
                            ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], [DIFERENCIA_DISPONIBLE], [DIFERENCIA_DISPONIBLE] - [WMS_TRANSITO]) DIF_ONHAND  " + 
                            ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], ABS([DIFERENCIA_DISPONIBLE]), ABS([DIFERENCIA_DISPONIBLE] - [WMS_TRANSITO])) DIF_OH_ABS  " + 
                            "FROM [SCALEINTCOLOMBIAV2].[dbo].[INVENTARIO] (NOLOCK)  " + 
                            "WHERE CONVERT(DATE, FECHA) >= CONVERT(DATE,GETDATE())  " + 
                            "AND (WMS_DISPONIBLE + WMS_SUSPENDIDO + WMS_TRANSITO + ERP_DISPONIBLE) != 0  " + 
                            "AND ERP_ALMACEN IN ('CEN-CU')) AS YY  " + 
                            "GROUP BY YY.WAREHOUSE  " + 
                            "ORDER BY ERP_ONHAND DESC ")
            registros=cursor.fetchall()
            for registro in registros:
                inventarioWmsErp=InventarioWmsErp(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8])
                comparativoWmsErpList.append(inventarioWmsErp)
            return comparativoWmsErpList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getItems(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            itemList=[]
            print('getItems Col')
            cursor.execute("SELECT   " +
                            "CASE   " +
                            "WHEN YY.[ERP_ALMACEN] = 'CEN-PT' THEN 'CEN-PT - PRODUCTO TERMINADO'  " +
                            "WHEN YY.[ERP_ALMACEN] = 'CEN-QA' THEN 'CEN-QA - CALIDAD'  " +
                            "WHEN YY.[ERP_ALMACEN] = 'CEN-OU' THEN 'CEN-OU - OUTLET'  " +
                            "WHEN YY.[ERP_ALMACEN] = 'CEN-MP' THEN 'CEN-MP - MERMA PROVEEDOR'  " +
                            "WHEN YY.[ERP_ALMACEN] = 'CEN-MA' THEN 'CEN-MA - MERMA ALMACEN'  " +
                            "WHEN YY.[ERP_ALMACEN] = 'CEN-DS' THEN 'CEN-DS - DESTRUCCION'  " +
                            "WHEN YY.[ERP_ALMACEN] = 'CEN-AC' THEN 'CEN-AC - AJUSTES DE INV'  " +
                            "WHEN YY.[ERP_ALMACEN] = 'CEN-FA' THEN 'CEN-FA - FALTANTE PROVEEDOR'  " +
                            "END  " +
                            "WAREHOUSE  " +
                            ",IIF(SUM(YY.[WMS_DISPONIBLE])=0 AND YY.[ERP_ALMACEN] = 'CEN-QA' OR YY.[ERP_ALMACEN] = 'CEN-FA' OR YY.[ERP_ALMACEN] = 'CEN-AC',  " +
                            "NULL, SUM(YY.[WMS_DISPONIBLE])) AS WMS_ONHAND  " +
                            ",SUM(YY.[ERP_DISPONIBLE]) ERP_ONHAND  " +
                            ", (SELECT COUNT (XX.[ITEM]) FROM [SCALEINTCOLOMBIAV2].[dbo].[INVENTARIO] XX (NOLOCK) WHERE CONVERT(DATE, XX.FECHA) = CONVERT(DATE,GETDATE()-1)  " +
                            "AND XX.[ERP_ALMACEN] = YY.[ERP_ALMACEN] AND (XX.ERP_DISPONIBLE) != 0) AS '#ITEMS'  " +
                            "FROM [SCALEINTCOLOMBIAV2].[dbo].[INVENTARIO] YY (NOLOCK)  " +
                            "WHERE CONVERT(DATE, YY.FECHA) = CONVERT(DATE,GETDATE())  " +
                            "AND (YY.WMS_DISPONIBLE + YY.WMS_SUSPENDIDO + YY.WMS_TRANSITO + YY.ERP_DISPONIBLE) != 0  " +
                            "AND YY.[ERP_ALMACEN] IN ('CEN-QA','CEN-FA','CEN-AC')  " +
                            "GROUP BY YY.[ERP_ALMACEN]  " +
                            "ORDER BY SUM(YY.ERP_DISPONIBLE) DESC ")
            registros=cursor.fetchall()
            for registro in registros:
                inventarioItem=InventarioItem(registro[0], registro[1], registro[2], registro[3])
                itemList.append(inventarioItem)
            return itemList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None: 
                self.closeConexion(conexion)
                    
#     def getInventarioWmsCol(self):
#         try:
#             conexion=self.getConexion()
#             cursor=conexion.cursor()
#             inventarioWmsList=[]
#             cursor.execute("SELECT  " +
#                             "ISNULL(NA.NOMBRE_ALMACEN, SI.WhsCode),  " +
#                             "Solicitado, OnHand, Comprometido, Disponible, SKU_SOL, SKU_OHD, SKU_CMP, CONVERT(date,DATEADD(DD,1,[Fecha Actualizacion]),103) AS 'Fecha Actualizacion'  " +
#                             "FROM SAP_INVCEDIS_COL SI (nolock)  " +
#                             "LEFT JOIN NOMBRE_ALMACEN NA ON NA.CLAVE_ALMACEN=SI.WhsCode  " +
#                             "ORDER BY OnHand DESC ")
#             registros=cursor.fetchall()
#             for registro in registros:
#                 inventarioWms=InventarioWms(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8])
# #                print(f"{registro[0]} {registro[1]} {registro[2]} {registro[3]} {registro[4]} {registro[5]} {registro[6]} {registro[7]} {registro[8]}")
#                 inventarioWmsList.append(inventarioWms)
#             return inventarioWmsList
#         except Exception as exception:
#             logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
#             raise exception
#         finally:
#             if conexion!= None:
#                 self.closeConexion(conexion)


    def getTopOneHundred(self, top):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            topOneHundredList=[]
            cadena=''
            if top:
                cadena= "top 800"
            cursor.execute("SELECT " +cadena +
                            " DATEADD(HH,3,FECHA) as DATE_TIME  " +
                            ",[ITEM]  " +
                            ",[ERP_ALMACEN] WAREHOUSE  " +
                            ",[WMS_SUSPENDIDO] WMS_SUSPENSE  " +
                            ",[WMS_TRANSITO] WMS_INTRANSIT  " +
                            ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], [WMS_DISPONIBLE], [WMS_DISPONIBLE] - [WMS_TRANSITO]) WMS_ONHAND  " +
                            ",[ERP_DISPONIBLE] ERP_ONHAND  " +
                            ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], [DIFERENCIA_DISPONIBLE], [DIFERENCIA_DISPONIBLE] - [WMS_TRANSITO]) DIF_ONHAND  " +
                            ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], ABS([DIFERENCIA_DISPONIBLE]), ABS([DIFERENCIA_DISPONIBLE] - [WMS_TRANSITO])) DIF_OH_ABS  " +
                            "FROM [SCALEINTCOLOMBIAV2].[dbo].[INVENTARIO] (NOLOCK)  " +
                            "WHERE CONVERT(DATE, FECHA) = CONVERT(DATE,GETDATE())  " +
                            "AND ERP_ALMACEN IN ('CEN-PT')  " +
                            "AND IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], ABS([DIFERENCIA_DISPONIBLE]), ABS([DIFERENCIA_DISPONIBLE] - [WMS_TRANSITO])) > 0  " +
                            "ORDER BY DIF_OH_ABS DESC ")
            registros=cursor.fetchall()
            for registro in registros:
                inventarioTop=InventarioTop(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8])
                topOneHundredList.append(inventarioTop)
            return topOneHundredList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
            
            
    def getInventarioDetalleErpWms(self, item):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            detalleErpWmsList=[]
            
            cursor.execute("SELECT " +
                            "DATEADD(HH,3,FECHA) as DATE_TIME  " +
                            ",[ITEM]  " +
                            ",[ERP_ALMACEN] WAREHOUSE  " +
                            ",IIF([WMS_SUSPENDIDO]=0 AND [ERP_ALMACEN] = 'CEN-QA' OR [ERP_ALMACEN] = 'CEN-FA' OR [ERP_ALMACEN] = 'CEN-AC',  " +
                            "NULL, [WMS_SUSPENDIDO]) AS WMS_SUSPENSE  " +
                            ",IIF([WMS_TRANSITO]=0 AND [ERP_ALMACEN] = 'CEN-QA' OR [ERP_ALMACEN] = 'CEN-FA' OR [ERP_ALMACEN] = 'CEN-AC',  " +
                            "NULL, [WMS_TRANSITO]) AS WMS_INTRANSIT  " +
                            ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE]   " +
                            ",IIF([WMS_DISPONIBLE]=0 AND [ERP_ALMACEN] = 'CEN-QA' OR [ERP_ALMACEN] = 'CEN-FA' OR [ERP_ALMACEN] = 'CEN-AC',  " +
                            "NULL, [WMS_DISPONIBLE])  " +
                            ",IIF([WMS_DISPONIBLE]=0 AND [ERP_ALMACEN] = 'CEN-QA' OR [ERP_ALMACEN] = 'CEN-FA' OR [ERP_ALMACEN] = 'CEN-AC',  " +
                            "NULL, [WMS_DISPONIBLE]) - [WMS_TRANSITO]) AS WMS_ONHAND  " +
                            ",[ERP_DISPONIBLE] ERP_ONHAND  " +
                            ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE]  " +
                            ",IIF([ERP_ALMACEN] = 'CEN-QA' OR [ERP_ALMACEN] = 'CEN-FA' OR [ERP_ALMACEN] = 'CEN-AC',  " +
                            "NULL, [DIFERENCIA_DISPONIBLE]),IIF([ERP_ALMACEN] = 'CEN-QA' OR [ERP_ALMACEN] = 'CEN-FA' OR [ERP_ALMACEN] = 'CEN-AC',  " +
                            "NULL, [DIFERENCIA_DISPONIBLE]) - [WMS_TRANSITO]) AS DIF_ONHAND  " +
                            ",ABS(IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE]  " +
                            ",IIF([ERP_ALMACEN] = 'CEN-QA' OR [ERP_ALMACEN] = 'CEN-FA' OR [ERP_ALMACEN] = 'CEN-AC',  " +
                            "NULL, ABS([DIFERENCIA_DISPONIBLE])),IIF([ERP_ALMACEN] = 'CEN-QA' OR [ERP_ALMACEN] = 'CEN-FA' OR [ERP_ALMACEN] = 'CEN-AC',  " +
                            "NULL, ABS([DIFERENCIA_DISPONIBLE])) - [WMS_TRANSITO])) AS DIF_OH_ABS  " +
                            "FROM [SCALEINTCOLOMBIAV2].[dbo].[INVENTARIO] (NOLOCK)  " +
                            "WHERE CONVERT(DATE, FECHA) = CONVERT(DATE,GETDATE())  " +
                            "AND (WMS_DISPONIBLE + WMS_SUSPENDIDO + WMS_TRANSITO + ERP_DISPONIBLE) != 0  " +
                            "AND ITEM like '%"+item+"%'  " +
                            "ORDER BY  " +
                            "ITEM ASC, ERP_DISPONIBLE DESC ")
            registros=cursor.fetchall()
            for registro in registros:
                inventarioDetalleErpWms=InventarioDetalleErpWms(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8])
                detalleErpWmsList.append(inventarioDetalleErpWms)
            return detalleErpWmsList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
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