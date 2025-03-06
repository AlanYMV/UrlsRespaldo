import logging
import pyodbc
from sevicios_app.vo.inventarioDetalleErpWms import InventarioDetalleErpWms
from sevicios_app.vo.inventarioItem import InventarioItem
from sevicios_app.vo.inventarioTop import InventarioTop
from sevicios_app.vo.inventarioWmsErp import InventarioWmsErp

logger = logging.getLogger('')

class ScaleIntChileDao():

    def getConexion(self):
        try:
            direccion_servidor = '192.168.84.107'
            nombre_bd = 'SCALEINTCHILE'
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
            cursor.execute(" SELECT     " +
                            " YY.WAREHOUSE     " +
                            " ,SUM(YY.WMS_ONHAND) AS WMS_ONHAND     " +
                            " ,SUM(YY.ERP_ONHAND) AS ERP_ONHAND     " +
                            " ,SUM(YY.DIF_ONHAND) AS DIFERENCIA     " +
                            " ,SUM(YY.DIF_OH_ABS) AS 'DIFERENCIA ABS'     " +
                            " ,SUM(YY.WMS_INTRANSIT) AS WMS_INTRANSIT     " +
                            " ,(SELECT COUNT (XX.ITEM) FROM INVENTARIO XX (NOLOCK) WHERE XX.ERP_ALMACEN = SUBSTRING(WAREHOUSE,1,8)      " +
                            " AND (XX.WMS_DISPONIBLE + XX.WMS_SUSPENDIDO + XX.WMS_TRANSITO) != 0 AND CONVERT(DATE, XX.FECHA) = CONVERT(DATE,GETDATE())) AS '#ITEMS_WMS'     " +
                            " ,(SELECT COUNT (XX.ITEM) FROM INVENTARIO XX (NOLOCK) WHERE XX.ERP_ALMACEN = SUBSTRING(WAREHOUSE,1,8)      " +
                            " AND (XX.ERP_DISPONIBLE) != 0 AND CONVERT(DATE, XX.FECHA) >= CONVERT(DATE,GETDATE())) AS '#ITEMS_ERP'     " +
                            " ,(SELECT COUNT (XX.ITEM) FROM INVENTARIO XX (NOLOCK) WHERE XX.ERP_ALMACEN = SUBSTRING(WAREHOUSE,1,8)      " +
                            " AND IIF(abs(XX.DIFERENCIA_DISPONIBLE)>0,(abs(XX.DIFERENCIA_DISPONIBLE) - XX.WMS_TRANSITO),abs(XX.DIFERENCIA_DISPONIBLE)) != 0      " +
                            " AND CONVERT(DATE, XX.FECHA) >= CONVERT(DATE,GETDATE())) AS '#ITEMS_DIF'     " +
                            " FROM (     " +
                            " SELECT     " +
                            " DATEADD(HH,3,INV.FECHA) as DATE_TIME     " +
                            " ,INV.[ITEM]     " +
                            " ,'CL001-PT - PRODUCTO TERMINADO' WAREHOUSE     " +
                            " ,INV.[WMS_SUSPENDIDO] WMS_SUSPENSE     " +
                            " ,INV.[WMS_TRANSITO] WMS_INTRANSIT     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], INV.[WMS_DISPONIBLE], INV.[WMS_DISPONIBLE] - INV.[WMS_TRANSITO]) WMS_ONHAND     " +
                            " ,INV.[ERP_DISPONIBLE] ERP_ONHAND     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], INV.[DIFERENCIA_DISPONIBLE], INV.[DIFERENCIA_DISPONIBLE] - INV.[WMS_TRANSITO]) DIF_ONHAND     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], ABS(INV.[DIFERENCIA_DISPONIBLE]), ABS(INV.[DIFERENCIA_DISPONIBLE] - INV.[WMS_TRANSITO])) DIF_OH_ABS     " +
                            " FROM [SCALEINTCHILE].[dbo].[INVENTARIO] (NOLOCK) INV  " +
                            "   " +
                            " WHERE CONVERT(DATE, INV.FECHA) >= CONVERT(DATE,GETDATE())     " +
                            " AND (INV.WMS_DISPONIBLE + INV.WMS_SUSPENDIDO + INV.WMS_TRANSITO + INV.ERP_DISPONIBLE) != 0     " +
                            " AND INV.ERP_ALMACEN IN ('CL001-PT')     " +
                            " UNION ALL     " +
                            " SELECT     " +
                            " DATEADD(HH,3,INV.FECHA) as DATE_TIME     " +
                            " ,INV.[ITEM]     " +
                            " ,'CL001-OU - OUTLET' WAREHOUSE     " +
                            " ,INV.[WMS_SUSPENDIDO] WMS_SUSPENSE     " +
                            " ,INV.[WMS_TRANSITO] WMS_INTRANSIT     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], INV.[WMS_DISPONIBLE], INV.[WMS_DISPONIBLE] - INV.[WMS_TRANSITO]) WMS_ONHAND     " +
                            " ,INV.[ERP_DISPONIBLE] ERP_ONHAND     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], INV.[DIFERENCIA_DISPONIBLE], INV.[DIFERENCIA_DISPONIBLE] - INV.[WMS_TRANSITO]) DIF_ONHAND     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], ABS(INV.[DIFERENCIA_DISPONIBLE]), ABS(INV.[DIFERENCIA_DISPONIBLE] - INV.[WMS_TRANSITO])) DIF_OH_ABS     " +
                            " FROM [SCALEINTCHILE].[dbo].[INVENTARIO] (NOLOCK)  INV  " +
                            "   " +
                            " WHERE CONVERT(DATE, INV.FECHA) >= CONVERT(DATE,GETDATE())     " +
                            " AND (INV.WMS_DISPONIBLE + INV.WMS_SUSPENDIDO + INV.WMS_TRANSITO + INV.ERP_DISPONIBLE) != 0     " +
                            " AND INV.ERP_ALMACEN IN ('CL001-OU')     " +
                            " UNION ALL     " +
                            " SELECT     " +
                            " DATEADD(HH,3,INV.FECHA) as DATE_TIME     " +
                            " ,INV.[ITEM]     " +
                            " ,'CL001-MP - MERMA PROVEEDOR' WAREHOUSE     " +
                            " ,INV.[WMS_SUSPENDIDO] WMS_SUSPENSE     " +
                            " ,INV.[WMS_TRANSITO] WMS_INTRANSIT     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], INV.[WMS_DISPONIBLE], INV.[WMS_DISPONIBLE] - INV.[WMS_TRANSITO]) WMS_ONHAND     " +
                            " ,INV.[ERP_DISPONIBLE] ERP_ONHAND     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], INV.[DIFERENCIA_DISPONIBLE], INV.[DIFERENCIA_DISPONIBLE] - INV.[WMS_TRANSITO]) DIF_ONHAND     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], ABS(INV.[DIFERENCIA_DISPONIBLE]), ABS(INV.[DIFERENCIA_DISPONIBLE] - INV.[WMS_TRANSITO])) DIF_OH_ABS     " +
                            " FROM [SCALEINTCHILE].[dbo].[INVENTARIO] (NOLOCK) INV  " +
                            "   " +
                            " WHERE CONVERT(DATE, INV.FECHA) >= CONVERT(DATE,GETDATE())     " +
                            " AND (INV.WMS_DISPONIBLE + INV.WMS_SUSPENDIDO + INV.WMS_TRANSITO + INV.ERP_DISPONIBLE) != 0     " +
                            " AND INV.ERP_ALMACEN IN ('CL001-MP')     " +
                            " UNION ALL     " +
                            " SELECT     " +
                            " DATEADD(HH,3,INV.FECHA) as DATE_TIME     " +
                            " ,INV.[ITEM]     " +
                            " ,'CL001-MA - MERMA ALMACEN' WAREHOUSE     " +
                            " ,INV.[WMS_SUSPENDIDO] WMS_SUSPENSE     " +
                            " ,INV.[WMS_TRANSITO] WMS_INTRANSIT     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], INV.[WMS_DISPONIBLE], INV.[WMS_DISPONIBLE] - INV.[WMS_TRANSITO]) WMS_ONHAND     " +
                            " ,INV.[ERP_DISPONIBLE] ERP_ONHAND     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], INV.[DIFERENCIA_DISPONIBLE], INV.[DIFERENCIA_DISPONIBLE] - INV.[WMS_TRANSITO]) DIF_ONHAND     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], ABS(INV.[DIFERENCIA_DISPONIBLE]), ABS(INV.[DIFERENCIA_DISPONIBLE] - INV.[WMS_TRANSITO])) DIF_OH_ABS      " +
                            " FROM [SCALEINTCHILE].[dbo].[INVENTARIO] (NOLOCK)    INV  " +
                            "   " +
                            " WHERE CONVERT(DATE, INV.FECHA) >= CONVERT(DATE,GETDATE())     " +
                            " AND (INV.WMS_DISPONIBLE + INV.WMS_SUSPENDIDO + INV.WMS_TRANSITO + INV.ERP_DISPONIBLE) != 0     " +
                            " AND INV.ERP_ALMACEN IN ('CL001-MA')     " +
                            " UNION ALL     " +
                            " SELECT     " +
                            " DATEADD(HH,3,INV.FECHA) as DATE_TIME     " +
                            " ,INV.ITEM     " +
                            " ,'CL001-DS - DESTRUCCION' WAREHOUSE     " +
                            " ,INV.[WMS_SUSPENDIDO] WMS_SUSPENSE     " +
                            " ,INV.[WMS_TRANSITO] WMS_INTRANSIT     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], INV.[WMS_DISPONIBLE], INV.[WMS_DISPONIBLE] - INV.[WMS_TRANSITO]) WMS_ONHAND     " +
                            " ,INV.[ERP_DISPONIBLE] ERP_ONHAND     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], INV.[DIFERENCIA_DISPONIBLE], INV.[DIFERENCIA_DISPONIBLE] - INV.[WMS_TRANSITO]) DIF_ONHAND     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], ABS(INV.[DIFERENCIA_DISPONIBLE]), ABS(INV.[DIFERENCIA_DISPONIBLE] - INV.[WMS_TRANSITO])) DIF_OH_ABS     " +
                            " FROM [SCALEINTCHILE].[dbo].[INVENTARIO] (NOLOCK) INV  " +
                            "   " +
                            " WHERE CONVERT(DATE, INV.FECHA) >= CONVERT(DATE,GETDATE())     " +
                            " AND (INV.WMS_DISPONIBLE + INV.WMS_SUSPENDIDO + INV.WMS_TRANSITO + INV.ERP_DISPONIBLE) != 0     " +
                            " AND INV.ERP_ALMACEN IN ('CL001-DS')     " +
                            " UNION ALL     " +
                            " SELECT     " +
                            " DATEADD(HH,3,INV.FECHA) as DATE_TIME     " +
                            " ,INV.[ITEM]     " +
                            " ,'CL001-CR - CUARENTENA' WAREHOUSE     " +
                            " ,INV.[WMS_SUSPENDIDO] WMS_SUSPENSE     " +
                            " ,INV.[WMS_TRANSITO] WMS_INTRANSIT     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], INV.[WMS_DISPONIBLE], INV.[WMS_DISPONIBLE] - INV.[WMS_TRANSITO]) WMS_ONHAND     " +
                            " ,INV.[ERP_DISPONIBLE] ERP_ONHAND     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], INV.[DIFERENCIA_DISPONIBLE], INV.[DIFERENCIA_DISPONIBLE] - INV.[WMS_TRANSITO]) DIF_ONHAND     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], ABS(INV.[DIFERENCIA_DISPONIBLE]), ABS(INV.[DIFERENCIA_DISPONIBLE] - INV.[WMS_TRANSITO])) DIF_OH_ABS     " +
                            " FROM [SCALEINTCHILE].[dbo].[INVENTARIO] (NOLOCK)    INV  " +
                            "   " +
                            " WHERE CONVERT(DATE, INV.FECHA) >= CONVERT(DATE,GETDATE())     " +
                            " AND (INV.WMS_DISPONIBLE + INV.WMS_SUSPENDIDO + INV.WMS_TRANSITO + INV.ERP_DISPONIBLE) != 0     " +
                            " AND INV.ERP_ALMACEN IN ('CL001-CR')     " +
                            " UNION ALL     " +
                            " SELECT     " +
                            " DATEADD(HH,3,INV.FECHA) as DATE_TIME     " +
                            " ,INV.[ITEM]     " +
                            " ,'CL001-AC - ACONDICIONADO' WAREHOUSE     " +
                            " ,INV.[WMS_SUSPENDIDO] WMS_SUSPENSE     " +
                            " ,INV.[WMS_TRANSITO] WMS_INTRANSIT     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], INV.[WMS_DISPONIBLE], INV.[WMS_DISPONIBLE] - INV.[WMS_TRANSITO]) WMS_ONHAND     " +
                            " ,INV.[ERP_DISPONIBLE] ERP_ONHAND     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], INV.[DIFERENCIA_DISPONIBLE], INV.[DIFERENCIA_DISPONIBLE] - INV.[WMS_TRANSITO]) DIF_ONHAND     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], ABS(INV.[DIFERENCIA_DISPONIBLE]), ABS(INV.[DIFERENCIA_DISPONIBLE] - INV.[WMS_TRANSITO])) DIF_OH_ABS      " +
                            " FROM [SCALEINTCHILE].[dbo].[INVENTARIO] (NOLOCK)    INV  " +
                            "   " +
                            " WHERE CONVERT(DATE, INV.FECHA) >= CONVERT(DATE,GETDATE())     " +
                            " AND (INV.WMS_DISPONIBLE + INV.WMS_SUSPENDIDO + INV.WMS_TRANSITO + INV.ERP_DISPONIBLE) != 0     " +
                            " AND INV.ERP_ALMACEN IN ('CL001-AC')     " +
                            " UNION ALL     " +
                            " SELECT     " +
                            " DATEADD(HH,3,INV.FECHA) as DATE_TIME     " +
                            " ,INV.[ITEM]     " +
                            " ,'CL001-S2 - SPARTA RENTA' WAREHOUSE     " +
                            " ,INV.[WMS_SUSPENDIDO] WMS_SUSPENSE     " +
                            " ,INV.[WMS_TRANSITO] WMS_INTRANSIT     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], INV.[WMS_DISPONIBLE], INV.[WMS_DISPONIBLE] - INV.[WMS_TRANSITO]) WMS_ONHAND     " +
                            " ,INV.[ERP_DISPONIBLE] ERP_ONHAND     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], INV.[DIFERENCIA_DISPONIBLE], INV.[DIFERENCIA_DISPONIBLE] - INV.[WMS_TRANSITO]) DIF_ONHAND     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], ABS(INV.[DIFERENCIA_DISPONIBLE]), ABS(INV.[DIFERENCIA_DISPONIBLE] - INV.[WMS_TRANSITO])) DIF_OH_ABS     " +
                            " FROM [SCALEINTCHILE].[dbo].[INVENTARIO] (NOLOCK)    INV  " +
                            "   " +
                            " WHERE CONVERT(DATE, INV.FECHA) >= CONVERT(DATE,GETDATE())     " +
                            " AND (INV.WMS_DISPONIBLE + INV.WMS_SUSPENDIDO + INV.WMS_TRANSITO + INV.ERP_DISPONIBLE) != 0     " +
                            " AND INV.ERP_ALMACEN IN ('CL001-S2')     " +
                            " UNION ALL     " +
                            " SELECT     " +
                            " DATEADD(HH,3,INV.FECHA) as DATE_TIME     " +
                            " ,INV.[ITEM]     " +
                            " ,'CL001-P1 - PROVEEDOR LOGISFASHION' WAREHOUSE     " +
                            " ,INV.[WMS_SUSPENDIDO] WMS_SUSPENSE     " +
                            " ,INV.[WMS_TRANSITO] WMS_INTRANSIT     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], INV.[WMS_DISPONIBLE], INV.[WMS_DISPONIBLE] - INV.[WMS_TRANSITO]) WMS_ONHAND     " +
                            " ,INV.[ERP_DISPONIBLE] ERP_ONHAND     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], INV.[DIFERENCIA_DISPONIBLE], INV.[DIFERENCIA_DISPONIBLE] - INV.[WMS_TRANSITO]) DIF_ONHAND     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], ABS(INV.[DIFERENCIA_DISPONIBLE]), ABS(INV.[DIFERENCIA_DISPONIBLE] - INV.[WMS_TRANSITO])) DIF_OH_ABS     " +
                            " FROM [SCALEINTCHILE].[dbo].[INVENTARIO] (NOLOCK)    INV  " +
                            "   " +
                            " WHERE CONVERT(DATE, INV.FECHA) >= CONVERT(DATE,GETDATE())     " +
                            " AND (INV.WMS_DISPONIBLE + INV.WMS_SUSPENDIDO + INV.WMS_TRANSITO + INV.ERP_DISPONIBLE) != 0     " +
                            " AND INV.ERP_ALMACEN IN ('CL001-P1')     " +
                            " UNION ALL     " +
                            " SELECT     " +
                            " DATEADD(HH,3,INV.FECHA) as DATE_TIME     " +
                            " ,INV.[ITEM]     " +
                            " ,'CL001-P2 - PROVEEDOR FASHION TRANSPORT' WAREHOUSE     " +
                            " ,INV.[WMS_SUSPENDIDO] WMS_SUSPENSE     " +
                            " ,INV.[WMS_TRANSITO] WMS_INTRANSIT     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], INV.[WMS_DISPONIBLE], INV.[WMS_DISPONIBLE] - INV.[WMS_TRANSITO]) WMS_ONHAND     " +
                            " ,INV.[ERP_DISPONIBLE] ERP_ONHAND     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], INV.[DIFERENCIA_DISPONIBLE], INV.[DIFERENCIA_DISPONIBLE] - INV.[WMS_TRANSITO]) DIF_ONHAND     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], ABS(INV.[DIFERENCIA_DISPONIBLE]), ABS(INV.[DIFERENCIA_DISPONIBLE] - INV.[WMS_TRANSITO])) DIF_OH_ABS      " +
                            " FROM [SCALEINTCHILE].[dbo].[INVENTARIO] (NOLOCK)    INV  " +
                            "   " +
                            " WHERE CONVERT(DATE, INV.FECHA) >= CONVERT(DATE,GETDATE())     " +
                            " AND (INV.WMS_DISPONIBLE + INV.WMS_SUSPENDIDO + INV.WMS_TRANSITO + INV.ERP_DISPONIBLE) != 0     " +
                            " AND INV.ERP_ALMACEN IN ('CL001-P2')     " +
                            " UNION ALL     " +
                            " SELECT     " +
                            " DATEADD(HH,3,INV.FECHA) as DATE_TIME     " +
                            " ,INV.[ITEM]     " +
                            " ,'CL001-S1 - COSMETICOS ENEA' WAREHOUSE     " +
                            " ,INV.[WMS_SUSPENDIDO] WMS_SUSPENSE     " +
                            " ,INV.[WMS_TRANSITO] WMS_INTRANSIT     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], INV.[WMS_DISPONIBLE], INV.[WMS_DISPONIBLE] - INV.[WMS_TRANSITO]) WMS_ONHAND     " +
                            " ,INV.[ERP_DISPONIBLE] ERP_ONHAND     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], INV.[DIFERENCIA_DISPONIBLE], INV.[DIFERENCIA_DISPONIBLE] - INV.[WMS_TRANSITO]) DIF_ONHAND     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], ABS(INV.[DIFERENCIA_DISPONIBLE]), ABS(INV.[DIFERENCIA_DISPONIBLE] - INV.[WMS_TRANSITO])) DIF_OH_ABS       " +
                            " FROM [SCALEINTCHILE].[dbo].[INVENTARIO] (NOLOCK)    INV  " +
                            "   " +
                            " WHERE CONVERT(DATE, INV.FECHA) >= CONVERT(DATE,GETDATE())     " +
                            " AND (INV.WMS_DISPONIBLE + INV.WMS_SUSPENDIDO + INV.WMS_TRANSITO + INV.ERP_DISPONIBLE) != 0     " +
                            " AND INV.ERP_ALMACEN IN ('CL001-S1')     " +
                            " UNION ALL     " +
                            " SELECT     " +
                            " DATEADD(HH,3,INV.FECHA) as DATE_TIME     " +
                            " ,INV.[ITEM]     " +
                            " ,'CL001-S3 - SUPERMERCADO' WAREHOUSE     " +
                            " ,INV.[WMS_SUSPENDIDO] WMS_SUSPENSE     " +
                            " ,INV.[WMS_TRANSITO] WMS_INTRANSIT     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], INV.[WMS_DISPONIBLE], INV.[WMS_DISPONIBLE] - INV.[WMS_TRANSITO]) WMS_ONHAND     " +
                            " ,INV.[ERP_DISPONIBLE] ERP_ONHAND     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], INV.[DIFERENCIA_DISPONIBLE], INV.[DIFERENCIA_DISPONIBLE] - INV.[WMS_TRANSITO]) DIF_ONHAND     " +
                            " ,IIF(INV.[WMS_DISPONIBLE]=INV.[ERP_DISPONIBLE], ABS(INV.[DIFERENCIA_DISPONIBLE]), ABS(INV.[DIFERENCIA_DISPONIBLE] - INV.[WMS_TRANSITO])) DIF_OH_ABS     " +
                            " FROM [SCALEINTCHILE].[dbo].[INVENTARIO] (NOLOCK)    INV  " +
                            "   " +
                            " WHERE CONVERT(DATE, INV.FECHA) >= CONVERT(DATE,GETDATE())     " +
                            " AND (INV.WMS_DISPONIBLE + INV.WMS_SUSPENDIDO + INV.WMS_TRANSITO + INV.ERP_DISPONIBLE) != 0     " +
                            " AND INV.ERP_ALMACEN IN ('CL001-S3')     " +
                            " ) AS YY     " +
                            " WHERE (SELECT IT.ITEM_CATEGORY1 FROM [192.168.84.34].[ILS].[dbo].[ITEM] IT WHERE YY.ITEM COLLATE Latin1_General_CI_AS = IT.ITEM COLLATE Latin1_General_CI_AS ) != 'BOLSAS' " +
                            " GROUP BY YY.WAREHOUSE     " +
                            " ORDER BY ERP_ONHAND DESC " )
            registros=cursor.fetchall()
            for registro in registros:
                inventarioWmsErp=InventarioWmsErp(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8])
                comparativoWmsErpList.append(inventarioWmsErp)
#                print(f"{registro[0]} {registro[1]} {registro[2]} {registro[3]} {registro[4]} {registro[5]} {registro[6]} {registro[7]} {registro[8]}")
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
            print('getItems')
            cursor.execute("SELECT "+
                           "CASE  "+
                           "WHEN YY.[ERP_ALMACEN] = 'CL001-AA' THEN 'CL001-AA - ACLARACIONES O AUDITORIA' "+
                           "WHEN YY.[ERP_ALMACEN] = 'CL001-AC' THEN 'CL001-AC - ACONDICIONADO' "+
                           "WHEN YY.[ERP_ALMACEN] = 'CL001-CR' THEN 'CL001-CR - CUARENTENA' "+
                           "WHEN YY.[ERP_ALMACEN] = 'CL001-CT' THEN 'CL001-CT - COSTEO' "+
                           "WHEN YY.[ERP_ALMACEN] = 'CL001-DF' THEN 'CL001-DF - DIFERENCIAS EN TIENDA' "+
                           "WHEN YY.[ERP_ALMACEN] = 'CL001-DS' THEN 'CL001-DS - DESTRUCCION' "+
                           "WHEN YY.[ERP_ALMACEN] = 'CL001-FA' THEN 'CL001-FA - FALTANTE PROVEEDOR' "+
                           "WHEN YY.[ERP_ALMACEN] = 'CL001-IR' THEN 'CL001-IR - INVENTARIO DE REMATE' "+
                           "WHEN YY.[ERP_ALMACEN] = 'CL001-LI' THEN 'CL001-LI - LOGISTICA INVERSA' "+
                           "WHEN YY.[ERP_ALMACEN] = 'CL001-S2' THEN 'CL001-S2 - SPARTA RENTA' "+
                           "WHEN YY.[ERP_ALMACEN] = 'CL001-MA' THEN 'CL001-MA - MERMA ALMACEN' "+
                           "WHEN YY.[ERP_ALMACEN] = 'CL001-MP' THEN 'CL001-MP - MERMA PROVEEDOR' "+
                           "WHEN YY.[ERP_ALMACEN] = 'CL001-OU' THEN 'CL001-OU - OUTLET' "+
                           "WHEN YY.[ERP_ALMACEN] = 'CL001-P1' THEN 'CL001-P1 - PROVEEDOR LOGISFASHION' "+
                           "WHEN YY.[ERP_ALMACEN] = 'CL001-P2' THEN 'CL001-P2 - PROVEEDOR FASHION TRANSPORT' "+
                           "WHEN YY.[ERP_ALMACEN] = 'CL001-PT' THEN 'CL001-PT - PRODUCTO TERMINADO' "+
                           "WHEN YY.[ERP_ALMACEN] = 'CL001-QA' THEN 'CL001-QA - CALIDAD Y LOGÍSTICA INVERSA' "+
                           "WHEN YY.[ERP_ALMACEN] = 'CL001-RO' THEN 'CL001-RO - ROBO' "+
                           "WHEN YY.[ERP_ALMACEN] = 'CL001-TR' THEN 'CL001-TR - TRANSITO COMODIN' "+
                           "WHEN YY.[ERP_ALMACEN] = 'CL001-S1' THEN 'CL001-S1 - COSMETICOS ENEA' "+
                           "WHEN YY.[ERP_ALMACEN] = 'CL001-S3' THEN 'CL001-S3 - SUPERMERCADO' "+
                           "WHEN YY.[ERP_ALMACEN] = 'CL001-AL' THEN 'CL001-AL - ACLARATORIA LOGISFASHION' "+
                           "ELSE YY.[ERP_ALMACEN] "+
                           "END "+
                           "WAREHOUSE "+
                           ",IIF(SUM(YY.[WMS_DISPONIBLE])=0 AND YY.[ERP_ALMACEN] = 'CL001-QA' OR YY.[ERP_ALMACEN] = 'CL001-FA' OR YY.[ERP_ALMACEN] = 'CL001-AA', "+
                           "NULL, SUM(YY.[WMS_DISPONIBLE])) AS WMS_ONHAND "+
                           ",SUM(YY.[ERP_DISPONIBLE]) ERP_ONHAND "+
                           ", (SELECT COUNT (XX.[ITEM]) FROM [SCALEINTCHILE].[dbo].[INVENTARIO] XX (NOLOCK) WHERE CONVERT(DATE, XX.FECHA) = CONVERT(DATE,GETDATE()-1) "+
                           "AND XX.[ERP_ALMACEN] = YY.[ERP_ALMACEN] AND (XX.ERP_DISPONIBLE) != 0) AS '#ITEMS' "+
                           "FROM [SCALEINTCHILE].[dbo].[INVENTARIO] YY (NOLOCK) "+
                           "WHERE "+ #CONVERT(DATE, YY.FECHA) = CONVERT(DATE,GETDATE()) "+
                           "(YY.WMS_DISPONIBLE + YY.WMS_SUSPENDIDO + YY.WMS_TRANSITO + YY.ERP_DISPONIBLE) != 0 "+
                           "AND	ISNULL(YY.[ERP_ALMACEN],'')<> '' "+
                           "AND YY.[ERP_ALMACEN] IN ('CL001-QA','CL001-FA','CL001-AA') "+
                           "GROUP BY YY.[ERP_ALMACEN] "+
                           "ORDER BY SUM(YY.ERP_DISPONIBLE) DESC")
            registros=cursor.fetchall()
            for registro in registros:
                inventarioItem=InventarioItem(registro[0], registro[1], registro[2], registro[3])
                itemList.append(inventarioItem)
#                print(f"{registro[0]} {registro[1]} {registro[2]}")
            return itemList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None: 
                self.closeConexion(conexion)


    def getTopOneHundred(self, top):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            limite=""
            if top== True:
                limite="TOP 100 "
            topOneHundredList=[]
            cursor.execute("SELECT "+limite+
                           "DATEADD(HH,3,FECHA) as DATE_TIME "+
                           ",[ITEM] "+
	                       ",[ERP_ALMACEN] WAREHOUSE "+
                           ",[WMS_SUSPENDIDO] WMS_SUSPENSE "+
                           ",[WMS_TRANSITO] WMS_INTRANSIT "+
	                       ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], [WMS_DISPONIBLE], [WMS_DISPONIBLE] - [WMS_TRANSITO]) WMS_ONHAND "+
                           ",[ERP_DISPONIBLE] ERP_ONHAND "+
                           ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], [DIFERENCIA_DISPONIBLE], [DIFERENCIA_DISPONIBLE] - [WMS_TRANSITO]) DIF_ONHAND "+
	                       ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], ABS([DIFERENCIA_DISPONIBLE]), ABS([DIFERENCIA_DISPONIBLE] - [WMS_TRANSITO])) DIF_OH_ABS "+
                           "FROM [SCALEINTCHILE].[dbo].[INVENTARIO] (NOLOCK) "+
                           "WHERE CONVERT(DATE, FECHA) = CONVERT(DATE,GETDATE()) "+
                           "AND ERP_ALMACEN IN ('CL001-PT') "+
                        #    "AND IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE], ABS([DIFERENCIA_DISPONIBLE]), ABS([DIFERENCIA_DISPONIBLE] - [WMS_TRANSITO])) > 0 "+
                            "AND IIF(abs(DIFERENCIA_DISPONIBLE)>0,(abs(DIFERENCIA_DISPONIBLE) - WMS_TRANSITO),abs(DIFERENCIA_DISPONIBLE)) != 0  " +
                           "ORDER BY DIF_OH_ABS DESC")
            registros=cursor.fetchall()
            for registro in registros:
                inventarioTop=InventarioTop(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8])
                topOneHundredList.append(inventarioTop)
#                print(f"{registro[0]} {registro[1]} {registro[2]} {registro[3]} {registro[4]} {registro[5]} {registro[6]} {registro[7]} {registro[8]}")
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
            
            cursor.execute("SELECT "+
                           "DATEADD(HH,3,FECHA) as DATE_TIME "+
                           ",[ITEM] "+
	                       ",[ERP_ALMACEN] WAREHOUSE "+
                           ",IIF([WMS_SUSPENDIDO]=0 AND [ERP_ALMACEN] = 'CL001-QA' OR [ERP_ALMACEN] = 'CL001-FA' OR [ERP_ALMACEN] = 'CL001-AA', "+
	                       "NULL, [WMS_SUSPENDIDO]) AS WMS_SUSPENSE "+
                           ",IIF([WMS_TRANSITO]=0 AND [ERP_ALMACEN] = 'CL001-QA' OR [ERP_ALMACEN] = 'CL001-FA' OR [ERP_ALMACEN] = 'CL001-AA', "+
	                       "NULL, [WMS_TRANSITO]) AS WMS_INTRANSIT "+
	                       ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE]  "+
	                       ",IIF([WMS_DISPONIBLE]=0 AND [ERP_ALMACEN] = 'CL001-QA' OR [ERP_ALMACEN] = 'CL001-FA' OR [ERP_ALMACEN] = 'CL001-AA', "+
	                       "NULL, [WMS_DISPONIBLE]) "+
	                       ",IIF([WMS_DISPONIBLE]=0 AND [ERP_ALMACEN] = 'CL001-QA' OR [ERP_ALMACEN] = 'CL001-FA' OR [ERP_ALMACEN] = 'CL001-AA', "+
	                       "NULL, [WMS_DISPONIBLE]) - [WMS_TRANSITO]) AS WMS_ONHAND "+
                           ",[ERP_DISPONIBLE] ERP_ONHAND "+
                           ",IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE] "+
	                       ",IIF([ERP_ALMACEN] = 'CL001-QA' OR [ERP_ALMACEN] = 'CL001-FA' OR [ERP_ALMACEN] = 'CL001-AA', "+
	                       "NULL, [DIFERENCIA_DISPONIBLE]),IIF([ERP_ALMACEN] = 'CL001-QA' OR [ERP_ALMACEN] = 'CL001-FA' OR [ERP_ALMACEN] = 'CL001-AA', "+
	                       "NULL, [DIFERENCIA_DISPONIBLE]) - [WMS_TRANSITO]) AS DIF_ONHAND "+
                           ",ABS(IIF([WMS_DISPONIBLE]=[ERP_DISPONIBLE] "+
	                       ",IIF([ERP_ALMACEN] = 'CL001-QA' OR [ERP_ALMACEN] = 'CL001-FA' OR [ERP_ALMACEN] = 'CL001-AA', "+
	                       "NULL, ABS([DIFERENCIA_DISPONIBLE])),IIF([ERP_ALMACEN] = 'CL001-QA' OR [ERP_ALMACEN] = 'CL001-FA' OR [ERP_ALMACEN] = 'CL001-AA', "+
	                       "NULL, ABS([DIFERENCIA_DISPONIBLE])) - [WMS_TRANSITO])) AS DIF_OH_ABS "+
                           "FROM [SCALEINTCHILE].[dbo].[INVENTARIO] (NOLOCK) "+
                           "WHERE CONVERT(DATE, FECHA) = CONVERT(DATE,GETDATE()) "+
                           "AND (WMS_DISPONIBLE + WMS_SUSPENDIDO + WMS_TRANSITO + ERP_DISPONIBLE) != 0 "+
                           "AND ITEM like '%"+item+"%' "+
                           "ORDER BY "+
                           "ITEM ASC, ERP_DISPONIBLE DESC")
            registros=cursor.fetchall()
            for registro in registros:
                inventarioDetalleErpWms=InventarioDetalleErpWms(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8])
                detalleErpWmsList.append(inventarioDetalleErpWms)
#                print(f"{registro[0]} {registro[1]} {registro[2]} {registro[3]} {registro[4]} {registro[5]} {registro[6]} {registro[7]} {registro[8]}")
            return detalleErpWmsList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)