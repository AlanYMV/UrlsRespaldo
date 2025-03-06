import logging
from hdbcli import dbapi
from sevicios_app.vo.articulo import Articulo
from sevicios_app.vo.detallePedido import DetallePedido
from sevicios_app.vo.codigoSat import CodigoSat
from sevicios_app.vo.storageTemplate import StorageTemplate
from sevicios_app.vo.precioCl import PrecioCl
from sevicios_app.vo.infoPedidoSinTr import InfoPedidoSinTr
from sevicios_app.vo.pedidoSapPlaneacion import PedidoSapPlaneacion

logger = logging.getLogger('')

class SAPDaoClII():

    def getConexion(self):
        try:
            conexion=dbapi.connect(address="192.168.84.45", port=30015, user="SYSTEM", password="Sy573Mmnso!!")
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
        
    def getStorageTemplatesCL(self, numRegistros):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            storagesTemplatesList=[]
            registros=''
            if numRegistros:
                registros='TOP '+numRegistros
            cursor.execute('SELECT '+registros+' T0."ItemCode", '+
                            'T1."UgpCode"  as "StorageTemplate", '+
                            'T0."U_SYS_GUML"  as "GrupoLogistico", '+
                            'T0."SalUnitMsr", '+
                            'T2."ItmsGrpNam" as "Familia", '+
                            'SF."Name" as "SubFamilia", '+
                            'SSF."Name" as "SubSubFamilia", '+
                            'T0."U_SYS_CAT4", '+
                            'T0."U_SYS_CAT5", '+
                            'T0."U_SYS_CAT6", '+
                            'T0."U_SYS_CAT7", '+
                            'T0."U_SYS_CAT8", '+
                            'T0."BHeight1"  as "Height", '+
                            'T0."BWidth1" as "Width", '+
                            'T0."BLength1" as "Length", '+
                            'T0."BVolume" as "Volume", '+
                            'T0."BWeight1"  as "Weight" '+
                            'FROM "SBO_MINISO_CHILE"."OITM"  T0 '+
                            'INNER JOIN "SBO_MINISO_CHILE"."OUGP"  T1 ON T0."UgpEntry" = T1."UgpEntry" '+
                            'INNER JOIN "SBO_MINISO_CHILE"."OITB" T2 ON T0."ItmsGrpCod" = T2."ItmsGrpCod" '+
                            'LEFT JOIN "SBO_MINISO_CHILE"."@SUBSUBFAMILIA" SSF ON T0."U_SUBSUBFAMILIA" = SSF."Code" '+
                            'LEFT JOIN "SBO_MINISO_CHILE"."@SUBFAMILIA" SF ON T0."U_SUBFAMILIA" = SF."Code"')
            registros=cursor.fetchall()
            for registro in registros:
                storageTemplate=StorageTemplate(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], 
                                  registro[11], registro[12], registro[13], registro[14], registro[15], registro[16])
                storagesTemplatesList.append(storageTemplate)
            return storagesTemplatesList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getPrecios(self, numRegistros):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            preciosList=[]
            registros=''
            if numRegistros:
                registros='TOP '+numRegistros
            cursor.execute('SELECT '+registros+' T0."ItemCode", '+
                            'C0."BcdCode" as "Codigo_Barras", '+
                            'T0."U_SYS_FAMI" as "Categoria", '+
                            'T0."U_SUBFAMILIA" as "Subcategoria", '+
                            'T0."U_SUBSUBFAMILIA" as "Clase", '+
                            'T0."ItemName", '+
                            'T6."UgpCode" as "Storage_Template", '+ 
                            'T0."U_SYS_GUML" as "ST_USR", '+                         
                            'T0."U_SYS_SLICE" as "Licencia", '+ 
                            'T0."BHeight1" as "Height", '+ 
                            'T0."BWidth1" as "Width", '+
                            'T0."BLength1" as "Length", '+
                            'T0."BVolume" as "Volume", '+
                            'T0."BWeight1" as "Weight", '+
                            'T1."Price" as "PRECIO_SIN_IVA", '+
                            'T2."Price" as "PRECIO_CON_IVA", '+
                            'T3."Price" as "PRECIO_EN_LINEA_SIN_IVA", '+
                            'T4."Price" as "PRECIO_EN_LINEA_CON_IVA", '+
                            'T7."CardName" as "Proveedor" '+
                            'FROM "SBO_MINISO_CHILE"."OITM" T0 '+
                            'LEFT JOIN "SBO_MINISO_CHILE"."ITM1" T1 ON T0."ItemCode" = T1."ItemCode" and T1."PriceList"=1 '+
                            'LEFT JOIN "SBO_MINISO_CHILE"."ITM1" T2 ON T0."ItemCode" = T2."ItemCode" and T2."PriceList"=2 '+
                            'LEFT JOIN "SBO_MINISO_CHILE"."ITM1" T3 ON T0."ItemCode" = T3."ItemCode" and T3."PriceList"=3 '+
                            'LEFT JOIN "SBO_MINISO_CHILE"."ITM1" T4 ON T0."ItemCode" = T4."ItemCode" and T4."PriceList"=4 '+
                            'LEFT JOIN "SBO_MINISO_CHILE"."OUGP"  T6 ON T0."UgpEntry" = T6."UgpEntry" '+
                            'LEFT JOIN "SBO_MINISO_CHILE"."OCRD" T7 ON T0."CardCode" = T7."CardCode" '+
                            'LEFT JOIN "SBO_MINISO_CHILE"."OBCD" C0 ON C0."ItemCode"=T0."ItemCode" '+
                            'where T0."SellItem" = \'Y\' AND T0."validFor" = \'Y\' '+ 
                            'order by T0."ItemCode"')
            registros=cursor.fetchall()
            itemCode=''
            precio=PrecioCl('','','','','','','','','','','','','','','','','','','')
            for registro in registros:
                if itemCode!=registro[0]:
                    precio=PrecioCl(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], 
                                registro[10], registro[11], registro[12], registro[13], registro[14], registro[15], registro[16], registro[17], registro[18])
                    preciosList.append(precio)
                    itemCode=registro[0]
                else:
                    precio.codigoBarras=precio.codigoBarras+', '+registro[1]
            return preciosList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
                
