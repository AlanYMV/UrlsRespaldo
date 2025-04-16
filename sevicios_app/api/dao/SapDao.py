import logging
import pyodbc
from sevicios_app.vo.articulo import Articulo
from sevicios_app.vo.detallePedido import DetallePedido
from sevicios_app.vo.codigoSat import CodigoSat
from sevicios_app.vo.storageTemplate import StorageTemplate
from sevicios_app.vo.precio import Precio
from sevicios_app.vo.infoPedidoSinTr import InfoPedidoSinTr
from sevicios_app.vo.pedidoSapPlaneacion import PedidoSapPlaneacion
from sevicios_app.vo.stringOne import StringOne 

logger = logging.getLogger(__name__) #New __name__

class SAPDao():

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
        
    def getArticulos(self, numRegistros):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            articulosList=[]
            registros=''
            if numRegistros:
                registros='TOP '+numRegistros
            cursor.execute('SELECT '+registros+' Tx.SKU, '+
	                        'Tx.DescripcionSKU, '+
                            'Tx.CodigoSAT, '+
                            'Tx.CodigoProveedor, '+
                            'Tx.Proveedor, '+
                            'Tx.UnidadMedidaCompra, '+
                            'Tx.ClaveUnidad, '+
                            'Tx.PesoArticulo, '+
                            'Tx.ItemsUnidadCompra, '+
                            'Tx.CantidadPaquete '+
                        'FROM OPENQUERY '+
                        '([HANADB_SAP_COMX],\' '+
                        'SELECT T0."ItemCode" as "SKU", '+
                        'T0."ItemName" as "DescripcionSKU", '+
                        'T1."NcmCode" as "CodigoSAT", '+
                        'T0."CardCode" as "CodigoProveedor", '+
                        'T2."CardName" as "Proveedor", '+
                        'T0."BuyUnitMsr" as "UnidadMedidaCompra", '+
                        '\'\'H87\'\' as "ClaveUnidad", '+
                        'REPLACE(CASE WHEN IFNULL(T0."U_SYS_GUML",\'\'\'\') = \'\'\'\' THEN VZ."Weight1" ELSE SY."U_SYS_PE01" END,\'\',\'\',\'\'\'\') as "PesoArticulo", '+
                        'T0."NumInBuy" as "ItemsUnidadCompra", '+
                        'T0."PurPackUn" as "CantidadPaquete" '+
                        'FROM "SBOMINISO"."OITM" T0 '+
                        'INNER JOIN "SBOMINISO"."ONCM" T1 ON T0."NCMCode" = T1."AbsEntry" '+
                        'INNER JOIN "SBOMINISO"."OCRD" T2 ON T0."CardCode" = T2."CardCode" '+
                        'LEFT JOIN "SBOMINISO"."UGP1" PZ ON T0."UgpEntry" = PZ."UgpEntry" '+
                        'AND PZ."UomEntry" = 1 '+
                        'LEFT JOIN "SBOMINISO"."ITM12" VZ ON T0."ItemCode" = VZ."ItemCode" '+
                        'AND PZ."UomEntry" = VZ."UomEntry" '+
                        'AND VZ."UomType" = \'\'P\'\' '+
                        'LEFT JOIN "SBOMINISO"."@SYS_ARTICULOS" SY ON T0."ItemCode" = SY."U_SYS_CODA"\') Tx')
            registros=cursor.fetchall()
            for registro in registros:
                articulo=Articulo(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9])
                articulosList.append(articulo)
            return articulosList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getPedido(self, idsPedidos, numRegistros):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            detallesPedidoList=[]
            registros=''
            if numRegistros:
                registros='TOP '+numRegistros
            cursor.execute('SELECT '+registros+' Tx.Cantidad, '+
                            'Tx.IdUnidadEmbalaje, '+
                            'Tx.DescripcionMaterialCarga, '+
                            'Tx.PesoArticulo, '+
                            'IdUnidadPeso, '+
                            'Tx.ClaveProductoServicio, '+
                            'Tx.ClaveUnidadMedidaEmbalaje, '+
                            'Tx.ClaveUnidad, '+
                            'Tx.MaterialPeligroso, '+
                            'Tx.Pedido, '+
                            'Tx.Tienda, '+
                            'Tx.NombreTienda, '+
                            'Tx.CodigoPostal, '+
                            'Tx.Pais, '+
                            'Tx.Estado, '+
                            'Tx.Direccion, '+
                            'Tx.UnidadMedida, '+
                            'Tx.IdOrigen, '+
                            'Tx.IdDestino, '+
                            'Tx.Volumen, '+
                            'Tx.UnidadVolumen '+
                        'FROM OPENQUERY '+
                        '([HANADB_SAP_COMX],\' '+
                        'SELECT T2."Quantity" as "Cantidad", '+
                        '\'\'5\'\' as "IdUnidadEmbalaje", '+
                        'SF."Name" as "DescripcionMaterialCarga", '+
                        'REPLACE(CASE WHEN IFNULL(T3."U_SYS_GUML",\'\'\'\') = \'\'\'\' THEN VZ."Weight1" ELSE SY."U_SYS_PE01" END,\'\',\'\',\'\'\'\') as "PesoArticulo", '+
                        '\'\'21\'\' as "IdUnidadPeso", '+
                        'T4."NcmCode" as "ClaveProductoServicio", '+
                        '\'\'KGM\'\' as "ClaveUnidadMedidaEmbalaje", '+
                        '\'\'H87\'\' as "ClaveUnidad", '+
                        '\'\'NO\'\' as "MaterialPeligroso", '+
                        'T0."DocNum" as "Pedido", '+
                        'T0."ToWhsCode" as "Tienda", '+
                        'T1."WhsName" as "NombreTienda", '+
                        'T1."ZipCode" as "CodigoPostal", '+
                        '\'\'MEX\'\' as "Pais", '+
                        'CASE T1."State" '+
                        'WHEN \'\'MCH\'\' THEN \'\'MIC\'\' '+
                        'WHEN \'\'QR\'\' THEN \'\'ROO\'\' '+
                        'WHEN \'\'CMX\'\' THEN \'\'DIF\'\' '+
                        'WHEN \'\'CHS\'\' THEN \'\'CHP\'\' '+
                        'WHEN \'\'GTO\'\' THEN \'\'GUA\'\' '+
                        'WHEN \'\'CHI\'\' THEN \'\'CHH\'\' '+
                        'WHEN \'\'NL\'\' THEN \'\'NLE\'\' '+
                        'WHEN \'\'AGS\'\' THEN \'\'AGU\'\' '+
                        'WHEN \'\'BC\'\' THEN \'\'BCN\'\' '+
                        'WHEN \'\'DF\'\' THEN \'\'DIF\'\' '+
                        'ELSE T1."State" '+
                        'END as "Estado", '+
                        'CONCAT(CONCAT(T1."Street", \'\', \'\'), T1."StreetNo") as "Direccion", '+
                        'T3."BuyUnitMsr" as "UnidadMedida", '+
                        'T0."Filler" as "IdOrigen", '+
                        'CONCAT(\'\'DE00\'\', substring(T0."ToWhsCode",2,4)) as "IdDestino", '+
                        'T3."BVolume" as "Volumen", '+
                        'CASE T3."BVolUnit" '+
                        'WHEN 1 THEN \'\'cc\'\' '+
                        'WHEN 2 THEN \'\'cf\'\' '+
                        'WHEN 3 THEN \'\'ci\'\' '+
                        'WHEN 4 THEN \'\'cm\'\' '+
                        'WHEN 5 THEN \'\'cmm\'\' '+
                        'WHEN 6 THEN \'\'dm3\'\'  '+
                        'ELSE \'\'sin\'\' '+
                        'END as "UnidadVolumen" '+
                        'FROM "SBOMINISO"."OWTQ" T0 '+
                        'INNER JOIN "SBOMINISO"."OWHS" T1 ON T0."ToWhsCode" = T1."WhsCode" '+
                        'INNER JOIN "SBOMINISO"."WTQ1" T2 ON T0."DocEntry" = T2."DocEntry" '+
                        'INNER JOIN "SBOMINISO"."OITM" T3 ON T2."ItemCode" = T3."ItemCode" '+
                        'INNER JOIN "SBOMINISO"."ONCM" T4 ON T2."NCMCode" = T4."AbsEntry" '+
                        'INNER JOIN "SBOMINISO"."OADM" AM ON 1 = 1 '+
                        'LEFT JOIN "SBOMINISO"."OUGP" UM ON T3."UgpEntry" = UM."UgpEntry" '+
                        'LEFT JOIN "SBOMINISO"."UGP1" PZ ON UM."UgpEntry" = PZ."UgpEntry" '+
                        'AND PZ."UomEntry" = 1 '+
                        'LEFT JOIN "SBOMINISO"."ITM12" VZ ON T3."ItemCode" = VZ."ItemCode" '+
                        'AND PZ."UomEntry" = VZ."UomEntry" '+
                        'AND VZ."UomType" = \'\'P\'\' '+
                        'LEFT JOIN "SBOMINISO"."@SYS_ARTICULOS" SY ON T3."ItemCode" = SY."U_SYS_CODA" '+
                        'LEFT JOIN "SBOMINISO"."@SUBSUBFAMILIA" SF ON T3."U_SUBSUBFAMILIA" = SF."Code" '+
                        'WHERE T0."DocNum" in ('+idsPedidos+') order by T0."ToWhsCode", T0."DocNum"\') Tx')
            registros=cursor.fetchall()
            for registro in registros:
                detallePedido=DetallePedido(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], registro[10], 
                                  registro[11], registro[12], registro[13], registro[14], registro[15], registro[16], registro[17], registro[18], registro[19], registro[20])
                if detallePedido.tienda[0:3]=='MKP':
                    detallePedido.idDestino='DE30'+detallePedido.tienda[3:]
                elif detallePedido.tienda =='CEN-PT':
                    detallePedido.idDestino='DE100000'
                elif detallePedido.tienda =='CEN-UV':    
                    detallePedido.idDestino='DE200000'
                if detallePedido.idOrigen =='CEN-PT':
                    detallePedido.idOrigen = 'OR000001'
                elif detallePedido.idOrigen =='CEN-UV':
                    detallePedido.idOrigen = 'OR000002'
                elif detallePedido.idOrigen[0:1]=='T':
                    detallePedido.idOrigen = 'OR10'+detallePedido.tienda[1:]
                    
                detallesPedidoList.append(detallePedido)
            return detallesPedidoList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getStorageTemplates(self, numRegistros):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            storagesTemplatesList=[]
            registros=''
            if numRegistros:
                registros='TOP '+numRegistros
            cursor.execute('SELECT '+registros+' Tx.ItemCode, '+
                            'Tx.StorageTemplate, '+
                            'Tx.GrupoLogistico, '+
                            'Tx.SalUnitMsr, '+
                            'Tx.Familia, '+
                            'Tx.SubFamilia, '+
                            'Tx.SubSubFamilia, '+
                            'Tx.U_SYS_CAT4, '+
                            'Tx.U_SYS_CAT5, '+
                            'Tx.U_SYS_CAT6, '+
                            'Tx.U_SYS_CAT7, '+
                            'Tx.U_SYS_CAT8, '+
                            'Tx.Height, '+
                            'Tx.Width, '+
                            'Tx.Length, '+
                            'Tx.Volume, '+
                            'Tx.Weight '+
                            'FROM OPENQUERY ([HANADB_SAP_COMX],\' '+
                            'SELECT T0."ItemCode", '+
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
                            'FROM "SBOMINISO"."OITM"  T0 '+
                            'INNER JOIN "SBOMINISO"."OUGP"  T1 ON T0."UgpEntry" = T1."UgpEntry" '+
                            'INNER JOIN "SBOMINISO"."OITB" T2 ON T0."ItmsGrpCod" = T2."ItmsGrpCod" '+
                            'LEFT JOIN "SBOMINISO"."@SUBSUBFAMILIA" SSF ON T0."U_SUBSUBFAMILIA" = SSF."Code" '+
                            'LEFT JOIN "SBOMINISO"."@SUBFAMILIA" SF ON T0."U_SUBFAMILIA" = SF."Code"\') TX')
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

    def getStorageTemplatesCL(self, numRegistros):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            storagesTemplatesList=[]
            registros=''
            if numRegistros:
                registros='TOP '+numRegistros
            cursor.execute('SELECT '+registros+' Tx.ItemCode, '+
                            'Tx.StorageTemplate, '+
                            'Tx.GrupoLogistico, '+
                            'Tx.SalUnitMsr, '+
                            'Tx.Familia, '+
                            'Tx.SubFamilia, '+
                            'Tx.SubSubFamilia, '+
                            'Tx.U_SYS_CAT4, '+
                            'Tx.U_SYS_CAT5, '+
                            'Tx.U_SYS_CAT6, '+
                            'Tx.U_SYS_CAT7, '+
                            'Tx.U_SYS_CAT8, '+
                            'Tx.Height, '+
                            'Tx.Width, '+
                            'Tx.Length, '+
                            'Tx.Volume, '+
                            'Tx.Weight '+
                            'FROM OPENQUERY ([HANADB_SAP_LATAM],\' '+
                            ' SELECT T0."ItemCode",  ' +
                            ' T1."UgpCode" AS "StorageTemplate",   ' +
                            ' T0."U_SYS_GUML" AS "GrupoLogistico",  ' +
                            ' COALESCE(Tz."U_SYS_UNID", CAST(T0."SalUnitMsr" AS NVARCHAR(200))) AS "SalUnitMsr",  ' +
                            ' T2."ItmsGrpNam" AS "Familia",   ' +
                            ' COALESCE(SF."Name" , CAST(T0."U_SUBFAMILIA" AS NVARCHAR(200))) AS "SubFamilia",   ' +
                            ' COALESCE(SSF."Name", CAST(T0."U_SUBSUBFAMILIA" AS NVARCHAR(200))) AS "SubSubFamilia", ' +
                            ' T0."U_SYS_CAT4",   ' +
                            ' T0."U_SYS_CAT5",   ' +
                            ' T0."U_SYS_CAT6",	  ' +
                            ' T0."U_SYS_CAT7",   ' +
                            ' T0."U_SYS_CAT8",   ' +
                            ' COALESCE(Tz."U_SYS_ALTO", CAST(T0."BHeight1" AS NVARCHAR(200))) as "Height",   ' +
                            ' COALESCE(Tz."U_SYS_ANCH", CAST(T0."BWidth1" AS NVARCHAR(200))) as "Width",   ' +
                            ' COALESCE(Tz."U_SYS_LONG", CAST(T0."BLength1" AS NVARCHAR(200))) as "Length",   ' +
                            ' COALESCE(Tz."U_SYS_VOLU", CAST(T0."BVolume" AS NVARCHAR(200))) as "Volume",   ' +
                            ' COALESCE(Tz."U_SYS_PESO", CAST(T0."BWeight1" AS NVARCHAR(200))) as "Weight"   ' +
                            ' FROM "SBO_MINISO_CHILE"."OITM" T0  ' +
                            ' LEFT JOIN "SBO_MINISO_CHILE"."@SYS_DIMENSIONES" Tz ON Tz."U_SYS_CODA"=T0."ItemCode"   ' +
                            ' INNER JOIN "SBO_MINISO_CHILE"."OUGP"  T1 ON T0."UgpEntry" = T1."UgpEntry"  ' +
                            ' INNER JOIN "SBO_MINISO_CHILE"."OITB" T2 ON T0."ItmsGrpCod" = T2."ItmsGrpCod"   ' +
                            ' LEFT JOIN "SBO_MINISO_CHILE"."@SUBSUBFAMILIA" SSF ON T0."U_SUBSUBFAMILIA" = SSF."Code"   ' +
                            ' LEFT JOIN "SBO_MINISO_CHILE"."@SUBFAMILIA" SF ON T0."U_SUBFAMILIA" = SF."Code"  ' +
                            ' ORDER BY T0."ItemCode",  Tz."U_SYS_UNID"  \') TX')
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

    def getCodigosSAT(self):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            codigosSatList=[]
            cursor.execute('SELECT Tx.ItemCode, '+
                            'Tx.CodigoSat '+
                            'FROM OPENQUERY '+
                            '([HANADB_SAP_COMX],\' '+
                            'SELECT T0."ItemCode", '+
                            'T1."NcmCode" as "CodigoSat" '+
                            'FROM "SBOMINISO"."OITM" T0  '+
                            'INNER JOIN "SBOMINISO"."ONCM" T1 ON T0."NCMCode" = T1."AbsEntry"\') Tx')
            registros=cursor.fetchall()
            print (f'total registros {len(registros)}')
            for registro in registros:
                codigoSat=CodigoSat(registro[0], registro[1])
                codigosSatList.append(codigoSat)
            return codigosSatList
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
            cursor.execute('SELECT '+registros+' Tx.ItemCode, '+
                            'Tx.Codigo_Barras, '+
                            'Tx.Categoria, '+
                            'Tx.Subcategoria, '+
                            'Tx.Clase, '+
                            'Tx.ItemName, '+
                            'Tx.Storage_Template, '+
                            'TX.ST_USR, '+
                            'Tx.Licencia, '+
                            'Tx.Height, '+
                            'Tx.Width, '+
                            'Tx.Length, '+
                            'Tx.Volume, '+
                            'Tx.Weight, '+
                            'Tx.FV_ESTANDAR, '+
                            'Tx.ESTANDAR_SIN_IVA, '+
                            'Tx.FV_APTOS_CDMX, '+
                            'Tx.FV_APTOS_FORANEOS, '+
                            'Tx.FV_APTOS_FRONTERIZOS, '+
                            'Tx.FV_OUTLET, '+
                            'Tx.FV_FRONTERA, '+
                            'Tx.Proveedor '+
                            'FROM OPENQUERY ([HANADB_SAP_COMX],\' '+
                            'SELECT T0."ItemCode", '+ 
                            'C0."BcdCode" as "Codigo_Barras", '+
                            'F."ItmsGrpNam" as "Categoria", '+ 
                            'SF."Name" as "Subcategoria", '+ 
                            'SSF."Name" as "Clase", '+
                            'T0."ItemName", '+ 
                            'T6."UgpCode"  as "Storage_Template", '+
                            'T0."U_SYS_SLICE"  as "Licencia", '+
                            'T0."BHeight1"  as "Height", '+  
                            'T0."BWidth1" as "Width", '+ 
                            'T0."BLength1" as "Length", '+
                            'T0."BVolume" as "Volume", '+ 
                            'T0."BWeight1"  as "Weight", '+
                            'T1."Price" as "FV_ESTANDAR", '+ 
                            'T9."Price" as "ESTANDAR_SIN_IVA", '+ 
                            'T2."Price" as "FV_APTOS_CDMX", '+ 
                            'T8."Price" as "FV_APTOS_FORANEOS", '+
                            'T3."Price" as "FV_APTOS_FRONTERIZOS", '+
                            'T4."Price" as "FV_OUTLET", '+
                            'T5."Price" as "FV_FRONTERA", '+
                            'T7."CardName" as "Proveedor", '+
                            'T0."U_SYS_GUML" as "ST_USR" '+
                            'FROM "SBOMINISO"."OITM" T0  '+ 
                            'LEFT JOIN "SBOMINISO"."ITM1" T1 ON T0."ItemCode" = T1."ItemCode" and T1."PriceList"=3 '+
                            'LEFT JOIN "SBOMINISO"."ITM1" T2 ON T0."ItemCode" = T2."ItemCode" and T2."PriceList"=11 '+
                            'LEFT JOIN "SBOMINISO"."ITM1" T3 ON T0."ItemCode" = T3."ItemCode" and T3."PriceList"=10 '+
                            'LEFT JOIN "SBOMINISO"."ITM1" T4 ON T0."ItemCode" = T4."ItemCode" and T4."PriceList"=8 '+
                            'LEFT JOIN "SBOMINISO"."ITM1" T5 ON T0."ItemCode" = T5."ItemCode" and T5."PriceList"=9 '+
                            'LEFT JOIN "SBOMINISO"."ITM1" T8 ON T0."ItemCode" = T8."ItemCode" and T8."PriceList"=4 '+
                            'LEFT JOIN "SBOMINISO"."ITM1" T9 ON T0."ItemCode" = T9."ItemCode" and T9."PriceList"=2 '+
                            'INNER JOIN "SBOMINISO"."OITB" F ON T0."ItmsGrpCod" = F."ItmsGrpCod" '+ 
                            'LEFT JOIN "SBOMINISO"."@SUBSUBFAMILIA" SSF ON T0."U_SUBSUBFAMILIA" = SSF."Code" '+
                            'LEFT JOIN "SBOMINISO"."@SUBFAMILIA" SF ON T0."U_SUBFAMILIA" = SF."Code" '+
                            'LEFT JOIN "SBOMINISO"."OUGP"  T6 ON T0."UgpEntry" = T6."UgpEntry" '+
                            'LEFT JOIN "SBOMINISO"."OCRD" T7 ON T0."CardCode" = T7."CardCode" '+
                            'LEFT JOIN "SBOMINISO"."OBCD" C0 ON C0."ItemCode"=T0."ItemCode" '+
                            'where T0."SellItem" = \'\'Y\'\' AND T0."validFor" = \'\'Y\'\' '+ 
                            'order by T0."ItemCode"\') TX')
            registros=cursor.fetchall()
            itemCode=''
            precio=Precio('','','','','','','','','','','','','','','','','','','','','','')
            for registro in registros:
                if itemCode!=registro[0]:
                    precio=Precio(registro[0], registro[1], registro[2], registro[3], registro[4], registro[5], registro[6], registro[7], registro[8], registro[9], 
                                registro[10], registro[11], registro[12], registro[13], registro[14], registro[15], registro[16], registro[17], registro[18], registro[19], registro[20], registro[21])
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
                
    def getInfoTransaccionTR(self, pedidosSinTrList):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            infoPedidosSinTrList=[]
            for pedidoSinTr in pedidosSinTrList:
                cursor.execute('SELECT Tx.* FROM OPENQUERY ([HANADB_SAP_COMX],\' '+
                               'select DISTINCT '+
                               'B."DocNum" AS PEDIDO, '+
                               'C."ToWhsCode" AS TIENDA, '+
                               'A."WhsCode" AS ALMACEN, '+
                               'C."U_SYS_REFE" AS BNEXT, '+
                               'C."DocNum" AS DOCK_ENTRY '+
                               'from "SBOMINISO"."WTR1" A  '+
                               'INNER JOIN "SBOMINISO"."OWTQ" B ON A."BaseEntry" = B."DocEntry" '+
                               'INNER JOIN "SBOMINISO"."OWTR" C on A."DocEntry"=C."DocEntry" '+
                               'INNER JOIN "SBOMINISO"."WTQ1" D ON B."DocEntry"= D."DocEntry" '+
                               'where B."DocNum"='+pedidoSinTr+' \') Tx')
                registros=cursor.fetchall()
                for registro in registros:
                    infoPedidoSinTr=InfoPedidoSinTr(registro[0], registro[1], registro[2], registro[3], registro[4])
                    infoPedidosSinTrList.append(infoPedidoSinTr)
            return infoPedidosSinTrList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getPedidosPlaneacion(self, fechaInicio, fechaFin):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            pedidosList=[]
            cursor.execute('SELECT Tx.DocNum, Tx.ItemCode, Tx.Quantity, Tx.DocDate, Tx.DocDueDate '+
                'FROM OPENQUERY ([HANADB_SAP_COMX],\' '+
                'SELECT T0."DocNum", T1."ItemCode", T1."Quantity", T0."DocDate", T0."DocDueDate" '+ 
                'FROM "SBOMINISO"."OWTQ" T0 '+  
                'INNER JOIN "SBOMINISO"."WTQ1" T1 ON T0."DocEntry" = T1."DocEntry" '+ 
                'WHERE TO_NVARCHAR(T0."DocDate", \'\'yyyy-MM-dd\'\') between \'\''+fechaInicio+'\'\' and \'\''+fechaFin+'\'\' ORDER BY T0."DocDate", T0."DocNum" \') Tx')
            
            registros=cursor.fetchall()
            for registro in registros:
                pedidoSapPlaneacion=PedidoSapPlaneacion(registro[0], registro[1], registro[2], registro[3], registro[4])
                pedidosList.append(pedidoSapPlaneacion)
            return pedidosList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getPedidosPlaneacionTop(self, fechaInicio, fechaFin):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            pedidosList=[]
            cursor.execute('SELECT Tx.DocNum, Tx.ItemCode, Tx.Quantity, Tx.DocDate, Tx.DocDueDate '+
                'FROM OPENQUERY ([HANADB_SAP_COMX],\' '+
                'SELECT TOP 100 T0."DocNum", T1."ItemCode", T1."Quantity", T0."DocDate", T0."DocDueDate" '+ 
                'FROM "SBOMINISO"."OWTQ" T0 '+  
                'INNER JOIN "SBOMINISO"."WTQ1" T1 ON T0."DocEntry" = T1."DocEntry" '+ 
                'WHERE TO_NVARCHAR(T0."DocDate", \'\'yyyy-MM-dd\'\') between \'\''+fechaInicio+'\'\' and \'\''+fechaFin+'\'\' ORDER BY T0."DocDate", T0."DocNum" \') Tx')
            
            registros=cursor.fetchall()
            for registro in registros:
                pedidoSapPlaneacion=PedidoSapPlaneacion(registro[0], registro[1], registro[2], registro[3], registro[4])
                pedidosList.append(pedidoSapPlaneacion)
            return pedidosList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
    
    def getShopList(self):
            try:
                conexion=self.getConexion()
                cursor=conexion.cursor()
                tiendaList=[]
                cursor.execute("""SELECT sap.CUSTOMER
                                FROM OPENQUERY(
                                    [HANADB_SAP_COMX],
                                    'SELECT "WhsCode" AS CUSTOMER
                                    FROM SBOMINISO.OWHS 
                                    WHERE "WhsCode" LIKE ''T%'' 
                                    ORDER BY "WhsCode"'
                                ) sap
                                """)
                registros=cursor.fetchall()
                for registro in registros:
                    tienda=StringOne(registro[0])
                    tiendaList.append(tienda)
                return tiendaList
            except Exception as exception:
                logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
                raise exception
            finally:
                if conexion!= None:
                    self.closeConexion(conexion)