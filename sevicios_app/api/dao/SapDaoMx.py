import logging
from hdbcli import dbapi
from sevicios_app.vo.storageTemplate import StorageTemplate


logger = logging.getLogger('')

class SapDaoMx():

    def getConexion(self):
        try:
            conexion=dbapi.connect(address="192.168.84.182", port=30015, user="SYSTEM", password="Sy573Mmnso!!")
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

    def getStorageTemplates(self, numRegistros):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            storagesTemplatesList=[]
            registros=''
            if numRegistros:
                registros='TOP '+numRegistros
            cursor.execute(f""" SELECT {registros}  T0."ItemCode", 
                            T1."UgpCode" AS "StorageTemplate",  
                            T0."U_SYS_GUML" AS "GrupoLogistico", 
                            COALESCE(Tz."U_SYS_UNID", CAST(T0."SalUnitMsr" AS NVARCHAR(200))) AS "SalUnitMsr", 
                            T2."ItmsGrpNam" AS "Familia",  
                            COALESCE(SF."Name" , CAST(T0."U_SUBFAMILIA" AS NVARCHAR(200))) AS "SubFamilia",  
                            COALESCE(SSF."Name", CAST(T0."U_SUBSUBFAMILIA" AS NVARCHAR(200))) AS "SubSubFamilia",
                            T0."U_SYS_CAT4",  
                            T0."U_SYS_CAT5",  
                            T0."U_SYS_CAT6",	 
                            T0."U_SYS_CAT7",  
                            T0."U_SYS_CAT8",  
                            COALESCE(Tz."U_SYS_ALTO", CAST(T0."BHeight1" AS NVARCHAR(200))) as "Height",  
                            COALESCE(Tz."U_SYS_ANCH", CAST(T0."BWidth1" AS NVARCHAR(200))) as "Width",  
                            COALESCE(Tz."U_SYS_LONG", CAST(T0."BLength1" AS NVARCHAR(200))) as "Length",  
                            COALESCE(Tz."U_SYS_VOLU", CAST(T0."BVolume" AS NVARCHAR(200))) as "Volume",  
                            COALESCE(Tz."U_SYS_PESO", CAST(T0."BWeight1" AS NVARCHAR(200))) as "Weight"  
                            FROM "SBOMINISO"."OITM" T0 
                            LEFT JOIN "SBOMINISO"."@SYS_DIMENSIONES" Tz ON Tz."U_SYS_CODA"=T0."ItemCode"  
                            INNER JOIN "SBOMINISO"."OUGP"  T1 ON T0."UgpEntry" = T1."UgpEntry" 
                            INNER JOIN "SBOMINISO"."OITB" T2 ON T0."ItmsGrpCod" = T2."ItmsGrpCod"  
                            LEFT JOIN "SBOMINISO"."@SUBSUBFAMILIA" SSF ON T0."U_SUBSUBFAMILIA" = SSF."Code"  
                            LEFT JOIN "SBOMINISO"."@SUBFAMILIA" SF ON T0."U_SUBFAMILIA" = SF."Code" 
                            ORDER BY T0."ItemCode",  Tz."U_SYS_UNID"
                            """ )
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