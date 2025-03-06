import pyodbc
import logging
import pandas as pd
import time
from datetime import datetime

logger = logging.getLogger('')

class UpdatePromotion:
    
    def getConexion(self):
        try:
            direccion_servidor = '192.168.84.23'
            nombre_bd = 'recepciontienda'
            nombre_usuario = 'soportedb'
            password = 'T+Sab!G@(N)'
            conexion = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};SERVER='+direccion_servidor+';DATABASE='+nombre_bd+';UID='+nombre_usuario+';PWD='+password)
            return conexion
        except Exception as exception:
            logger.error(f"Se presentó un error al establecer la conexión: {exception}")
            raise exception

    def closeConexion(self, conexion):
        try:
            conexion.close()
        except Exception as exception:
            logger.error(f"Se presentó una incidencia al cerrar la conexión: {exception}")
            raise exception

    def validate_promotions(self):
        try:
            conexion = self.getConexion()
            cursor = conexion.cursor()
            cursor.execute("select count(*) numReg FROM PROMOCION WHERE FORMAT(FIN, 'yyyy-MM-dd')<format(GETDATE(), 'yyyy-MM-dd')")
            regBorrar=cursor.fetchone()
            print(f"{regBorrar[0]} registros de promociones finalizadas")
            cursor.execute("DELETE PROMOCION WHERE FORMAT(FIN, 'yyyy-MM-dd')<format(GETDATE(), 'yyyy-MM-dd')")
            conexion.commit()
            print("Depuro registros pasados")
            return True
        except Exception as e:
            logger.error(f"Error al comprobar contenedores existentes: {e}")
            raise
        finally:
            self.closeConexion(conexion)

    def parse_date(self,date_str):
        for fmt in ('%Y-%m-%d %H:%M:%S', '%d/%m/%Y'):
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                pass
        raise ValueError(f"No matching format found for date: {date_str}")

    def verificar_promociones(self, data):
        conexion = self.getConexion()
        cursor = conexion.cursor()
        sql_select = """
        SELECT ITEM, CODIGO_BARRAS, DESCRIPCION, PROMOCION, PRECIO_NORMAL, PRECIO_PROMOCION, INICIO, FIN
        FROM PROMOCION
        WHERE CODIGO_BARRAS = ?
        ORDER BY INICIO
        """
        results = []
        try:
            for row in data:
                codigos = str(row['COD DE BARRAS']).split(",")

                inicio_vigencia = row['Inicio Vigencia'] 
                fin_vigencia = row['FIN'] 

                for codigo in codigos:
                    codigo = str(codigo)
                    cursor.execute(sql_select, (codigo))
                    promociones = cursor.fetchall()
                    updated = False
                    
                    if promociones:
                        for promocion in promociones:
                            inicio = promocion[6]
                            fin = promocion[7]

                            normalPrice = round(float(promocion[4]), 2)
                            promocionPrice = round(float(promocion[5]), 2)

                            if isinstance(row['Inicio Vigencia'], str):
                                row_inicio_vigencia = self.parse_date(row['Inicio Vigencia'])
                            else:
                                row_inicio_vigencia = row['Inicio Vigencia']

                            if isinstance(row['FIN'], str):
                                row_fin = self.parse_date(row['FIN'])
                            else:
                                row_fin = row['FIN']
                            if row_inicio_vigencia <= inicio and row_fin >= fin:
                                cursor.execute("DELETE FROM PROMOCION WHERE ITEM = ? AND CODIGO_BARRAS = ? AND DESCRIPCION = ? AND PROMOCION = ? AND PRECIO_NORMAL = ? AND PRECIO_PROMOCION = ? AND INICIO = ? AND FIN = ?",
                                            (str(promocion[0]), codigo, str(promocion[2]), str(promocion[3]), str(normalPrice), str(promocionPrice), inicio, fin))
                                updated = True
                                # print("Primer escenario")

                            elif row_inicio_vigencia > inicio and row_fin >= fin and row_inicio_vigencia < fin:
                                cursor.execute("UPDATE PROMOCION SET FIN=DATEADD(DAY, -1, ?) WHERE ITEM=? and CODIGO_BARRAS=? and DESCRIPCION=? and PROMOCION=? and PRECIO_NORMAL=? and PRECIO_PROMOCION=? and INICIO=? and FIN=?", 
                                            (row['Inicio Vigencia'], str(promocion[0]), codigo, str(promocion[2]), str(promocion[3]), str(normalPrice), str(promocionPrice), inicio,fin))
                                updated = True
                                # print("Segundo escenario")

                            elif row_inicio_vigencia <= inicio and row_fin < fin:
                                cursor.execute("UPDATE PROMOCION SET INICIO=DATEADD(DAY, +1, ?) WHERE ITEM=? and CODIGO_BARRAS=? and DESCRIPCION=? and PROMOCION=? and PRECIO_NORMAL=? and PRECIO_PROMOCION=? and INICIO=? and FIN=?", 
                                            (row['FIN'], str(promocion[0]), codigo, str(promocion[2]), str(promocion[3]), str(normalPrice), str(promocionPrice), inicio, fin))
                                updated = True
                                # print("Tercer escenario")

                            elif row_inicio_vigencia > inicio and row_fin < fin:
                                cursor.execute("UPDATE PROMOCION SET FIN=DATEADD(DAY, -1, ?) WHERE ITEM=? and CODIGO_BARRAS=? and DESCRIPCION=? and PROMOCION=? and PRECIO_NORMAL=? and PRECIO_PROMOCION=? and INICIO=? and FIN=?", 
                                            (row['Inicio Vigencia'], str(promocion[0]), codigo, str(promocion[2]), str(promocion[3]), str(normalPrice), str(promocionPrice), inicio, fin))
                                cursor.execute("INSERT INTO PROMOCION (ITEM, CODIGO_BARRAS, DESCRIPCION, PROMOCION, PRECIO_NORMAL, PRECIO_PROMOCION, INICIO, FIN, FECHA_REGISTRO) VALUES (?, ?, ?, ?, ?, ?, DATEADD(DAY, 1, ?), ?, GETDATE())", 
                                            (str(promocion[0]), codigo, str(promocion[2]), str(promocion[3]), str(normalPrice), str(promocionPrice), row['Inicio Vigencia'], row['FIN']))
                                updated = True
                                # print("Cuarto escenario")
                                
                    if updated:
                        results.append({
                            "Respuesta": "Promoción actualizada correctamente",
                            "Codigo": row['CÓDIGO'],
                            "Precio promoción": row['precio promoción'],
                            "Inicio Vigencia": row['Inicio Vigencia'],
                            "Fin Vigencia": row['FIN']
                        })
                    else:
                        results.append({
                            "Respuesta": "No se encontró el artículo, se agrega.",
                            "Codigo": row['CÓDIGO'],
                            "Precio promoción": row['precio promoción'],
                            "Inicio Vigencia": row['Inicio Vigencia'],
                            "Fin Vigencia": row['FIN']
                        })

                    cursor.execute("INSERT INTO PROMOCION (ITEM, CODIGO_BARRAS, DESCRIPCION, PROMOCION, PRECIO_NORMAL, PRECIO_PROMOCION, INICIO, FIN, FECHA_REGISTRO) "+
                                        "VALUES (?,?,?,?, ?, ?, ?, ?, GETDATE())",
                                        (str(row['CÓDIGO']),codigo,str(row['DESCRIPCIÓN']),str(row['promoción final']),str(round(float(row['precio normal']),2)),str(round(float(row['precio promoción']),2)),row['Inicio Vigencia'],row['FIN']))
                    conexion.commit()

            return results
        except Exception as e:
            logger.error(f"Error al verificar promociones: {e}")
            print(e ,row['COD DE BARRAS'])
            return str(e)
        finally:
            cursor.close()
            conexion.close()