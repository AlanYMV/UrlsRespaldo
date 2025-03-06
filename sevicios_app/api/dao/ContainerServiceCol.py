import pyodbc
import logging
import pandas as pd
import time

logger = logging.getLogger('')

class ContainerServiceCol:
    
    def getConexion(self):
        try:
            direccion_servidor = '192.168.110.4'
            nombre_bd = 'ILS'
            nombre_usuario = 'manh'
            password = 'Pa$$w0rdLDM'
            conexion = pyodbc.connect(
                f'DRIVER={{SQL Server}};SERVER={direccion_servidor};DATABASE={nombre_bd};UID={nombre_usuario};PWD={password}'
            )
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

    def validate_columns(self, dataframe, action, expected_columns):
        columns = dataframe.columns
        missing_columns = [col for col in expected_columns[action] if col not in columns]
        if missing_columns:
            return f"Faltan las siguientes columnas: {', '.join(missing_columns)}"
        return None
    
    def check_existing_containers(self, container_types):
        try:
            conexion = self.getConexion()
            cursor = conexion.cursor()
            batch_size = 2000  

            existing_containers = set()
            container_types = [str(ct) for ct in container_types]
            for i in range(0, len(container_types), batch_size):
                sub_batch = container_types[i:i + batch_size]
                placeholders = ', '.join(['?'] * len(sub_batch))
                sql_check = f"SELECT CONTAINER_TYPE FROM CONTAINER_TYPE WHERE CONTAINER_TYPE IN ({placeholders})"
                cursor.execute(sql_check, sub_batch)
                existing_containers.update(row[0] for row in cursor.fetchall())

            return existing_containers
        except Exception as e:
            logger.error(f"Error al comprobar contenedores existentes: {e}")
            raise
        finally:
            self.closeConexion(conexion)
    
    def check_existing_items(self, sku_list, um_list):
        conexion = self.getConexion()
        cursor = conexion.cursor()
        
        items = list(zip(sku_list, um_list))
        
        if not items:
            return set()
        
        conditions = ' OR '.join(['(ITEM = ? AND QUANTITY_UM = ?)'] * len(items))
        sql_check = f"""
        SELECT ITEM, QUANTITY_UM 
        FROM ITEM_UNIT_OF_MEASURE 
        WHERE {conditions}
        """
        
        try:
            cursor.execute(sql_check, [param for item in items for param in item])
            existing_items = set()
            registros = cursor.fetchall()
            for row in registros:
                existing_items.add((row[0], row[1]))
            
            return existing_items
        except Exception as e:
            logger.error(f"Error al comprobar elementos existentes: {e}")
            raise
        finally:
            self.closeConexion(conexion)

    def insert_containers(self, values_to_insert):
        try:
            conexion = self.getConexion()
            cursor = conexion.cursor()
            sql_insert = """
            INSERT INTO CONTAINER_TYPE(CONTAINER_TYPE, DESCRIPTION, CONTAINER_CLASS, EMPTY_WEIGHT, WEIGHT_UM, LENGTH, WIDTH, HEIGHT, ACTIVE, WEIGHT_TOLERANCE, MAXIMUM_WEIGHT, USE_AS_DEFAULT, DATE_TIME_STAMP)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'N', GETDATE())
            """
            
            total_params_per_record = 12  
            max_params = 2100  
            batch_size = max_params // total_params_per_record  

            for i in range(0, len(values_to_insert), batch_size):
                sub_batch = values_to_insert[i:i + batch_size]
                cursor.executemany(sql_insert, sub_batch)
                conexion.commit()

            logger.info("Batch insert realizado correctamente.")
            return True
        except Exception as e:
            logger.error(f"Error al realizar el batch insert: {e}")
            conexion.rollback()
            return str(e)
        finally:
            self.closeConexion(conexion)

    def delete_containers(self, container_types):
        conexion = self.getConexion()
        cursor = conexion.cursor()
        container_types = [str(ct) for ct in container_types]

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
        batch_size = 2000  # Tamaño del lote

        try:
            values_to_update = [
                (row['LONGITUD'], row['ANCHURA'], row['ALTURA'], row['PESO'], row['SKU'], row['UM'])
                for row in data if (row['SKU'], row['UM']) in self.check_existing_items([row['SKU']], [row['UM']])
                
            ]
            if not values_to_update:
                return "No se encontraron elementos para actualizar."

            for i in range(0, len(values_to_update), batch_size):
                sub_batch = values_to_update[i:i + batch_size]
                cursor.executemany(sql_update, sub_batch)

            conexion.commit()
            logger.info("Actualizaciones realizadas con éxito")
            return True
        except Exception as e:
            logger.error(f"Error al actualizar en bloque: {e}")
            conexion.rollback()
            return str(e)
        finally:
            self.closeConexion(conexion)