import logging
import pyodbc
from sevicios_app.vo.redAddress import RedAddress
from sevicios_app.api.dao.SapService import SAPService
from sevicios_app.vo.containerPza import ContainerPza


logger = logging.getLogger('')

class WmsRed():

    def getConexion(self):
        try:
            direccion_servidor = '192.168.84.103'
            nombre_bd = 'ILS'
            nombre_usuario = 'manh'
            password = 'Pa$$w0rdLDM'
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
        
    def getAddress(self,shop):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            addressList=[]
            cursor.execute("select  top 1 customer,ship_to,route,stop,ship_to_address1,customer_name,ship_to_city,SHIP_TO_POSTAL_CODE,CUSTOMER_ADDRESS1,CUSTOMER_CITY,CUSTOMER_POSTAL_CODE,SHIP_TO_NAME from SHIPMENT_HEADER where CUSTOMER = ? ",shop)
            registros=cursor.fetchall()
            for registro in registros:
                address=RedAddress(registro[0],registro[1],registro[2],registro[3],registro[4],registro[5],registro[6],registro[7],registro[8],registro[9],registro[10],registro[11])
                addressList.append(address)
            return addressList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los reistros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def updateAddress(self, data,pedido):
        conexion = self.getConexion()
        cursor = conexion.cursor()

        try:
            header = self.validateShipHeader(pedido)
            detail = self.validateShipDetail(pedido)
            workI = self.validateWorkInstruction(pedido)
            for info in data:
                if int(header) > 0:
                    cursor.execute("""
                        update shipment_header 
                                    SET customer = ? ,ship_to = ? ,route = ? ,stop = ? ,ship_to_address1 = ? ,
                                        customer_name = ? ,ship_to_city = ? ,SHIP_TO_POSTAL_CODE = ? ,CUSTOMER_ADDRESS1 = ? ,
                                        CUSTOMER_CITY = ? ,CUSTOMER_POSTAL_CODE = ? ,SHIP_TO_NAME = ? 
                        where SHIPMENT_ID = ? and LAST_STATUS_UPLOADED != 900
                                    """,info.customer,info.ship_to,info.route,info.stop,info.ship_to_address1,info.customer_name,info.ship_to_city,info.ship_to_postal_code,info.customer_address1,info.customer_city,info.customer_postal_code,info.ship_to_name,pedido)
                    print("Header actualizado")
                
                if int(detail) > 0:
                    cursor.execute("""
                        update SHIPMENT_DETAIL 
                                    SET SHIP_TO = ? ,CUSTOMER = ? 
                                where SHIPMENT_ID = ?
                                    """,info.customer,info.customer,pedido)
                    print("Detail actualizado")
                    
                if int(workI) > 0:                
                    cursor.execute("""
                        update WORK_INSTRUCTION set ACCOUNT = ?
                                where  REFERENCE_ID = ?
                                    """,info.customer,pedido)
                    print("Work Instruction actualizado")

                conexion.commit()
                # print(info.customer,info.ship_to,info.route,info.stop,info.ship_to_address1,info.customer_name,info.ship_to_city,info.ship_to_postal_code,info.customer_address1,info.customer_city,info.customer_postal_code,info.ship_to_name,pedido)
            print("Actualizaciones listas wms")
            return True
        except Exception as e:
            logger.error(f"Error al actualizar en bloque: {e}")
            conexion.rollback()
            return str(e)
        finally:
            self.closeConexion(conexion)    
        
    def redirection(self,pedido,shop):
        try:
            addressList = self.getAddress(shop)
            confirm = self.updateAddress(addressList,pedido)

            s = SAPService()
            sapConfirm = s.ActializarPedido(pedido,shop)

            if confirm and sapConfirm:
                print("Redireccion exitosa")
                return "Redireccion exitosa"
            elif confirm and not sapConfirm:
                print("Solo se actualizo wms")
                return "Solo se actualizo wms"
            elif sapConfirm and not confirm:
                print("Solo se actualizo sap")
                return "Solo se actualizo sap"
            else:
                print("Redireccion fallida")
                return "Redireccion fallida"
        except Exception as e:
            logger.error(f"Error al actualizar en bloque: {e}")
            print(e)
            
    def validateShipHeader(self, pedido):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute('SELECT count(*) FROM Shipment_Header WHERE Shipment_id = ?',pedido)
            docEntry = cursor.fetchone()
            return docEntry[0]
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def validateShipDetail(self, pedido):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute('SELECT count(*) FROM SHIPMENT_DETAIL WHERE Shipment_id = ?',pedido)
            docEntry = cursor.fetchone()
            return docEntry[0]
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
                        
    def validateWorkInstruction(self, pedido):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute('SELECT count(*) FROM WORK_INSTRUCTION WHERE REFERENCE_ID = ?',pedido)
            docEntry = cursor.fetchone()
            return docEntry[0]
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def getContainerAndQty(self, pedido):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            containerList=[]
            cursor.execute(f"""
                            select  count(distinct s.CONTAINER_ID) Contenedores, 
                            (SELECT sum(a.total_qty) FROM SHIPMENT_DETAIL a WHERE a.SHIPMENT_ID = c.SHIPMENT_ID and a.STATUS1 NOT IN ('900','999')) CantidadTotal,
                            c.SHIP_TO
                            from SHIPPING_CONTAINER s
                            left join  SHIPMENT_DETAIL c on s.INTERNAL_SHIPMENT_NUM = c.INTERNAL_SHIPMENT_NUM 
                            where c.SHIPMENT_ID = '{pedido}' and s.CONTAINER_ID is not null and s.status != 900  
                            group by c.SHIP_TO, c.SHIPMENT_ID""")
            docEntry = cursor.fetchall()
            for registro in docEntry:
                container = ContainerPza(registro[0],registro[1],registro[2])
                containerList.append(container)
            return containerList
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)