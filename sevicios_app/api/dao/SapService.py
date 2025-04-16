import requests
import logging
from hdbcli import dbapi
import json

logger = logging.getLogger(__name__)

class SAPService:
    
    server = "https://192.168.84.182"
    port = "50000"
    version = "/b1s/v2"
    company_db = "SBOMINISO"
    username = "JONATHAN"
    password = "J*n4th4n-4"

    def iniciarSesion(self):
        try:
            url = f"{self.server}:{self.port}{self.version}/Login"

            
            body = {
                "CompanyDB": self.company_db,
                "UserName": self.username,
                "Password": self.password
            }
            
            response = requests.post(url, json=body, verify=False)

            if response.status_code == 200:
                response_data = response.json()
                session_id = response_data.get('SessionId')
                print(session_id)
                return session_id
            else:
                print(response.raise_for_status())
        except Exception as e:
            print(e)
            raise

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
        
    def getDocEntry(self, pedido):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute('SELECT "DocEntry"  FROM "SBOMINISO"."OWTQ" WHERE "DocNum" = ?',pedido)
            docEntry = cursor.fetchone()
            return docEntry
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)
                 
    def getLineas(self, docEntry):
        try:
            conexion=self.getConexion()
            cursor=conexion.cursor()
            cursor.execute('SELECT  Count(*)  FROM "SBOMINISO"."WTQ1"  where "DocEntry" = ? ',docEntry[0])
            nItems = cursor.fetchone()
            return nItems
        except Exception as exception:
            logger.error(f"Se presento una incidencia al obtener los registros: {exception}")
            raise exception
        finally:
            if conexion!= None:
                self.closeConexion(conexion)

    def ActializarPedido(self, pedido, tienda):
        try:
            docEntry = self.getDocEntry(pedido) 
            nItems = self.getLineas(docEntry)

            cookie = self.iniciarSesion()

            url = f"{self.server}:{self.port}{self.version}/InventoryTransferRequests({docEntry[0]})"

            headers = {
                "Cookie": f"B1SESSION={cookie}; ROUTEID=.node1"
            }

            stock_transfer_lines = [ #Armar json para cada detalle de item
                {"LineNum": i, "WarehouseCode": tienda} for i in range(nItems[0])
            ]

            body = {
                "ToWarehouse": tienda,
                "StockTransferLines": stock_transfer_lines
            }
            response = requests.patch(url, headers=headers, json=body, verify=False)

            if response.status_code == 204:
                print("Pedido actualizado correctamente")
                return True
            else:
                print(response.raise_for_status())
                return False
        except Exception as e:
            print(e)
            return False
            raise

# s = SAPService()
# s.ActializarPedido("202", "T0002")