class InventarioDetalleErpWms():
    def __init__(self, fecha, item, warehouse, wmsComprometido, wmsTransito, wmsOnHand, ErpOnHand, diferenciaOnHand, diferenciaOnHandAbsoluta):
        self.fecha=fecha
        self.item=item
        self.warehouse=warehouse
        self.wmsComprometido=wmsComprometido
        self.wmsTransito=wmsTransito
        self.wmsOnHand=wmsOnHand
        self.erpOnHand=ErpOnHand
        self.diferenciaOnHand=diferenciaOnHand
        self.diferenciaOnHandAbsoluta=diferenciaOnHandAbsoluta
