class InventarioTop():
    def __init__(self, fecha, item, warehouse, wmsComprometido, wmsTransito, wmsOnHand, erpOnHand, difOnHand, difOnHandAbsolute):
        self.fecha=fecha
        self.item=item
        self.warehouse=warehouse
        self.wmsComprometido=wmsComprometido
        self.wmsTransito=wmsTransito
        self.wmsOnHand=wmsOnHand
        self.erpOnHand=erpOnHand
        self.difOnHand=difOnHand
        self.difOnHandAbsolute=difOnHandAbsolute
