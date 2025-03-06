class InventarioWmsErp():
    def __init__(self, warehouse, wmsOnHand, erpOnHand, diferencia, diferenciaAbsoluta, wmsInTransit, numItemsWms, numItemsErp, numItemsDif):
        self.warehouse=warehouse
        self.wmsOnHand=wmsOnHand
        self.erpOnHand=erpOnHand
        self.diferencia=diferencia
        self.diferenciaAbsoluta=diferenciaAbsoluta
        self.wmsInTransit=wmsInTransit
        self.numItemsWms=numItemsWms
        self.numItemsErp=numItemsErp
        self.numItemsDif=numItemsDif
