class InventarioWms():
    def __init__(self, warehouseCode, solicitado, onHand, comprometido, disponible, skuSolicitado, skuOnHand, skuComprometido, fechaActualizacion):
        self.warehouseCode=warehouseCode
        self.solicitado=solicitado
        self.onHand=onHand
        self.comprometido=comprometido
        self.disponible=disponible
        self.skuSolicitado=skuSolicitado
        self.skuOnHand=skuOnHand
        self.skuComprometido=skuComprometido
        self.fechaActualizacion=fechaActualizacion
