class ContenedorOla():
    
    def __init__(self, ola, semana, pedidos, contenedores, pickingPending, inPicking, packingPending, inPacking, stagingPending, loadingPending, shipConfirmPending, loadConfirmPending, closed, carton, bolsa):
        self.ola=ola
        self.semana= semana
        self.pedidos= pedidos
        self.contenedores= contenedores
        self.pickingPending= pickingPending
        self.inPicking= inPicking
        self.packingPending= packingPending
        self.inPacking= inPacking
        self.stagingPending= stagingPending
        self.loadingPending= loadingPending
        self.shipConfirmPending= shipConfirmPending
        self.loadConfirmPending= loadConfirmPending
        self.closed= closed
        self.carton= carton
        self.bolsa= bolsa
