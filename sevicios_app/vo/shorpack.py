class Shorpack():
    
    def __init__(self,pickWaveCode,clientCode,productCode,documentCode,request_qty,total_qty,rechazadas,pzasFaltantes,fecha):
        self.pickWaveCode = pickWaveCode
        self.clientCode = clientCode
        self.productCode = productCode
        self.documentCode = documentCode
        self.request_qty = request_qty
        self.total_qty = total_qty
        self.rechazadas = rechazadas
        self.pzasFaltantes = pzasFaltantes
        self.fecha = fecha