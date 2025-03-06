from datetime import datetime

class Planeacion():
    
    def __init__(self, subindice, unidad, orden, numTienda, tienda, tipo, ola, id, cantidadSolicitada, volumen, numeroContenedores, diaCarga, diaArribo, horaArribo, inicioDescarga, finDescarga, finProcesoAdmin):
        self.subindice=subindice
        self.unidad=unidad
        self.orden=orden
        self.numTienda=numTienda
        self.tienda=tienda
        self.tipo=tipo
        self.ola=ola
        self.id=id
        self.cantidadSolicitada=cantidadSolicitada
        self.volumen=volumen
        self.numeroContenedores=numeroContenedores
        self.diaCarga=diaCarga
        self.diaArribo=diaArribo
        self.horaArribo=horaArribo
        self.inicioDescarga=inicioDescarga
        self.finDescarga=finDescarga
        self.finProcesoAdmin=finProcesoAdmin
