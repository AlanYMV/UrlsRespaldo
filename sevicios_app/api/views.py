import logging
import io
import xlsxwriter
from hdbcli import dbapi
from datetime import date, datetime
from rest_framework.response import Response
from django.http.response import HttpResponse
from rest_framework.decorators import api_view
from rest_framework import status
from sevicios_app.vo.pedido import Pedido
from sevicios_app.api.dao.WmsDao import WMSDao
from sevicios_app.api.dao.SapDao import SAPDao
from sevicios_app.api.dao.SapDaoII import SAPDaoII
from sevicios_app.api.dao.SapDaoCl import SAPDaoCl
from sevicios_app.api.dao.SapDaoClII import SAPDaoClII
from sevicios_app.api.dao.PlaneacionDao import PlaneacionDao
from sevicios_app.api.dao.MonitoreoDao import MonitoreoDao
from sevicios_app.api.dao.ScaleIntDao import ScaleIntDao
from sevicios_app.api.dao.RecepcionTiendaDao import RecepcionTiendaDao
from sevicios_app.api.dao.RecepcionTiendaDaoCl import RecepcionTiendaDaoCl
from sevicios_app.api.dao.ScaleIntChileDao import ScaleIntChileDao
from sevicios_app.api.dao.TraficoDao import TraficoDao
from sevicios_app.api.dao.WmsCLDao import WMSCLDao
from sevicios_app.api.dao.WmsCOLDao import WMSCOLDao
from sevicios_app.api.serializers import *
from django.http import JsonResponse, FileResponse
from sevicios_app.api.dao.SapDaoCol import *
from sevicios_app.api.dao.SapDaoII import *
from sevicios_app.api.dao.ScaleIntColDao import ScaleIntColDao
from sevicios_app.api.dao.WmsDaoQA import *
from sevicios_app.api.dao.ContainerService import ContainerService
from sevicios_app.api.dao.ContainerServiceCl import ContainerServiceCl
import pandas as pd
from django.views.decorators.csrf import csrf_exempt
from sevicios_app.api.dao.SapDaoMx import SapDaoMx
import time
from sevicios_app.api.dao.UpdatePromotion import UpdatePromotion
from io import BytesIO
from sevicios_app.api.dao.ContainerServiceCol import ContainerServiceCol

logger = logging.getLogger('')

@api_view(['GET'])
def pedidos_list(request):
    try:
        wmsDao=WMSDao()
        pedidosList=wmsDao.getPedidos()
        serializer=PedidoSerializer(pedidosList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def cargas_list(request):
    try:
        wmsDao=WMSDao()
        cargasList=wmsDao.getCargas()

        serializer=CargaSerializer(cargasList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def articulosList(request):
    try:
        sapDao=SAPDao()
        articulosList=sapDao.getArticulos('100')

        serializer=ArticuloSerializer(articulosList, many=True)
        return Response(serializer.data)

    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def pedido_detalle(request, idPedido):
    try:
        pedidos=idPedido.split(',')
        indice =True
        busqueda=''
        for pedido in pedidos:
            if(indice==False):
                busqueda=busqueda + ','
            busqueda=busqueda+'\'\''+pedido.strip()+'\'\''
            if(indice):
                indice=False
        sapDao=SAPDao()
        detallesPedidoList=sapDao.getPedido(busqueda, '100')

        serializer=DetallePedidoSerializer(detallesPedidoList, many=True)
        return Response(serializer.data)
        return Response()
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def carga_detalle(request, idCarga):
    try:
        cargas=idCarga.split(',')
        indice =True
        busqueda=''
        for carga in cargas:
            if(indice==False):
                busqueda=busqueda + ','
            busqueda=busqueda+"'"+carga.strip()+"'"
            if(indice):
                indice=False
        wmsDao=WMSDao()
        detallesCargaList=wmsDao.getCarga(busqueda, '100')

        serializer=DetalleCargaSerializer(detallesCargaList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def descargaPedido(request, idPedido):
    try:
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)

        nombreArchivo=''
        pedidos=idPedido.split(',')
        indice =True
        busqueda=''
        for pedido in pedidos:
            if(indice==False):
                nombreArchivo=nombreArchivo+'-'
                busqueda=busqueda + ','
            nombreArchivo=nombreArchivo+pedido.strip()
            busqueda=busqueda+'\'\''+pedido.strip()+'\'\''
            if(indice):
                indice=False
        sapDao=SAPDao()
        detallesPedidoList=sapDao.getPedido(busqueda, '')
        tiendaActual=''
        for detallePedido in detallesPedidoList:
            if tiendaActual != detallePedido.tienda:
                worksheet = workbook.add_worksheet(detallePedido.tienda)
                worksheet.write(0, 0, 'Cantidad')
                worksheet.write(0, 1, 'IdUnidadEmbalaje')
                worksheet.write(0, 2, 'DescripcionMaterialCarga')
                worksheet.write(0, 3, 'PesoArticulo')
                worksheet.write(0, 4, 'IdUnidadPeso')
                worksheet.write(0, 5, 'ClaveProductoServicio')
                worksheet.write(0, 6, 'ClaveUnidadMedidaEmbalaje')
                worksheet.write(0, 7, 'ClaveUnidad')
                worksheet.write(0, 8, 'MaterialPeligroso')
                worksheet.write(0, 9, 'Pedido')
                worksheet.write(0, 10, 'Tienda')
                worksheet.write(0, 11, 'NombreTienda')
                worksheet.write(0, 12, 'CodigoPostal')
                worksheet.write(0, 13, 'Pais')
                worksheet.write(0, 14, 'Estado')
                worksheet.write(0, 15, 'Direccion')
                worksheet.write(0, 16, 'UnidadMedida')
                worksheet.write(0, 17, 'IdOrigen')
                worksheet.write(0, 18, 'IdDestino')
                worksheet.write(0, 19, 'Volumen')
                worksheet.write(0, 20, 'UnidadVolumen')
                tiendaActual = detallePedido.tienda
                row=1

            worksheet.write(row, 0, detallePedido.cantidad)
            worksheet.write(row, 1, detallePedido.idUnidadEmbalaje)
            worksheet.write(row, 2, detallePedido.descripcionMaterialCarga)
            worksheet.write(row, 3, detallePedido.pesoArticulo)
            worksheet.write(row, 4, detallePedido.idUnidadPeso)
            worksheet.write(row, 5, detallePedido.claveProductoServicio)
            worksheet.write(row, 6, detallePedido.claveUnidadMedidaEmbalaje)
            worksheet.write(row, 7, detallePedido.claveUnidad)
            worksheet.write(row, 8, detallePedido.materialPeligroso)
            worksheet.write(row, 9, detallePedido.pedido)
            worksheet.write(row, 10, detallePedido.tienda)
            worksheet.write(row, 11, detallePedido.nombreTienda)
            worksheet.write(row, 12, detallePedido.codigoPostal)
            worksheet.write(row, 13, detallePedido.pais)
            worksheet.write(row, 14, detallePedido.estado)
            worksheet.write(row, 15, detallePedido.direccion)
            worksheet.write(row, 16, detallePedido.unidadMedida)
            worksheet.write(row, 17, detallePedido.idOrigen)
            worksheet.write(row, 18, detallePedido.idDestino)
            worksheet.write(row, 19, detallePedido.volumen)
            worksheet.write(row, 20, detallePedido.unidadVolumen)
            row=row+1
        workbook.close()

        output.seek(0)

        filename = f'pedido{nombreArchivo}.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def descargaCarga(request, idCarga):
    try:
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        
        nombreArchivo=''
        cargas=idCarga.split(',')
        indice =True
        busqueda=''
        for carga in cargas:
            if(indice==False):
                nombreArchivo=nombreArchivo+'-'
                busqueda=busqueda + ','
            nombreArchivo=nombreArchivo+carga.strip()
            busqueda=busqueda+"'"+carga.strip()+"'"
            if(indice):
                indice=False
       
        wmsDao=WMSDao()
        detallesCargaList=wmsDao.getCarga(busqueda, '')
        tiendaActual=''
        row=1
        for detalleCarga in detallesCargaList:
            if tiendaActual != detalleCarga.tienda:
                worksheet = workbook.add_worksheet(detalleCarga.tienda)
                worksheet.write(0, 0, 'Cantidad')
                worksheet.write(0, 1, 'IdUnidadEmbalaje')
                worksheet.write(0, 2, 'DescripcionMaterialCarga')
                worksheet.write(0, 3, 'PesoArticulo')
                worksheet.write(0, 4, 'IdUnidadPeso')
                worksheet.write(0, 5, 'ClaveProductoServicio')
                worksheet.write(0, 6, 'ClaveUnidadMedidaEmbalaje')
                worksheet.write(0, 7, 'ClaveUnidad')
                worksheet.write(0, 8, 'MaterialPeligroso')
                worksheet.write(0, 9, 'Pedido')
                worksheet.write(0, 10, 'Tienda')
                worksheet.write(0, 11, 'NombreTienda')
                worksheet.write(0, 12, 'CodigoPostal')
                worksheet.write(0, 13, 'Pais')
                worksheet.write(0, 14, 'Estado')
                worksheet.write(0, 15, 'Direccion')
                worksheet.write(0, 16, 'UnidadMedida')
                worksheet.write(0, 17, 'IdOrigen')
                worksheet.write(0, 18, 'IdDestino')
                worksheet.write(0, 19, 'Volumen')
                worksheet.write(0, 20, 'UnidadVolumen')
                tiendaActual = detalleCarga.tienda
                row=1
            
            worksheet.write(row, 0, detalleCarga.cantidad)
            worksheet.write(row, 1, detalleCarga.idUnidadEmbalaje)
            worksheet.write(row, 2, detalleCarga.descripcionMaterialCarga)
            worksheet.write(row, 3, detalleCarga.pesoArticulo)
            worksheet.write(row, 4, detalleCarga.idUnidadPeso)
            worksheet.write(row, 5, detalleCarga.claveProductoServicio)
            worksheet.write(row, 6, detalleCarga.claveUnidadMedidaEmbalaje)
            worksheet.write(row, 7, detalleCarga.claveUnidad)
            worksheet.write(row, 8, detalleCarga.materialPeligroso)
            worksheet.write(row, 9, detalleCarga.pedido)
            worksheet.write(row, 10, detalleCarga.tienda)
            worksheet.write(row, 11, detalleCarga.nombreTienda)
            worksheet.write(row, 12, detalleCarga.codigoPostal)
            worksheet.write(row, 13, detalleCarga.pais)
            worksheet.write(row, 14, detalleCarga.estado)
            worksheet.write(row, 15, detalleCarga.direccion)
            worksheet.write(row, 16, detalleCarga.unidadMedida)
            worksheet.write(row, 17, detalleCarga.idOrigen)
            worksheet.write(row, 18, detalleCarga.idDestino)
            worksheet.write(row, 19, detalleCarga.volumen)
            worksheet.write(row, 20, detalleCarga.unidadVolumen)
            row=row+1
        workbook.close()

        output.seek(0)

        filename = f'carga{nombreArchivo}.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def descargaArticulos(request):
    try:
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'SKU')
        worksheet.write(0, 1, 'Descripción Sku')
        worksheet.write(0, 2, 'Codigo Sat')
        worksheet.write(0, 3, 'Codigo Proveedor')
        worksheet.write(0, 4, 'Proveedor')
        worksheet.write(0, 5, 'Unidad de Medida Compra')
        worksheet.write(0, 6, 'Clave Unidad')
        worksheet.write(0, 7, 'Peso Articulo')
        worksheet.write(0, 8, 'Items Unidad de Compra')
        worksheet.write(0, 9, 'Cantidad X Paquete')
        sapDao=SAPDao()
        articulosList=sapDao.getArticulos('')

        row=1
        for articulo in articulosList:
            worksheet.write(row, 0, articulo.sku)
            worksheet.write(row, 1, articulo.descripcionSku)
            worksheet.write(row, 2, articulo.codigoSat)
            worksheet.write(row, 3, articulo.codigoProveedor)
            worksheet.write(row, 4, articulo.proveedor)
            worksheet.write(row, 5, articulo.unidadMedidaCompra)
            worksheet.write(row, 6, articulo.claveUnidad)
            worksheet.write(row, 7, articulo.pesoArticulo)
            worksheet.write(row, 8, articulo.itemsUnidadCompra)
            worksheet.write(row, 9, articulo.cantidadPaquete)
            row=row+1
        workbook.close()

        output.seek(0)

        filename = 'articulos.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def storagesTemplates(request):
    try:
        sapDao=SapDaoMx()
        storagesTemplatesList=sapDao.getStorageTemplates('100')
        serializer=StorageTemplateSerializer(storagesTemplatesList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def actualizarCodigosSat(request):
    try:
        sapDao=SAPDao()
        codigosSatList=sapDao.getCodigosSAT()
        wmsDao=WMSDao()
        wmsDao.updateCodigoSat(codigosSatList)
        return Response()
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def precios(request):
    try:
        sapDao=SAPDaoII()
        preciosList=sapDao.getPrecios('100')

        serializer=PreciosSerializer(preciosList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def descargaPrecios(request):
    try:
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'SKU')
        worksheet.write(0, 1, 'Codigos de Barras')
        worksheet.write(0, 2, 'Categoria')
        worksheet.write(0, 3, 'Subcategoria')
        worksheet.write(0, 4, 'Clase')
        worksheet.write(0, 5, 'Descripción')
        worksheet.write(0, 6, 'Storage Template')
        worksheet.write(0, 7, 'Storage Template User')
        worksheet.write(0, 8, 'Licencia')
        worksheet.write(0, 9, 'Height')
        worksheet.write(0, 10, 'Width')
        worksheet.write(0, 11, 'Length')
        worksheet.write(0, 12, 'Volume')
        worksheet.write(0, 13, 'Weight')
        worksheet.write(0, 14, 'Estandar')
        worksheet.write(0, 15, 'Estandar Sin Iva')
        worksheet.write(0, 16, 'Aeropuertos CDMX')
        worksheet.write(0, 17, 'Aeropuertos Foraneos')
        worksheet.write(0, 18, 'Aeropuertos Fronterisos')
        worksheet.write(0, 19, 'Outlet')
        worksheet.write(0, 20, 'Frontera')
        worksheet.write(0, 21, 'Proveedor')
        sapDao=SAPDaoII()
        preciosList=sapDao.getPrecios('')

        row=1
        for precio in preciosList:
            worksheet.write(row, 0, precio.itemCode)
            worksheet.write(row, 1, precio.codigoBarras)
            worksheet.write(row, 2, precio.categoria)
            worksheet.write(row, 3, precio.subcategoria)
            worksheet.write(row, 4, precio.clase)
            worksheet.write(row, 5, precio.itemName)
            worksheet.write(row, 6, precio.storageTemplate)
            worksheet.write(row, 7, precio.stUsr)
            worksheet.write(row, 8, precio.licencia)
            worksheet.write(row, 9, precio.height)
            worksheet.write(row, 10, precio.width)
            worksheet.write(row, 11, precio.length)
            worksheet.write(row, 12, precio.volume)
            worksheet.write(row, 13, precio.weight)
            worksheet.write(row, 14, precio.fvEstandar)
            worksheet.write(row, 15, precio.fvEstandarSI)
            worksheet.write(row, 16, precio.fvAptosCdmx)
            worksheet.write(row, 17, precio.fvAptosForaneos)
            worksheet.write(row, 18, precio.fvAptosFronterizos)
            worksheet.write(row, 19, precio.fvOutlet)
            worksheet.write(row, 20, precio.fvFrontera)
            worksheet.write(row, 21, precio.proveedor)
            row=row+1
        workbook.close()

        output.seek(0)

        filename = 'Precios.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def descargaStorages(request):
    try:
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'SKU')
        worksheet.write(0, 1, 'Storage Template')
        worksheet.write(0, 2, 'Grupo Logistico')
        worksheet.write(0, 3, 'Unidad')
        worksheet.write(0, 4, 'Familia')
        worksheet.write(0, 5, 'Subfamilia')
        worksheet.write(0, 6, 'Subsubfamilia')
        worksheet.write(0, 7, 'uSysCat4')
        worksheet.write(0, 8, 'uSysCat5')
        worksheet.write(0, 9, 'uSysCat6')
        worksheet.write(0, 10, 'uSysCat7')
        worksheet.write(0, 11, 'uSysCat8')
        worksheet.write(0, 12, 'Height')
        worksheet.write(0, 13, 'Width')
        worksheet.write(0, 14, 'Length')
        worksheet.write(0, 15, 'Volume')
        worksheet.write(0, 16, 'Weight')
        sapDao=SapDaoMx()
        storagesTemplatesList=sapDao.getStorageTemplates('')

        row=1
        for storageTemplate in storagesTemplatesList:
            worksheet.write(row, 0, storageTemplate.itemCode)
            worksheet.write(row, 1, storageTemplate.storageTemplate)
            worksheet.write(row, 2, storageTemplate.grupoLogistico)
            worksheet.write(row, 3, storageTemplate.salUnitMsr)
            worksheet.write(row, 4, storageTemplate.familia)
            worksheet.write(row, 5, storageTemplate.subFamilia)
            worksheet.write(row, 6, storageTemplate.subSubFamilia)
            worksheet.write(row, 7, storageTemplate.uSysCat4)
            worksheet.write(row, 8, storageTemplate.uSysCat5)
            worksheet.write(row, 9, storageTemplate.uSysCat6)
            worksheet.write(row, 10, storageTemplate.uSysCat7)
            worksheet.write(row, 11, storageTemplate.uSysCat8)
            worksheet.write(row, 12, storageTemplate.height)
            worksheet.write(row, 13, storageTemplate.width)
            worksheet.write(row, 14, storageTemplate.length)
            worksheet.write(row, 15, storageTemplate.volume)
            worksheet.write(row, 16, storageTemplate.weight)
            row=row+1
        workbook.close()

        output.seek(0)

        filename = 'StorageTemplate.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def inventarioWmsErp(request):
    try:
        scaleIntDao=ScaleIntDao()
        wmsErpList=scaleIntDao.getWmsErp()
        serializer=InventarioWmsErpSerializer(wmsErpList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def inventarioItems(request):
    try:
        print('inventarioItems')
        scaleIntDao=ScaleIntDao()
        itemsList=scaleIntDao.getItems()
        serializer=InventarioItemSerializer(itemsList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def inventarioWms(request):
    try:
        monitoreoDao=MonitoreoDao()
        inventariosWmsList=monitoreoDao.getInventarioWms()
        serializer=InventarioWmsSerializer(inventariosWmsList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def topOneHundred(request):
    try:
        scaleIntDao=ScaleIntDao()
        TopList=scaleIntDao.getTopOneHundred(True)
        serializer=InventarioTopSerializer(TopList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def inventarioDetalleErpWms(request, item):
    try:
        scaleIntDao=ScaleIntDao()
        detallesErpWmsList=scaleIntDao.getInventarioDetalleErpWms(item)
        serializer=InventarioDetalleErpWmsSerializer(detallesErpWmsList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def getPlaneacion(request):
    try:
        planeacionDao=PlaneacionDao()
        planeacionList=planeacionDao.getPlaneacion()
        serializer=PlaneacionSerializer(planeacionList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getReciboPendientes(request):
    try:
        wmsDao=WMSDao()
        recibosPendientesList=wmsDao.getRecibosPendientes()
        serializer=ReciboPendienteSerializer(recibosPendientesList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def decargaReciboPendientes(request):
    try:
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'Identificador de Recibo')
        worksheet.write(0, 1, 'item')
        worksheet.write(0, 2, 'Descripción de Item')
        worksheet.write(0, 3, 'Cantidad Total')
        worksheet.write(0, 4, 'Cantidad Abierta')
        wmsDao=WMSDao()
        recibosPendientesList=wmsDao.getRecibosPendientes()

        row=1
        for reciboPendiente in recibosPendientesList:
            worksheet.write(row, 0, reciboPendiente.receiptId)
            worksheet.write(row, 1, reciboPendiente.item)
            worksheet.write(row, 2, reciboPendiente.itemDesc)
            worksheet.write(row, 3, reciboPendiente.totalQty)
            worksheet.write(row, 4, reciboPendiente.openQty)
            row=row+1
        workbook.close()

        output.seek(0)

        filename = 'RecibosPendientes.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getSplits(request, fecha):
    try:
        wmsDao=WMSDao()
        splitsList=wmsDao.getSplitsByFecha(fecha)
        serializer=SplitSerializer(splitsList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def executeEnvioCorreos(request, tienda):
    try:
        recepcionTiendaDao=RecepcionTiendaDao()
        recepcionTiendaDao.executeEnvioCorreos(tienda)
        return Response(status=status.HTTP_200_OK)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def executeActualizarTablasTiendas(request):
    try:
        logger.error("Llamado actualización de tablas view")
        recepcionTiendaDao=RecepcionTiendaDao()
        recepcionTiendaDao.executeActualizarTablasTiendas()
        return Response(status=status.HTTP_200_OK)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getTiendasPendientes(request):
    try:
        recepcionTiendaDao=RecepcionTiendaDao()
        tiendasPendientesList=recepcionTiendaDao.getTiendasPendientes()
        serializer=TiendaPendienteSerializer(tiendasPendientesList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getTiendasPendientesAll(request):
    try:
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'SOLICITUD ESTATUS')
        worksheet.write(0, 1, 'CARGA')
        worksheet.write(0, 2, 'PEDIDO')
        worksheet.write(0, 3, 'NOMBRE ALMACEN')
        worksheet.write(0, 4, 'FECHA EMBARQUE')
        worksheet.write(0, 5, 'FECHA PLANEADA')
        worksheet.write(0, 6, 'TRANSITO')
        worksheet.write(0, 7, 'CROSS DOCK')
        worksheet.write(0, 8, 'FECHA ENTREGA')
        
        recepcionTiendaDao=RecepcionTiendaDao()
        tiendasPendientesList=recepcionTiendaDao.getTiendasPendientesAll()
        
        row=1
        for tiendaPendiente in tiendasPendientesList:
            worksheet.write(row, 0, tiendaPendiente.solicitudEstatus)
            worksheet.write(row, 1, tiendaPendiente.carga)
            worksheet.write(row, 2, tiendaPendiente.pedido)
            worksheet.write(row, 3, tiendaPendiente.nombreAlmacen)
            worksheet.write(row, 4, tiendaPendiente.fechaEmbarque)
            worksheet.write(row, 5, tiendaPendiente.fechaPlaneada)
            worksheet.write(row, 6, tiendaPendiente.transito)
            worksheet.write(row, 7, tiendaPendiente.crossDock)
            worksheet.write(row, 8, tiendaPendiente.fechaEntrega)
            row=row+1
        workbook.close()

        output.seek(0)

        filename = 'tiendasPendientes.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getTiendasPendientesCorreo(request):
    try:
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        
        worksheet.set_column('A:A', 20)
        worksheet.set_column('B:C', 10)
        worksheet.set_column('D:D', 40)
        worksheet.set_column('E:I', 16)
        
        cell_formatRed = workbook.add_format()
        cell_formatRed.set_bold()
        cell_formatRed.set_font_color('#C00000')
        
        worksheet.write(0, 1, 'TIENDAS PENDIENTES DE CONFIRMAR', cell_formatRed)
        worksheet.write(0, 9, 'PEND', cell_formatRed)
        worksheet.write(1, 9, 'Σ')

        cell_formatEncabezado = workbook.add_format()
        cell_formatEncabezado.set_bg_color('#76933C')
        cell_formatEncabezado.set_font_color('#FFFFFF')
        worksheet.write(2, 0, 'SOLICITUD ESTATUS', cell_formatEncabezado)
        worksheet.write(2, 1, 'CARGA', cell_formatEncabezado)
        worksheet.write(2, 2, 'PEDIDO', cell_formatEncabezado)
        worksheet.write(2, 3, 'NOMBRE ALMACEN', cell_formatEncabezado)
        worksheet.write(2, 4, 'FECHA EMBARQUE', cell_formatEncabezado)
        worksheet.write(2, 5, 'FECHA PLANEADA', cell_formatEncabezado) 
        worksheet.write(2, 6, 'TRANSITO', cell_formatEncabezado)
        worksheet.write(2, 7, 'CROSS DOCK', cell_formatEncabezado)
        worksheet.write(2, 8, 'FECHA ENTREGA', cell_formatEncabezado)
        
        recepcionTiendaDao=RecepcionTiendaDao()
        tiendasPendientesList=recepcionTiendaDao.getTiendasPendientesCorreo()
        piezasTransito=0
        piezasCrossDock=0
        pedidosTransito=0
        pedidosCrossDock=0

        cell_formatSolRec = workbook.add_format()
        cell_formatSolRec.set_bg_color('#FCD5B4')
        cell_formatSolRec.set_num_format('#,##0')

        cell_formatSolTra = workbook.add_format()
        cell_formatSolTra.set_bg_color('#A9D08E')

        cell_formatCarPedFec = workbook.add_format()
        cell_formatCarPedFec.set_bg_color('#C6E0B4')
        cell_formatCarPedFec.set_align('right')

        cell_formatFec = workbook.add_format()
        cell_formatFec.set_align('right')

        cell_formatNumTra = workbook.add_format()
        cell_formatNumTra.set_bg_color('#B8CCE4')
        cell_formatNumTra.set_num_format('#,##0')
        
        row=3
        for tiendaPendiente in tiendasPendientesList:
            if tiendaPendiente.solicitudEstatus=='REC PARCIAL':
                worksheet.write(row, 0, tiendaPendiente.solicitudEstatus, cell_formatSolRec)    
                worksheet.write(row, 6, tiendaPendiente.transito, cell_formatSolRec)
                worksheet.write(row, 7, tiendaPendiente.crossDock, cell_formatSolRec)
            else:
                worksheet.write(row, 0, tiendaPendiente.solicitudEstatus, cell_formatSolTra)    
                worksheet.write(row, 6, tiendaPendiente.transito, cell_formatNumTra)
                worksheet.write(row, 7, tiendaPendiente.crossDock, cell_formatNumTra)
                
            worksheet.write(row, 1, tiendaPendiente.carga, cell_formatCarPedFec)
            worksheet.write(row, 2, tiendaPendiente.pedido, cell_formatCarPedFec)
            worksheet.write(row, 3, tiendaPendiente.nombreAlmacen)
            worksheet.write(row, 4, tiendaPendiente.fechaEmbarque, cell_formatCarPedFec)
            worksheet.write(row, 5, tiendaPendiente.fechaPlaneada, cell_formatFec)
            worksheet.write(row, 8, tiendaPendiente.fechaEntrega, cell_formatFec)

            if tiendaPendiente.transito>0:
                piezasTransito=piezasTransito+tiendaPendiente.transito
                pedidosTransito=pedidosTransito+1

            if tiendaPendiente.crossDock>0:
                piezasCrossDock=piezasCrossDock+tiendaPendiente.crossDock
                pedidosCrossDock=pedidosCrossDock+1
            
            row=row+1
            
        cell_formatNum = workbook.add_format()
        cell_formatNum.set_num_format('#,##0')

        worksheet.write(0, 6, pedidosTransito)
        worksheet.write(0, 7, pedidosCrossDock)
        worksheet.write(0, 8, pedidosTransito+pedidosCrossDock)
        worksheet.write(1, 6, piezasTransito, cell_formatNum)
        worksheet.write(1, 7, piezasCrossDock, cell_formatNum)
        worksheet.write(1, 8, piezasTransito+piezasCrossDock, cell_formatNum)
        workbook.close()

        output.seek(0)

        filename = 'tiendasPendientesCorreo.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
def getPedidoTienda(request, tienda, carga, pedido):
    try:
        tienda=tienda.strip()
        carga=carga.strip()
        pedido=pedido.strip()
        if tienda=='' and carga=='' and pedido =='':
            return Response({'Debe ingresar al menos un parametro'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)    
        recepcionTiendaDao=RecepcionTiendaDao()
        pedidoList=recepcionTiendaDao.getPedido(tienda, carga, pedido)
        serializer=PedidoTiendaSerializer(pedidoList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getDetallePedidoTienda(request, tienda, carga, pedido, contenedor, articulo, estatusContenedor):
    try:
        tienda=tienda.strip()
        carga=carga.strip()
        pedido=pedido.strip()
        contenedor=contenedor.strip()
        articulo=articulo.strip()
        estatusContenedor=estatusContenedor.strip()
        if tienda=='' and carga=='' and pedido =='' and contenedor=='' and articulo=='':
            return Response({'Debe ingresar al menos un parametro'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)    
        recepcionTiendaDao=RecepcionTiendaDao()
        detallePedidoList=recepcionTiendaDao.getDetallePedido(tienda, carga, pedido, contenedor, articulo, estatusContenedor)
        serializer=DetallePedidoTiendaSerializer(detallePedidoList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getSolicitudTraslado(request, documento, origenSolicitud, destinoSolicitud, origenTraslado, destinoTraslado):
    try:
        logger.error("Acceso al servicio")
        documento=documento.strip()
        origenSolicitud=origenSolicitud.strip()
        destinoSolicitud=destinoSolicitud.strip()
        origenTraslado=origenTraslado.strip()
        destinoTraslado=destinoTraslado.strip()
        if documento=='' and origenSolicitud=='' and destinoSolicitud =='' and origenTraslado=='' and destinoTraslado=='':
            return Response({'Debe ingresar al menos un parametro'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)    
        recepcionTiendaDao=RecepcionTiendaDao()
        solicitudTrasladoList=recepcionTiendaDao.getSolicitudTraslado(documento, origenSolicitud, destinoSolicitud, origenTraslado, destinoTraslado)
        serializer=SolicitudTrasladoSerializer(solicitudTrasladoList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getTablaTiendasPendientes(request, fecha):
    try:
        recepcionTiendaDao=RecepcionTiendaDao()
        tablaTiendasPendientesList=recepcionTiendaDao.getTablaTiendasPendientes(fecha)
        serializer=TablaTiendaPendienteSerializer(tablaTiendasPendientesList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getTiendasPendientesFecha(request):
    try:
        recepcionTiendaDao=RecepcionTiendaDao()
        tiendasPendientesFechaList=recepcionTiendaDao.getTiendasPendientesFecha()
        serializer=TiendaPendienteFechaSerializer(tiendasPendientesFechaList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def insertFechaTrafico(request, fecha, carga, pedido):
    try:
        logger.error('Inicio de insert')
        recepcionTiendaDao=RecepcionTiendaDao()
        recepcionTiendaDao.insertFechaTrafico(fecha, carga, pedido)
        return Response(status=status.HTTP_200_OK)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getPedidosPorCerrar(request):
    try:
        recepcionTiendaDao=RecepcionTiendaDao()
        pedidosPorCerrarList=recepcionTiendaDao.getPedidosPorCerrar()
        serializer=PedidoPorCerrarSerializer(pedidosPorCerrarList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def cerrarPedidos(request):
    try:
        recepcionTiendaDao=RecepcionTiendaDao()
        recepcionTiendaDao.cerrarPedidoPendientes()
        return Response(status=status.HTTP_200_OK)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getPedidosSinTr(request):
    try:
        recepcionTiendaDao=RecepcionTiendaDao()
        pedidosSinTrList=recepcionTiendaDao.getPedidosSinTr()
        sapDao=SAPDao()
        infoPedidosSinTrList=sapDao.getInfoTransaccionTR(pedidosSinTrList)
        serializer=InfoPedidoSinTrSerializer(infoPedidosSinTrList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def getDiferenciasFile(request):
    try:
        
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 2, 'Cerradas SAP, pendientes en APP')
        worksheet.write(1, 0, 'PEDIDO')
        worksheet.write(1, 1, 'ESTATUS PEDIDO')
        worksheet.write(1, 2, 'TIENDA')
        worksheet.write(1, 3, 'CARGA')
        worksheet.write(1, 4, 'DOCUMENTO')
        worksheet.write(1, 5, 'ESTATUS DOCUMENTO')
        
        recepcionTiendaDao=RecepcionTiendaDao()
        pedidosPorCerrarList=recepcionTiendaDao.getPedidosPorCerrar()
        
        row=2
        for pedidoPorCerrar in pedidosPorCerrarList:
            worksheet.write(row, 0, pedidoPorCerrar.pedido)
            worksheet.write(row, 1, pedidoPorCerrar.estatusPedido)
            worksheet.write(row, 2, pedidoPorCerrar.tienda)
            worksheet.write(row, 3, pedidoPorCerrar.carga)
            worksheet.write(row, 4, pedidoPorCerrar.documento)
            worksheet.write(row, 5, pedidoPorCerrar.estatusDocumento)
            row=row+1
        
        row=row+2
        worksheet.write(row, 2, 'Sin Solicitud TR')
        row=row+1
        worksheet.write(row, 0, 'PEDIDO')
        worksheet.write(row, 1, 'TIENDA')
        worksheet.write(row, 2, 'ALMACEN')
        worksheet.write(row, 3, 'BNEXT')
        worksheet.write(row, 4, 'DOCK ENTRY')

        recepcionTiendaDao=RecepcionTiendaDao()
        pedidosSinTrList=recepcionTiendaDao.getPedidosSinTr()
        sapDao=SAPDao()
        infoPedidosSinTrList=sapDao.getInfoTransaccionTR(pedidosSinTrList)

        row=row+1
        for infoPedidoSinTr in infoPedidosSinTrList:
            worksheet.write(row, 0, infoPedidoSinTr.pedido)
            worksheet.write(row, 1, infoPedidoSinTr.tienda)
            worksheet.write(row, 2, infoPedidoSinTr.almacen)
            worksheet.write(row, 3, infoPedidoSinTr.bnext)
            worksheet.write(row, 4, infoPedidoSinTr.dockEntry)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = f'Diferencias_{date.today()}.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
def getReciboSap(request, recibos):
    try:
        recibosSplit=recibos.split(',')
        indice =True
        busqueda=''
        for recibo in recibosSplit:
            if(indice==False):
                busqueda=busqueda + ","
            busqueda=busqueda+"'"+recibo.strip()+"'"
            if(indice):
                indice=False
        monitoreoDao=MonitoreoDao()
        logger.error("Entro a getReciboSap "+busqueda)
        recibosList=monitoreoDao.getReciboSap(busqueda)
        serializer=ReciboSapSerializer(recibosList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getReciboSapByValor(request, valor):
    try:
        monitoreoDao=MonitoreoDao()
        recibosList=monitoreoDao.getReciboSapByValor(valor)
        serializer=ReciboSapSerializer(recibosList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getPedidoSap(request, pedidos):
    try:
        pedidosSplit=pedidos.split(',')
        indice =True
        busqueda=''
        for pedido in pedidosSplit:
            if(indice==False):
                busqueda=busqueda + ","
            busqueda=busqueda+"'"+pedido.strip()+"'"
            if(indice):
                indice=False
        monitoreoDao=MonitoreoDao()
        pedidosList=monitoreoDao.getPedidoSap(busqueda)
        serializer=PedidoSapSerializer(pedidosList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getPedidoSapByValor(request, valor):
    try:
        monitoreoDao=MonitoreoDao()
        if valor == 'O':
            pedidosList=monitoreoDao.getPedidoSapAbiertos()
        else:
            pedidosList=monitoreoDao.getPedidoSapByValor(valor, True)
        serializer=PedidoSapSerializer(pedidosList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getPedidoSapByValorFile(request, valor):
    try:
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'PEDIDO')
        worksheet.write(0, 1, 'C REQ QTY')
        worksheet.write(0, 2, 'C REC QTY')
        worksheet.write(0, 3, 'O REQ QTY')
        worksheet.write(0, 4, 'O REC QTY')
        worksheet.write(0, 5, 'TOTAL REQ QTY')
        worksheet.write(0, 6, 'TOTAL REC QTY')
        worksheet.write(0, 7, 'WMS CLOSE')
        worksheet.write(0, 8, 'DIF')
        worksheet.write(0, 9, 'SHIP DATE')
        worksheet.write(0, 10, 'STS WMS')
        worksheet.write(0, 11, 'VAL')
        worksheet.write(0, 12, 'OPEN SAP')
        
        monitoreoDao=MonitoreoDao()
        if valor == 'O':
            pedidosList=monitoreoDao.getPedidoSapAbiertos()
        else:
            pedidosList=monitoreoDao.getPedidoSapByValor(valor, False)
        
        row=1
        for pedido in pedidosList:
            worksheet.write(row, 0, pedido.docSDT)
            worksheet.write(row, 1, pedido.closeQtySdt)
            worksheet.write(row, 2, pedido.closeQtyTr)
            worksheet.write(row, 3, pedido.openQtySdt)
            worksheet.write(row, 4, pedido.openQtyTr)
            worksheet.write(row, 5, pedido.totalQtySdt)
            worksheet.write(row, 6, pedido.totalQtyTr)
            worksheet.write(row, 7, pedido.wmsClose)
            worksheet.write(row, 8, pedido.dif)
            worksheet.write(row, 9, pedido.shipDate)
            worksheet.write(row, 10, pedido.stsWms)
            worksheet.write(row, 11, pedido.val)
            worksheet.write(row, 12, pedido.openSap)
            row=row+1
        workbook.close()

        output.seek(0)

        filename = f'DiferenciasPedidos.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def executeCuadraje(request):
    try:
        monitoreoDao=MonitoreoDao()
        monitoreoDao.executeCuadraje()
        return Response(status=status.HTTP_200_OK)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getCuadraje(request):
    try:
        monitoreoDao=MonitoreoDao()
        cuadrajesList=monitoreoDao.getDatosCuadraje()
        serializer=CuadrajeSerializer(cuadrajesList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getPendienteSemana(request):
    try:
        monitoreoDao=MonitoreoDao()
        pendientesSemanaList=monitoreoDao.getPendienteSemana()
        serializer=PendienteSemanaSerializer(pendientesSemanaList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def insertSapReceiptVal(request, idReceipt):
    try:
        monitoreoDao=MonitoreoDao()
        monitoreoDao.insertSapReceiptVal(idReceipt)
        return Response(status=status.HTTP_200_OK)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def insertSapShipmentVal(request, idShipment):
    try:
        monitoreoDao=MonitoreoDao()
        monitoreoDao.insertSapShipmentVal(idShipment)
        return Response(status=status.HTTP_200_OK)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getContenedoresEpqDay(request):
    try:
        wmsDao=WMSDao()
        contenedoresEpqList=wmsDao.getContenedoresEpqDay()
        serializer=ContenedorEpqSerializer(contenedoresEpqList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getContenedoresEpqAll(request):
    try:
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'CONTENEDOR')
        worksheet.write(0, 1, 'FECHA')
        worksheet.write(0, 2, 'PEDIDO')
        worksheet.write(0, 3, 'OLA')
        
        wmsDao=WMSDao()
        contenedoresEpqList=wmsDao.getContenedoresEpqAll()
        
        row=1
        for contenedorEpq in contenedoresEpqList:
            worksheet.write(row, 0, contenedorEpq.contenedor)
            worksheet.write(row, 1, contenedorEpq.activityDateTime)
            worksheet.write(row, 2, contenedorEpq.pedido)
            worksheet.write(row, 3, contenedorEpq.ola)
            row=row+1
        workbook.close()

        output.seek(0)

        filename = f'ContenedoresEpq{date.today()}.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def executeInvetarioCedis(request):
    try:
        monitoreoDao=MonitoreoDao()
        monitoreoDao.executeInvetarioCedis()
        return Response(status=status.HTTP_200_OK)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def getDetallePedidoTiendaFile(request, tienda, carga, pedido, contenedor, articulo, estatusContenedor):
    try:
        tienda=tienda.strip()
        carga=carga.strip()
        pedido=pedido.strip()
        contenedor=contenedor.strip()
        articulo=articulo.strip()
        estatusContenedor=estatusContenedor.strip()
        if tienda=='' and carga=='' and pedido =='' and contenedor=='' and articulo=='':
            return Response({'Debe ingresar al menos un parametro'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR) 
        
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'ESTATUS PEDIDO')
        worksheet.write(0, 1, 'TIENDA')
        worksheet.write(0, 2, 'CARGA')
        worksheet.write(0, 3, 'PEDIDO')
        worksheet.write(0, 4, 'CONTENEDOR')
        worksheet.write(0, 5, 'ITEM')
        worksheet.write(0, 6, 'DESCRIPCION ITEM')
        worksheet.write(0, 7, 'ESTATUS CONTENEDOR')
        worksheet.write(0, 8, 'PIEZAS')

        recepcionTiendaDao=RecepcionTiendaDao()
        detallePedidoList=recepcionTiendaDao.getDetallePedido(tienda, carga, pedido, contenedor, articulo, estatusContenedor)
        
        row=1
        for detallePedido in detallePedidoList:
            worksheet.write(row, 0, detallePedido.estatusPedido)
            worksheet.write(row, 1, detallePedido.tienda)
            worksheet.write(row, 2, detallePedido.carga)
            worksheet.write(row, 3, detallePedido.pedido)
            worksheet.write(row, 4, detallePedido.contenedor)
            worksheet.write(row, 5, detallePedido.item)
            worksheet.write(row, 6, detallePedido.itemDescripcion)
            worksheet.write(row, 7, detallePedido.estatusContenedor)
            worksheet.write(row, 8, detallePedido.piezas)
            row=row+1
        workbook.close()

        output.seek(0)

        filename = 'DetallePedido.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def executeUpdateDiferencias(request):
    try:
        monitoreoDao=MonitoreoDao()
        monitoreoDao.executeUpdateDiferencias()
        return Response(status=status.HTTP_200_OK)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getCuadrajeFile(request):
    try:
        monitoreoDao=MonitoreoDao()
        cuadrajesList=monitoreoDao.getDatosCuadraje()
        cuadraje=cuadrajesList[0]
        
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 1, 'RECIBO')
        worksheet.write(0, 2, 'PEDIDO')
        worksheet.write(1, 0, 'TOTAL DE FOLIOS')
        worksheet.write(1, 1, cuadraje.recibosTotal)
        worksheet.write(1, 2, cuadraje.pedidosTotal)
        worksheet.write(2, 0, 'FOLIOS OK')
        worksheet.write(2, 1, cuadraje.recibosOk)
        worksheet.write(2, 2, cuadraje.pedidosOk)
        worksheet.write(3, 0, 'FOLIOS CON DIFERENCIA EN CANTIDAD')
        worksheet.write(3, 1, cuadraje.recibosQty)
        worksheet.write(3, 2, cuadraje.pedidosQty)
        worksheet.write(4, 0, 'FOLIOS PENDINETES DE CERRAR EN ERP')
        worksheet.write(4, 1, cuadraje.recibosCloseErp)
        worksheet.write(4, 2, cuadraje.pedidosCloseErp)
        worksheet.write(5, 0, 'FOLIOS PENDIENTES DE CERRAR EN ERP Y WMS')
        worksheet.write(5, 1, cuadraje.recibosRev)
        worksheet.write(5, 2, cuadraje.pedidosRev)
        
        pendientesSemanaList=monitoreoDao.getPendienteSemana()
        
        worksheet.write(7, 0, 'MES-AÑO')
        worksheet.write(7, 1, 'SEMANA')
        worksheet.write(7, 2, 'NUMERO')
        worksheet.write(7, 3, 'TIENDAS')
        worksheet.write(7, 4, 'RE-ETIQUETADO')
        row=8
        for pendienteSemana in pendientesSemanaList:
            worksheet.write(row, 0, pendienteSemana.fecha)
            worksheet.write(row, 1, pendienteSemana.shipDate)
            worksheet.write(row, 2, pendienteSemana.numeroRegistros)
            worksheet.write(row, 3, pendienteSemana.piezas)
            worksheet.write(row, 4, pendienteSemana.reetiquetado)
            row=row+1
        worksheet.write(row, 1, 'PEDIDOS ABIERTOS SOLO EN ERP')
        worksheet.write(row, 2, cuadraje.pedidosAbiertos)
        worksheet.write(row, 3, cuadraje.pedidosAbiertosNum)    
        
        workbook.close()

        output.seek(0)

        filename = 'Cuadraje.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def inventarioFile(request):
    try:
        scaleIntDao=ScaleIntDao()
        monitoreoDao=MonitoreoDao()
        
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'WAREHOUSE')
        worksheet.write(0, 1, 'WMS ONHAND')
        worksheet.write(0, 2, 'ERP ONHAND')
        worksheet.write(0, 3, 'DIFERENCIA')
        worksheet.write(0, 4, 'DIFERENCIA ABS')
        worksheet.write(0, 5, 'WMS INTRANSIT')
        worksheet.write(0, 6, '#ITEMS WMS')
        worksheet.write(0, 7, '#ITEMS ERP')
        worksheet.write(0, 8, '#ITEMS DIF')
        
        wmsErpList=scaleIntDao.getWmsErp()
        row=1
        for wmsErp in wmsErpList:
            worksheet.write(row, 0, wmsErp.warehouse)
            worksheet.write(row, 1, wmsErp.wmsOnHand)
            worksheet.write(row, 2, wmsErp.erpOnHand)
            worksheet.write(row, 3, wmsErp.diferencia)
            worksheet.write(row, 4, wmsErp.diferenciaAbsoluta)
            worksheet.write(row, 5, wmsErp.wmsInTransit)
            worksheet.write(row, 6, wmsErp.numItemsWms)
            worksheet.write(row, 7, wmsErp.numItemsErp)
            worksheet.write(row, 8, wmsErp.numItemsDif)
            row=row+1

        row=row+1
        worksheet.write(row, 0, 'WAREHOUSE')
        worksheet.write(row, 1, 'WMS ONHAND')
        worksheet.write(row, 2, 'ERP ONHAND')
        worksheet.write(row, 3, '#ITEMS')
        
        itemsList=scaleIntDao.getItems()
        row=row+1
        for item in itemsList:
            worksheet.write(row, 0, item.warehouse)
            worksheet.write(row, 1, item.wmsOnHand)
            worksheet.write(row, 2, item.erpOnHand)
            worksheet.write(row, 3, item.numItems)
            row=row+1
        
        row=row+1
        worksheet.write(row, 0, 'WAREHOUSE CODE')
        worksheet.write(row, 1, 'Solicitado')
        worksheet.write(row, 2, 'OnHand')
        worksheet.write(row, 3, 'Comprometido')
        worksheet.write(row, 4, 'Disponible')
        worksheet.write(row, 5, 'SKU SOL')
        worksheet.write(row, 6, 'SKU OHD')
        worksheet.write(row, 7, 'SKU CMP')
        worksheet.write(row, 8, 'Fecha Actualizacion')
        
        inventariosWmsList=monitoreoDao.getInventarioWms()
        row=row+1
        for inventarioWms in inventariosWmsList:
            worksheet.write(row, 0, inventarioWms.warehouseCode)
            worksheet.write(row, 1, inventarioWms.solicitado)
            worksheet.write(row, 2, inventarioWms.onHand)
            worksheet.write(row, 3, inventarioWms.comprometido)
            worksheet.write(row, 4, inventarioWms.disponible)
            worksheet.write(row, 5, inventarioWms.skuSolicitado)
            worksheet.write(row, 6, inventarioWms.skuOnHand)
            worksheet.write(row, 7, inventarioWms.skuComprometido)
            worksheet.write(row, 8, inventarioWms.fechaActualizacion)
            row=row+1
        
        workbook.close()

        output.seek(0)

        filename = 'Inventario.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getTablaTiendasPendientesFile(request, fecha):
    try:
        recepcionTiendaDao=RecepcionTiendaDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'ALMACEN')
        worksheet.write(0, 1, 'FECHA ENTREGA')
        worksheet.write(0, 2, 'NUMERO PEDIDOS')
        worksheet.write(0, 3, 'PIEZAS')
        worksheet.write(0, 4, 'CONTENEDORES')

        tablaTiendasPendientesList=recepcionTiendaDao.getTablaTiendasPendientes(fecha)
        
        row=1
        for tiendaPendiente in tablaTiendasPendientesList:
            worksheet.write(row, 0, tiendaPendiente.nombreAlmacen)
            worksheet.write(row, 1, tiendaPendiente.fechaEntrega)
            worksheet.write(row, 2, tiendaPendiente.numPedidos)
            worksheet.write(row, 3, tiendaPendiente.piezas)
            worksheet.write(row, 4, tiendaPendiente.contenedores)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'TablaTiendasPendientes.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def topOneHundredFile(request):
    try:
        scaleIntDao=ScaleIntDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'DATE TIME')
        worksheet.write(0, 1, 'ITEM')
        worksheet.write(0, 2, 'WAREHOUSE')
        worksheet.write(0, 3, 'WMS SUSPENSE')
        worksheet.write(0, 4, 'WMS INTRANSIT')
        worksheet.write(0, 5, 'WMS ONHAND')
        worksheet.write(0, 6, 'ERP ONHAND')
        worksheet.write(0, 7, 'DIF ONHAND')
        worksheet.write(0, 8, 'DIF OH ABS')
        
        TopList=scaleIntDao.getTopOneHundred(False)
        
        row=1
        for top in TopList:
            worksheet.write(row, 0, top.fecha.strftime("%m/%d/%Y %H:%M:%S"))
            worksheet.write(row, 1, top.item)
            worksheet.write(row, 2, top.warehouse)
            worksheet.write(row, 3, top.wmsComprometido)
            worksheet.write(row, 4, top.wmsTransito)
            worksheet.write(row, 5, top.wmsOnHand)
            worksheet.write(row, 6, top.erpOnHand)
            worksheet.write(row, 7, top.difOnHand)
            worksheet.write(row, 8, top.difOnHandAbsolute)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'TopOneHundred.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getPedidosPlaneacion(request, fechaInicio, fechaFin):
    try:
        sapDao=SAPDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'PEDIDO')
        worksheet.write(0, 1, 'ITEM')
        worksheet.write(0, 2, 'CANTIDAD')
        worksheet.write(0, 3, 'FECHA CREACION')
        worksheet.write(0, 4, 'FECHA VENCIMIENTO')
        
        pedidosList=sapDao.getPedidosPlaneacion(fechaInicio, fechaFin)
        
        row=1
        for pedido in pedidosList:
            fechaCreacion=datetime.strptime(pedido.docDate[:26], '%Y-%m-%d %H:%M:%S.%f')
            fechaVencimiento=datetime.strptime(pedido.docDueDate[:26], '%Y-%m-%d %H:%M:%S.%f')
            worksheet.write(row, 0, pedido.docNum)
            worksheet.write(row, 1, pedido.itemCode)
            worksheet.write(row, 2, pedido.quantity)
            worksheet.write(row, 3, fechaCreacion.strftime("%d/%m/%Y"))
            worksheet.write(row, 4, fechaVencimiento.strftime("%d/%m/%Y"))
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'PedidosPlaneacion_'+fechaInicio+'_'+fechaFin+'.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getPedidosPlaneacionTop(request, fechaInicio, fechaFin):
    try:
        sapDao=SAPDao()
        pedidosList=sapDao.getPedidosPlaneacionTop(fechaInicio, fechaFin)
        serializer=PedidoPlaneacionSerializer(pedidosList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getUbicacionesVaciasTop(request):
    try:
        wmsDao=WMSDao()
        ubicacionesList=wmsDao.getUbicacionesVaciasReservaTop()
        serializer=UbicacionVaciaSerializer(ubicacionesList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getUbicacionesVaciasFile(request):
    try:
        wmsDao=WMSDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'UBICACION')
        worksheet.write(0, 1, 'ESTATUS')
        worksheet.write(0, 2, 'ACTIVO')
        
        ubicacionesList=wmsDao.getUbicacionesVaciasAll()
        
        row=1
        for ubicacion in ubicacionesList:
            worksheet.write(row, 0, ubicacion.ubicacion)
            worksheet.write(row, 1, ubicacion.status)
            worksheet.write(row, 2, ubicacion.active)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'ubicacionesVacias.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getEstatusOla(request):
    try:
        wmsDao=WMSDao()
        estatusContenedorList=wmsDao.getEstatusContenedores()
        serializer=EstatusContendorSerializer(estatusContenedorList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getEstatusOlaFile(request):
    try:
        wmsDao=WMSDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'SEMANA')
        worksheet.write(0, 1, 'OLA')
        worksheet.write(0, 2, 'PEDIDOS')
        worksheet.write(0, 3, 'CONTENEDORES')
        worksheet.write(0, 4, 'CAJAS')
        worksheet.write(0, 5, 'BOLSAS')
        worksheet.write(0, 6, 'PICKING PENDING')
        worksheet.write(0, 7, 'IN PICKING')
        worksheet.write(0, 8, 'PACKING PENDING')
        worksheet.write(0, 9, 'IN PACKING')
        worksheet.write(0, 10, 'STAGING PENDING')
        worksheet.write(0, 11, 'LOADING PEDING')
        worksheet.write(0, 12, 'SHIP CONFIRM PENDING')
        worksheet.write(0, 13, 'LOAD CONFIRM PENDING')
        worksheet.write(0, 14, 'CLOSED')
        worksheet.write(0, 15, 'CUMPLIMIENTO')
        
        estatusContenedorList=wmsDao.getEstatusContenedores()
        
        row=1
        for estatusOla in estatusContenedorList:
            worksheet.write(row, 0, estatusOla.semana)
            worksheet.write(row, 1, estatusOla.ola)
            worksheet.write(row, 2, estatusOla.pedidos)
            worksheet.write(row, 3, estatusOla.contenedores)
            worksheet.write(row, 4, estatusOla.carton)
            worksheet.write(row, 5, estatusOla.bolsa)
            worksheet.write(row, 6, estatusOla.pickingPending)
            worksheet.write(row, 7, estatusOla.inPicking)
            worksheet.write(row, 8, estatusOla.packingPending)
            worksheet.write(row, 9, estatusOla.inPacking)
            worksheet.write(row, 10, estatusOla.stagingPending)
            worksheet.write(row, 11, estatusOla.loadingPending)
            worksheet.write(row, 12, estatusOla.shipConfirmPending)
            worksheet.write(row, 13, estatusOla.loadConfirmPending)
            worksheet.write(row, 14, estatusOla.closed)
            worksheet.write(row, 15, str("{0:.2f}".format(estatusOla.closed*100/estatusOla.contenedores))+"%")
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'estatusOla.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getOlaPiezasContenedores(request):
    try:
        wmsDao=WMSDao()
        olaList=wmsDao.getOlaPiezasContenedores()
        serializer=OlaPiezasContenedoresSerializer(olaList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getOlaPiezasContenedoresFile(request):
    try:
        wmsDao=WMSDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'OLA')
        worksheet.write(0, 1, 'NUMERO_PIEZAS')
        worksheet.write(0, 2, 'NUMERO_CONTENEDORES')
        
        olaList=wmsDao.getOlaPiezasContenedores()
        
        row=1
        for ola in olaList:
            worksheet.write(row, 0, ola.ola)
            worksheet.write(row, 1, ola.numPiezas)
            worksheet.write(row, 2, ola.numContenedores)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'OlaPiezasContenedores.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def getDetalleEstatusContenedores(request):
    try:
        
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'OLA')
        worksheet.write(0, 1, 'PEDIDO')
        worksheet.write(0, 2, 'CONTENEDOR')
        worksheet.write(0, 3, 'ESTATUS')
        worksheet.write(0, 4, 'TIPO')
        
        wmsDao=WMSDao()
        detalleList=wmsDao.getDetalleEstatusContenedores()

        row=1
        for detalle in detalleList:
            worksheet.write(row, 0, detalle.ola)
            worksheet.write(row, 1, detalle.pedido)
            worksheet.write(row, 2, detalle.contenedor)
            worksheet.write(row, 3, detalle.estatus)
            worksheet.write(row, 4, detalle.tipo)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'DetalleEstatusContenedores.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getLineasOla(request, ola):
    try:
        wmsDao=WMSDao()
        olaList=wmsDao.getLineasOla(True, ola)
        serializer=LineaOlaSerializer(olaList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getLineasOlaFile(request, ola):
    try:
        wmsDao=WMSDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'OLA')
        worksheet.write(0, 1, 'PEDIDO')
        worksheet.write(0, 2, 'ITEM')
        worksheet.write(0, 3, 'DESCRIPCION')
        worksheet.write(0, 4, 'TOTAL')
        worksheet.write(0, 5, 'STATUS')
        
        olaList=wmsDao.getLineasOla(False, ola)
        
        row=1
        for lineaOla in olaList:
            worksheet.write(row, 0, lineaOla.ola)
            worksheet.write(row, 1, lineaOla.pedido)
            worksheet.write(row, 2, lineaOla.item)
            worksheet.write(row, 3, lineaOla.descripcion)
            worksheet.write(row, 4, lineaOla.total)
            worksheet.write(row, 5, lineaOla.status)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'LineasOla'+ola+'.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def getTareasReaSurtAbiertasFile(request):
    try:
        wmsDao=WMSDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'WORK_UNIT')
        worksheet.write(0, 1, 'INSTRUCTION_TYPE')
        worksheet.write(0, 2, 'WORK_TYPE')
        worksheet.write(0, 3, 'WAVEPACK')
        worksheet.write(0, 4, 'FAMILIA')
        worksheet.write(0, 5, 'CONDICION')
        worksheet.write(0, 6, 'ITEM')
        worksheet.write(0, 7, 'ITEM_DESC')
        worksheet.write(0, 8, 'REFERENCE_ID')
        worksheet.write(0, 9, 'FROM_LOC')
        worksheet.write(0, 10, 'FROM_QTY')
        worksheet.write(0, 11, 'TO_LOC')
        worksheet.write(0, 12, 'TO_QTY')
        worksheet.write(0, 13, 'OLA')
        worksheet.write(0, 14, 'NUMERO_INTERNO_INSTRUCCION')
        worksheet.write(0, 15, 'CONVERTED_QTY')
        worksheet.write(0, 16, 'CONTENEDOR')
        worksheet.write(0, 17, 'TIPO_CONTENEDOR')
        worksheet.write(0, 18, 'AGING_DATE_TIME')
        worksheet.write(0, 19, 'START_DATE_TIME')
        
        tareasList=wmsDao.getTareasReaSurtAbiertas()
        
        row=1
        for tarea in tareasList:
            worksheet.write(row, 0, tarea.workUnit)
            worksheet.write(row, 1, tarea.instructionType)
            worksheet.write(row, 2, tarea.workType)
            worksheet.write(row, 3, tarea.wavepack)
            worksheet.write(row, 4, tarea.familia)
            worksheet.write(row, 5, tarea.condicion)
            worksheet.write(row, 6, tarea.item)
            worksheet.write(row, 7, tarea.itemDesc)
            worksheet.write(row, 8, tarea.referenceId)
            worksheet.write(row, 9, tarea.fromLoc)
            worksheet.write(row, 10, tarea.fromQty)
            worksheet.write(row, 11, tarea.toLoc)
            worksheet.write(row, 12, tarea.toQty)
            worksheet.write(row, 13, tarea.ola)
            worksheet.write(row, 14, tarea.numeroInternoInstruccion)
            worksheet.write(row, 15, tarea.convertedQty)
            worksheet.write(row, 16, tarea.numConteneder)
            worksheet.write(row, 17, tarea.tipoContenedor)
            worksheet.write(row, 18, tarea.agingDateTime)
            worksheet.write(row, 19, tarea.startDateTime)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'TareasAbiertas.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getContenedoresFile(request):
    try:
        wmsDao=WMSDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'CONTAINER_ID')
        worksheet.write(0, 1, 'SHIPMENT_ID')
        worksheet.write(0, 2, 'CONTAINER_TYPE')
        worksheet.write(0, 3, 'LEADING_STS')
        worksheet.write(0, 4, 'ITEM')
        worksheet.write(0, 5, 'QUANTITY')
        worksheet.write(0, 6, 'CUSTOMER')
        worksheet.write(0, 7, 'LAUNCH_NUM')
        worksheet.write(0, 8, 'FECHA')
        
        contenedoresList=wmsDao.getContenedores()
        
        row=1
        for contenedor in contenedoresList:
            worksheet.write(row, 0, contenedor.containerId)
            worksheet.write(row, 1, contenedor.shipmentId)
            worksheet.write(row, 2, contenedor.containerType)
            worksheet.write(row, 3, contenedor.leadingSts)
            worksheet.write(row, 4, contenedor.item)
            worksheet.write(row, 5, contenedor.quantity)
            worksheet.write(row, 6, contenedor.customer)
            worksheet.write(row, 7, contenedor.launchNum)
            worksheet.write(row, 8, contenedor.fecha)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'Contenedores.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getInventarioAlmacenajeFile(request):
    try:
        wmsDao=WMSDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'UBICACIÓN')
        worksheet.write(0, 1, 'PERMANENTE')
        worksheet.write(0, 2, 'ACTIVA')
        worksheet.write(0, 3, 'ZONA')
        worksheet.write(0, 4, 'ITEM')
        worksheet.write(0, 5, 'DESCRIPCIÓN')
        worksheet.write(0, 6, 'ESTATUS')
        worksheet.write(0, 7, 'DISPONIBLE')
        worksheet.write(0, 8, 'EXISTENTE')
        worksheet.write(0, 9, 'EN TRANSITO')
        worksheet.write(0, 10, 'COMPROMETIDO')
        worksheet.write(0, 11, 'SUSPENDIDO')
        worksheet.write(0, 12, 'FAMILIA')
        worksheet.write(0, 13, 'SUB FAMILIA')
        worksheet.write(0, 14, 'SUB SUB FAMILIA')
        
        inventarioList=wmsDao.getInventario()
        
        row=1
        for inventario in inventarioList:
            worksheet.write(row, 0, inventario.ubicacion)
            worksheet.write(row, 1, inventario.permanente)
            worksheet.write(row, 2, inventario.activa)
            worksheet.write(row, 3, inventario.zona)
            worksheet.write(row, 4, inventario.item)
            worksheet.write(row, 5, inventario.descripcion)
            worksheet.write(row, 6, inventario.estatus)
            worksheet.write(row, 7, inventario.disponible)
            worksheet.write(row, 8, inventario.existente)
            worksheet.write(row, 9, inventario.transito)
            worksheet.write(row, 10, inventario.comprometido)
            worksheet.write(row, 11, inventario.suspendido)
            worksheet.write(row, 12, inventario.familia)
            worksheet.write(row, 13, inventario.subfamila)
            worksheet.write(row, 14, inventario.subsubfamila)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'InventarioSurtido.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getCantidadCajas(request, item):
    try:
        wmsDao=WMSDao()
        cantidad=wmsDao.getCantidadCajas(item)
        serializer=CantidadCajasSerializer(cantidad, many=False)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getPorcentajeSKUsPrioritarios(request, container):
    try:
        wmsDao=WMSDao()
        porcentaje=wmsDao.getPorcentajeSKUsPrioritarios(container)
        serializer=PorcentajeSkusPrioritariosSerializer(porcentaje, many=False)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getInventarioTienda(request):
    try:
        monitoreoDao=MonitoreoDao()
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output)

        inventarioList=monitoreoDao.getInventarioTienda()
        
        row=0
        for inventario in inventarioList:
        
            if row%1000000==0:
                worksheet = workbook.add_worksheet()
                worksheet.write(0, 0, 'ALMACEN')
                worksheet.write(0, 1, 'ITEM')
                worksheet.write(0, 2, 'DESCRIPCION')
                worksheet.write(0, 3, 'STOCK')
                worksheet.write(0, 4, 'COMPROMETIDO')
                worksheet.write(0, 5, 'SOLICITADO')
                worksheet.write(0, 6, 'DISPONIBLE')
                worksheet.write(0, 7, 'ULTIMO PRECIO')
                row=1
            
            worksheet.write(row, 0, inventario.almacen)
            worksheet.write(row, 1, inventario.item)
            worksheet.write(row, 2, inventario.descripcion)
            worksheet.write(row, 3, inventario.stock)
            worksheet.write(row, 4, inventario.comprometido)
            worksheet.write(row, 5, inventario.solicitado)
            worksheet.write(row, 6, inventario.stock-inventario.comprometido+inventario.solicitado)
            worksheet.write(row, 7, inventario.ultimoPrecio)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'InventarioTiendas.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getWaveAnalysis(request, wave):
    try:
        wmsDao=WMSDao()
        waveList=wmsDao.getWaveAnalysis(True, wave)
        serializer=WaveSerializer(waveList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getWaveAnalysisFile(request, wave):
    try:
        wmsDao=WMSDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'ITEM')
        worksheet.write(0, 1, 'DESCRIPCION')
        worksheet.write(0, 2, 'STORAGE TEMPLATE')
        worksheet.write(0, 3, 'SHIPMENT ID')
        worksheet.write(0, 4, 'LAUNCH NUM')
        worksheet.write(0, 5, 'STATUS')
        worksheet.write(0, 6, 'REQUESTED QTY')
        worksheet.write(0, 7, 'ALLOCATED QTY')
        worksheet.write(0, 8, 'AV')
        worksheet.write(0, 9, 'OH')
        worksheet.write(0, 10, 'AL')
        worksheet.write(0, 11, 'IT')
        worksheet.write(0, 12, 'SU')
        worksheet.write(0, 13, 'CUSTOMER')
        worksheet.write(0, 14, 'ITEM CATEGORY')
        worksheet.write(0, 15, 'CREATION DATE')
        worksheet.write(0, 16, 'SCHEDULED SHIP DATE')
        worksheet.write(0, 17, 'DIVISION')
        worksheet.write(0, 18, 'CONV')
        
        waveList=wmsDao.getWaveAnalysis(False, wave)
        
        row=1
        for wav in waveList:
            worksheet.write(row, 0, wav.item)
            worksheet.write(row, 1, wav.description)
            worksheet.write(row, 2, wav.storageTemplate)
            worksheet.write(row, 3, wav.shipmentId)
            worksheet.write(row, 4, wav.launchNum)
            worksheet.write(row, 5, wav.status)
            worksheet.write(row, 6, wav.requestedQty)
            worksheet.write(row, 7, wav.allocatedQty)
            worksheet.write(row, 8, wav.av)
            worksheet.write(row, 9, wav.oh)
            worksheet.write(row, 10, wav.al)
            worksheet.write(row, 11, wav.it)
            worksheet.write(row, 12, wav.su)
            worksheet.write(row, 13, wav.customer)
            worksheet.write(row, 14, wav.itemCategory)
            worksheet.write(row, 15, wav.creationDateTimeStamp)
            worksheet.write(row, 16, wav.scheduledShipDate)
            worksheet.write(row, 17, wav.division)
            worksheet.write(row, 18, wav.conv)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'WaveAnalysis'+wave+'.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getTiendasCorreo(request):
    try:
        recepcionTiendaDao=RecepcionTiendaDao()
        tiendaCorreoList=recepcionTiendaDao.getCorreoTiendaPendiente()
        serializer=TiendaCorreoSerializer(tiendaCorreoList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getDiferenciasDetalleRecibo(request):
    try:
        monitoreoDao=MonitoreoDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'RECIBO')
        worksheet.write(0, 1, 'ITEM')
        worksheet.write(0, 2, 'CANTIDAD WMS')
        worksheet.write(0, 3, 'CANTIDAD SAP')
        worksheet.write(0, 4, 'DIFERENCIA')
        
        reciboDetalleList=monitoreoDao.getCuadrajeReciboDetalle()
        
        row=1
        for reciboDetalle in reciboDetalleList:
            worksheet.write(row, 0, reciboDetalle.recibo)
            worksheet.write(row, 1, reciboDetalle.item)
            worksheet.write(row, 2, reciboDetalle.cantidadWms)
            worksheet.write(row, 3, reciboDetalle.cantidadSap)
            worksheet.write(row, 4, (reciboDetalle.cantidadWms-reciboDetalle.cantidadSap))
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'DetalleRecibos.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getPesoCaja(request, contenedor):
    try:
        wmsDao=WMSDao()
        pesoContenedor=wmsDao.getPesoContenedor(contenedor)
        serializer=PesoContenedorSerializer(pesoContenedor, many=False)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getconsultaTest(request, fechaInicio):
    try:
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()

        row=0

        print('Inicia consulta')
        
        conexion=dbapi.connect(address="192.168.84.182", port=30015, user="SYSTEM", password="Sy573Mmnso!!")
        cursor=conexion.cursor()

        print('Ejecuta consulta')
        cursor.execute('SELECT C."WhsCode" , '+
                        'C."ItemCode", '+
                        'C."ItemName", '+    
                        'iFnull(C."OnHand",0)- iFnull(B."Stock",0) "Disponible", '+
                        '/*consulta de Inventario al 20180831, ya que se le restan los movimientos de septiembre */ '+
                        'C."LastPurPrc" '+
                        'FROM (   /*consulta de kardex a una fecha especifica (20180901), auditoria de stock totalizado por SKU/Almacen */ '+
                        'select distinct         K2."ItemCode", '+
                        'K2."ItemName",         K1."WhsCode", '+    
                        'K1."WhsName",        iFnull(sum(K6."Saldo"),0) "Stock" '+
                        'from "SBOMINISO".OITM K2      inner JOIN "SBOMINISO".OITW K0 ON K0."ItemCode" = K2."ItemCode" '+
                        'inner JOIN "SBOMINISO".OWHS K1 ON K1."WhsCode" = K0."WhsCode"      left join '+
                        '(select      A0."ItemCode", '+
                        'A0."Warehouse", '+
                        '(A0."InQty" - A0."OutQty") AS "Saldo", /* Entradas - Salidas*/ '+
                        'A0."TransValue"                      from "SBOMINISO".OINM A0 '+              
                        'where A0."DocDate" >= ?) K6 on K6."ItemCode" = K0."ItemCode" and K6."Warehouse" = K0."WhsCode" '+
                        'group by K2."ItemCode", K2."ItemName", K1."WhsCode", K1."WhsName"      having sum(K6."Saldo") <> 0      /*order by 1,3*/ ) B /* fin consulta de kardex a una fecha especifica (20180901), auditoria de stock */ FULL OUTER JOIN (  /* consulta de inventario a este momento*/ SELECT       T1."WhsCode" ,       T1."ItemCode",       T0."ItemName",       iFnull(T1."OnHand",0) "OnHand" ,       T0."LastPurPrc"  FROM  "SBOMINISO".OITM T0   INNER  JOIN "SBOMINISO".OITW T1  ON  T1."ItemCode" = T0."ItemCode"    WHERE       T1."ItemCode" = T0."ItemCode"   /*  AND T1."OnHand" <> 0 */  /*ORDER BY T1."WhsCode", T1."ItemCode"*/ ) C  /* fin consulta de inventario a este momento*/ ON B."ItemCode" = C."ItemCode" AND B."WhsCode" = C."WhsCode" WHERE    iFnull(C."OnHand",0)- iFnull(B."Stock",0) <> 0', (fechaInicio))
        
        registros=cursor.fetchall()
        print('Termina consulta')
        for registro in registros:

            if row%1000000==0:
                worksheet = workbook.add_worksheet()
                worksheet.write(0, 0, 'ALMACEN')
                worksheet.write(0, 1, 'ITEM')
                worksheet.write(0, 2, 'NOMBRE ITEM')
                worksheet.write(0, 3, 'DISPONIBLE')
                worksheet.write(0, 4, 'ULTIMO PRECIO COMPRA')
                row=1
            
            worksheet.write(row, 0, registro[0])
            worksheet.write(row, 1, registro[1])
            worksheet.write(row, 2, registro[2])
            worksheet.write(row, 3, registro[3])
            worksheet.write(row, 4, registro[4])
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'ConsultaTest.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    finally:
        if conexion!= None:
            try:
                conexion.close()
            except Exception as exception:
                logger.error(f"Se presento una incidencia al cerrar la conexion: {exception}")
                raise exception


@api_view(['GET'])
def getVentasXFecha(request, fechaInicio, fechaFin):
    try:
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output)
        row=0
        print('Inicia consulta')
        
        conexion=dbapi.connect(address="192.168.84.182", port=30015, user="SYSTEM", password="Sy573Mmnso!!")
        cursor=conexion.cursor()

        print('Ejecuta consulta')
        cursor.execute('SELECT T0."DocNum", T0."DocDate", T0."CardCode", T2."CardName", T1."ItemCode", T1."Dscription", T1."DiscPrcnt", '+
                        'T1."PriceBefDi", T3."AvgPrice", T1."GrossBuyPr", T1."OcrCode" AS "REGION", T1."OcrCode2" AS "UNIDAD NEGOCIO", '+
                        'T1."OcrCode3" AS "DEPTO", SUM(T1."Quantity"), SUM(T1."LineTotal"), T0."DiscSum" AS "DctoCabecera", T4."ItmsGrpNam", T1."AcctCode" '+
                        'FROM "SBOMINISO".OINV T0 '+
                        'INNER JOIN "SBOMINISO".INV1 T1 ON T0."DocEntry" = T1."DocEntry" '+
                        'INNER JOIN "SBOMINISO".OCRD T2 ON T0."CardCode" = T2."CardCode" '+
                        'LEFT JOIN "SBOMINISO".OITM T3 ON T1."ItemCode" = T3."ItemCode" '+
                        'LEFT JOIN "SBOMINISO".OITB T4 ON T3."ItmsGrpCod" = T4."ItmsGrpCod" '+
                        'WHERE (T0."DocDate" between ? AND ?) '+
                        'and T0."CANCELED"=\'N\' '+
                        'GROUP BY T0."DocNum",T0."DocDate",T0."CardCode", T2."CardName", T1."ItemCode",T1."Dscription",T1."DiscPrcnt", T1."PriceBefDi", '+
                        'T3."AvgPrice", T1."GrossBuyPr", T1."OcrCode", T1."OcrCode2", T1."OcrCode3", T1."OcrCode4",T0."DiscSum",T4."ItmsGrpNam",T1."AcctCode" '+
                        'ORDER BY T0."CardCode"', (fechaInicio, fechaFin))

        registros=cursor.fetchall()
        print('Termina consulta')
        for registro in registros:
            if row%1040000==0:
                worksheet = workbook.add_worksheet()
                worksheet.write(0, 0, 'DocNum')
                worksheet.write(0, 1, 'DocDate')
                worksheet.write(0, 2, 'CardCode')
                worksheet.write(0, 3, 'CardName')
                worksheet.write(0, 4, 'ItemCode')
                worksheet.write(0, 5, 'Dscription')
                worksheet.write(0, 6, 'DiscPrcnt')
                worksheet.write(0, 7, 'PriceBefDi')
                worksheet.write(0, 8, 'AvgPrice')
                worksheet.write(0, 9, 'GrossBuyPr')
                worksheet.write(0, 10, 'REGION')
                worksheet.write(0, 11, 'UNIDAD NEGOCIO')
                worksheet.write(0, 12, 'DEPTO')
                worksheet.write(0, 13, 'CANTIDAD')
                worksheet.write(0, 14, 'TOTAL LINEA')
                worksheet.write(0, 15, 'DctoCabecera')
                worksheet.write(0, 16, 'ItmsGrpNam')
                worksheet.write(0, 17, 'AcctCode')
                row=1

            worksheet.write(row, 0, registro[0])
            worksheet.write(row, 1, registro[1])
            worksheet.write(row, 2, registro[2])
            worksheet.write(row, 3, registro[3])
            worksheet.write(row, 4, registro[4])
            worksheet.write(row, 5, registro[5])
            worksheet.write(row, 6, registro[6])
            worksheet.write(row, 7, registro[7])
            worksheet.write(row, 8, registro[8])
            worksheet.write(row, 9, registro[9])
            worksheet.write(row, 10, registro[10])
            worksheet.write(row, 11, registro[11])
            worksheet.write(row, 12, registro[12])
            worksheet.write(row, 13, registro[13])
            worksheet.write(row, 14, registro[14])
            worksheet.write(row, 15, registro[15])
            worksheet.write(row, 16, registro[16])
            worksheet.write(row, 17, registro[17])
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'VentasXFecha.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    finally:
        if conexion!= None:
            try:
                conexion.close()
            except Exception as exception:
                logger.error(f"Se presento una incidencia al cerrar la conexion: {exception}")
                raise exception


#################################################Chile####################################################

#################################################Inventario#################################################

@api_view(['GET'])
def inventarioClWmsErp(request):
    try:
        scaleIntChileDao=ScaleIntChileDao()
        wmsErpList=scaleIntChileDao.getWmsErp()
        serializer=InventarioWmsErpSerializer(wmsErpList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def inventarioClItems(request):
    try:
        scaleIntChileDao=ScaleIntChileDao()
        itemsList=scaleIntChileDao.getItems()
        serializer=InventarioItemSerializer(itemsList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def inventarioClWms(request):
    try:
        monitoreoDao=MonitoreoDao()
        inventariosWmsList=monitoreoDao.getInventarioClWms()
        serializer=InventarioWmsSerializer(inventariosWmsList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def topOneHundredCl(request):
    try:
        scaleIntChileDao=ScaleIntChileDao()
        TopList=scaleIntChileDao.getTopOneHundred(True)
        serializer=InventarioTopSerializer(TopList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def inventarioClDetalleErpWms(request, item):
    try:
        scaleIntChileDao=ScaleIntChileDao()
        detallesErpWmsList=scaleIntChileDao.getInventarioDetalleErpWms(item)
        serializer=InventarioDetalleErpWmsSerializer(detallesErpWmsList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def inventarioClFile(request):
    try:
        scaleIntChileDao=ScaleIntChileDao()
        monitoreoDao=MonitoreoDao()
        
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'WAREHOUSE')
        worksheet.write(0, 1, 'WMS ONHAND')
        worksheet.write(0, 2, 'ERP ONHAND')
        worksheet.write(0, 3, 'DIFERENCIA')
        worksheet.write(0, 4, 'DIFERENCIA ABS')
        worksheet.write(0, 5, 'WMS INTRANSIT')
        worksheet.write(0, 6, '#ITEMS WMS')
        worksheet.write(0, 7, '#ITEMS ERP')
        worksheet.write(0, 8, '#ITEMS DIF')
        
        wmsErpList=scaleIntChileDao.getWmsErp()
        row=1
        for wmsErp in wmsErpList:
            worksheet.write(row, 0, wmsErp.warehouse)
            worksheet.write(row, 1, wmsErp.wmsOnHand)
            worksheet.write(row, 2, wmsErp.erpOnHand)
            worksheet.write(row, 3, wmsErp.diferencia)
            worksheet.write(row, 4, wmsErp.diferenciaAbsoluta)
            worksheet.write(row, 5, wmsErp.wmsInTransit)
            worksheet.write(row, 6, wmsErp.numItemsWms)
            worksheet.write(row, 7, wmsErp.numItemsErp)
            worksheet.write(row, 8, wmsErp.numItemsDif)
            row=row+1

        row=row+1
        worksheet.write(row, 0, 'WAREHOUSE')
        worksheet.write(row, 1, 'WMS ONHAND')
        worksheet.write(row, 2, 'ERP ONHAND')
        worksheet.write(row, 3, '#ITEMS')
        
        itemsList=scaleIntChileDao.getItems()
        row=row+1
        for item in itemsList:
            worksheet.write(row, 0, item.warehouse)
            worksheet.write(row, 1, item.wmsOnHand)
            worksheet.write(row, 2, item.erpOnHand)
            worksheet.write(row, 3, item.numItems)
            row=row+1
        
        row=row+1
        worksheet.write(row, 0, 'WAREHOUSE CODE')
        worksheet.write(row, 1, 'Solicitado')
        worksheet.write(row, 2, 'OnHand')
        worksheet.write(row, 3, 'Comprometido')
        worksheet.write(row, 4, 'Disponible')
        worksheet.write(row, 5, 'SKU SOL')
        worksheet.write(row, 6, 'SKU OHD')
        worksheet.write(row, 7, 'SKU CMP')
        worksheet.write(row, 8, 'Fecha Actualizacion')
        
        inventariosWmsList=monitoreoDao.getInventarioClWms()
        row=row+1
        for inventarioWms in inventariosWmsList:
            worksheet.write(row, 0, inventarioWms.warehouseCode)
            worksheet.write(row, 1, inventarioWms.solicitado)
            worksheet.write(row, 2, inventarioWms.onHand)
            worksheet.write(row, 3, inventarioWms.comprometido)
            worksheet.write(row, 4, inventarioWms.disponible)
            worksheet.write(row, 5, inventarioWms.skuSolicitado)
            worksheet.write(row, 6, inventarioWms.skuOnHand)
            worksheet.write(row, 7, inventarioWms.skuComprometido)
            worksheet.write(row, 8, inventarioWms.fechaActualizacion)
            row=row+1
        
        workbook.close()

        output.seek(0)

        filename = 'Inventario.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
def topOneHundredClFile(request):
    try:
        scaleIntChileDao=ScaleIntChileDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'DATE TIME')
        worksheet.write(0, 1, 'ITEM')
        worksheet.write(0, 2, 'WAREHOUSE')
        worksheet.write(0, 3, 'WMS SUSPENSE')
        worksheet.write(0, 4, 'WMS INTRANSIT')
        worksheet.write(0, 5, 'WMS ONHAND')
        worksheet.write(0, 6, 'ERP ONHAND')
        worksheet.write(0, 7, 'DIF ONHAND')
        worksheet.write(0, 8, 'DIF OH ABS')
        
        TopList=scaleIntChileDao.getTopOneHundred(False)
        
        row=1
        for top in TopList:
            worksheet.write(row, 0, top.fecha.strftime("%m/%d/%Y %H:%M:%S"))
            worksheet.write(row, 1, top.item)
            worksheet.write(row, 2, top.warehouse)
            worksheet.write(row, 3, top.wmsComprometido)
            worksheet.write(row, 4, top.wmsTransito)
            worksheet.write(row, 5, top.wmsOnHand)
            worksheet.write(row, 6, top.erpOnHand)
            worksheet.write(row, 7, top.difOnHand)
            worksheet.write(row, 8, top.difOnHandAbsolute)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'TopOneHundred.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def executeInvetarioClCedis(request):
    try:
        monitoreoDao=MonitoreoDao()
        monitoreoDao.executeInvetarioClCedis()
        return Response(status=status.HTTP_200_OK)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

#################################################Cuadraje#################################################

@api_view(['GET'])
def getReciboSapCl(request, recibos):
    try:
        recibosSplit=recibos.split(',')
        indice =True
        busqueda=''
        for recibo in recibosSplit:
            if(indice==False):
                busqueda=busqueda + ","
            busqueda=busqueda+"'"+recibo.strip()+"'"
            if(indice):
                indice=False
        monitoreoDao=MonitoreoDao()
        logger.error("Entro a getReciboSap "+busqueda)
        recibosList=monitoreoDao.getReciboSapCl(busqueda)
        serializer=ReciboSapSerializer(recibosList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getReciboSapByValorCl(request, valor):
    try:
        monitoreoDao=MonitoreoDao()
        recibosList=monitoreoDao.getReciboSapByValorCl(valor)
        serializer=ReciboSapSerializer(recibosList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getPedidoSapCl(request, pedidos):
    try:
        pedidosSplit=pedidos.split(',')
        indice =True
        busqueda=''
        for pedido in pedidosSplit:
            if(indice==False):
                busqueda=busqueda + ","
            busqueda=busqueda+"'"+pedido.strip()+"'"
            if(indice):
                indice=False
        monitoreoDao=MonitoreoDao()
        pedidosList=monitoreoDao.getPedidoSapCl(busqueda)
        serializer=PedidoSapSerializer(pedidosList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getPedidoSapByValorCl(request, valor):
    try:
        monitoreoDao=MonitoreoDao()
        if valor == 'O':
            pedidosList=monitoreoDao.getPedidoSapAbiertosCl()
        else:
            pedidosList=monitoreoDao.getPedidoSapByValorCl(valor)
        serializer=PedidoSapSerializer(pedidosList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def executeCuadrajeCl(request):
    try:
        monitoreoDao=MonitoreoDao()
        monitoreoDao.executeCuadrajeCl()
        return Response(status=status.HTTP_200_OK)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getCuadrajeCl(request):
    try:
        monitoreoDao=MonitoreoDao()
        cuadrajesList=monitoreoDao.getDatosCuadrajeCl()
        serializer=CuadrajeSerializer(cuadrajesList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getPendienteSemanaCl(request):
    try:
        monitoreoDao=MonitoreoDao()
        pendientesSemanaList=monitoreoDao.getPendienteSemanaCl()
        serializer=PendienteSemanaSerializer(pendientesSemanaList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def insertSapReceiptValCl(request, idReceipt):
    try:
        monitoreoDao=MonitoreoDao()
        monitoreoDao.insertSapReceiptValCl(idReceipt)
        return Response(status=status.HTTP_200_OK)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def insertSapShipmentValCl(request, idShipment):
    try:
        monitoreoDao=MonitoreoDao()
        monitoreoDao.insertSapShipmentValCl(idShipment)
        return Response(status=status.HTTP_200_OK)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def executeUpdateDiferenciasCl(request):
    try:
        monitoreoDao=MonitoreoDao()
        monitoreoDao.executeUpdateDiferenciasCl()
        return Response(status=status.HTTP_200_OK)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getCuadrajeFileCl(request):
    try:
        monitoreoDao=MonitoreoDao()
        cuadrajesList=monitoreoDao.getDatosCuadrajeCl()
        cuadraje=cuadrajesList[0]
        
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 1, 'RECIBO')
        worksheet.write(0, 2, 'PEDIDO')
        worksheet.write(1, 0, 'TOTAL DE FOLIOS')
        worksheet.write(1, 1, cuadraje.recibosTotal)
        worksheet.write(1, 2, cuadraje.pedidosTotal)
        worksheet.write(2, 0, 'FOLIOS OK')
        worksheet.write(2, 1, cuadraje.recibosOk)
        worksheet.write(2, 2, cuadraje.pedidosOk)
        worksheet.write(3, 0, 'FOLIOS CON DIFERENCIA EN CANTIDAD')
        worksheet.write(3, 1, cuadraje.recibosQty)
        worksheet.write(3, 2, cuadraje.pedidosQty)
        worksheet.write(4, 0, 'FOLIOS PENDINETES DE CERRAR EN ERP')
        worksheet.write(4, 1, cuadraje.recibosCloseErp)
        worksheet.write(4, 2, cuadraje.pedidosCloseErp)
        worksheet.write(5, 0, 'FOLIOS PENDIENTES DE CERRAR EN ERP Y WMS')
        worksheet.write(5, 1, cuadraje.recibosRev)
        worksheet.write(5, 2, cuadraje.pedidosRev)
        
        pendientesSemanaList=monitoreoDao.getPendienteSemanaCl()
        
        worksheet.write(7, 0, 'MES-AÑO')
        worksheet.write(7, 1, 'SEMANA')
        worksheet.write(7, 2, 'NUMERO')
        worksheet.write(7, 3, 'TIENDAS')
        worksheet.write(7, 4, 'RE-ETIQUETADO')
        row=8
        for pendienteSemana in pendientesSemanaList:
            worksheet.write(row, 0, pendienteSemana.fecha)
            worksheet.write(row, 1, pendienteSemana.shipDate)
            worksheet.write(row, 2, pendienteSemana.numeroRegistros)
            worksheet.write(row, 3, pendienteSemana.piezas)
            worksheet.write(row, 4, pendienteSemana.reetiquetado)
            row=row+1
        worksheet.write(row, 1, 'PEDIDOS ABIERTOS SOLO EN ERP')
        worksheet.write(row, 2, cuadraje.pedidosAbiertos)
        worksheet.write(row, 3, cuadraje.pedidosAbiertosNum)    
        
        workbook.close()

        output.seek(0)

        filename = 'Cuadraje.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getRecibosTienda(request, fechaInicio, fechaFin):
    try:
        recepcionTiendaDao=RecepcionTiendaDao()
        recibosTiendaList = recepcionTiendaDao.getRecibosTienda(fechaInicio, fechaFin)
        serializer=ReciboTiendaSerializer(recibosTiendaList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getRecibosTiendaFile(request, fechaInicio, fechaFin):
    try:
        recepcionTiendaDao=RecepcionTiendaDao()
        recibosTiendaList = recepcionTiendaDao.getRecibosTienda(fechaInicio, fechaFin)
        
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'CARGA')
        worksheet.write(0, 1, 'PEDIDO')
        worksheet.write(0, 2, 'LLEGADA DEL TRANSPORTISTA')
        worksheet.write(0, 3, 'INICIO DE SCANEO')
        worksheet.write(0, 4, 'FIN DE SCANEO')
        worksheet.write(0, 5, 'CIERRE DE CAMION')
        
        row=1
        for reciboTienda in recibosTiendaList:
            worksheet.write(row, 0, reciboTienda.carga)
            worksheet.write(row, 1, reciboTienda.pedido)
            worksheet.write(row, 2, reciboTienda.llegadaTransportista.strftime("%d/%m/%Y %H:%M:%S"))
            worksheet.write(row, 3, reciboTienda.inicioScaneo.strftime("%d/%m/%Y %H:%M:%S"))
            worksheet.write(row, 4, reciboTienda.finScaneo.strftime("%d/%m/%Y %H:%M:%S"))
            worksheet.write(row, 5, reciboTienda.cierreCamion.strftime("%d/%m/%Y %H:%M:%S"))
            row=row+1
        
        workbook.close()

        output.seek(0)

        filename = 'ReciboTienda.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getDatosDash(request):
    try:
        mes  = request.query_params.get('mes' , None)
        gastoDistribucion  = request.query_params.get('gastoDistribucion' , None)
        venta  = request.query_params.get('venta' , None)
        contenedoresEmbarcados  = request.query_params.get('contenedoresEmbarcados' , None)
        pedidosEmbarcados  = request.query_params.get('pedidosEmbarcados' , None)
        rentaMensual  = request.query_params.get('rentaMensual' , None)
        inventarioMensual  = request.query_params.get('inventarioMensual' , None)
        dias  = request.query_params.get('dias' , None)
        ontime  = request.query_params.get('ontime' , None)
        fillRate  = request.query_params.get('fillRate' , None)
        leadTime  = request.query_params.get('leadTime' , None)
        dato1RatioEntradas  = request.query_params.get('dato1RatioEntradas' , None)
        dato2RatioEntradas  = request.query_params.get('dato2RatioEntradas' , None)
        dato1RatioSalidas  = request.query_params.get('dato1RatioSalidas' , None)
        dato2RatioSalidas  = request.query_params.get('dato2RatioSalidas' , None)
        ticketsReportados  = request.query_params.get('ticketsReportados' , None)
        piezasReportadas  = request.query_params.get('piezasReportadas' , None)
        rotacionStocks  = request.query_params.get('rotacionStocks' , None)
        stockBajaRotacion  = request.query_params.get('stockBajaRotacion' , None)
        stockSinRotacion  = request.query_params.get('stockSinRotacion' , None)
        
        planeacionDao=PlaneacionDao()
        datosDashList=planeacionDao.getDatosDash(mes, gastoDistribucion, venta, contenedoresEmbarcados, pedidosEmbarcados, rentaMensual, inventarioMensual, dias, ontime, fillRate, leadTime, dato1RatioEntradas, dato2RatioEntradas, dato1RatioSalidas, dato2RatioSalidas, ticketsReportados, piezasReportadas, rotacionStocks, stockBajaRotacion, stockSinRotacion)
        serializer=DatoDashSerializer(datosDashList, many=True)
        
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def updateSolicitudVacaciones(request, folio, estatus):
    try:
        planeacionDAO=PlaneacionDao()
        planeacionDAO.updateSolicitudVacaciones(folio, estatus)
        return Response(status=status.HTTP_200_OK)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
def updateSolicitudPermiso(request, folio, estatus):
    try:
        planeacionDAO=PlaneacionDao()
        planeacionDAO.updateSolicitudPermiso(folio, estatus)
        return Response(status=status.HTTP_200_OK)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    
#####################################Reporte Tiendas CL###########################################################

@api_view(['GET'])
def getTiendasPendientesCl(request):
    try:
        recepcionTiendaDao=RecepcionTiendaDaoCl()
        tiendasPendientesList=recepcionTiendaDao.getTiendasPendientes()
        serializer=TiendaPendienteSerializer(tiendasPendientesList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def getTiendasPendientesFechaCl(request):
    try:
        recepcionTiendaDao=RecepcionTiendaDaoCl()
        tiendasPendientesFechaList=recepcionTiendaDao.getTiendasPendientesFecha()
        serializer=TiendaPendienteFechaSerializer(tiendasPendientesFechaList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def getPedidosPorCerrarCl(request):
    try:
        recepcionTiendaDao=RecepcionTiendaDaoCl()
        pedidosPorCerrarList=recepcionTiendaDao.getPedidosPorCerrar()
        serializer=PedidoPorCerrarSerializer(pedidosPorCerrarList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def getPedidosSinTrCl(request):
    try:
        recepcionTiendaDao=RecepcionTiendaDaoCl()
        pedidosSinTrList=recepcionTiendaDao.getPedidosSinTr()
        serializer=[]
        #sapDao=SAPDao()
        #infoPedidosSinTrList=sapDao.getInfoTransaccionTR(pedidosSinTrList)
        #serializer=InfoPedidoSinTrSerializer(infoPedidosSinTrList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def executeActualizarTablasTiendasCl(request):
    try:
        logger.error("Llamado actualización de tablas view")
        recepcionTiendaDao=RecepcionTiendaDaoCl()
        recepcionTiendaDao.executeActualizarTablasTiendas()
        return Response(status=status.HTTP_200_OK)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def cerrarPedidosCl(request):
    try:
        recepcionTiendaDao=RecepcionTiendaDaoCl()
        recepcionTiendaDao.cerrarPedidoPendientes()
        return Response(status=status.HTTP_200_OK)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def insertFechaTraficoCl(request, fecha, carga, pedido):
    try:
        logger.error('Inicio de insert')
        recepcionTiendaDao=RecepcionTiendaDaoCl()
        recepcionTiendaDao.insertFechaTrafico(fecha, carga, pedido)
        return Response(status=status.HTTP_200_OK)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def getPedidoTiendaCl(request, tienda, carga, pedido):
    try:
        tienda=tienda.strip()
        carga=carga.strip()
        pedido=pedido.strip()
        if tienda=='' and carga=='' and pedido =='':
            return Response({'Debe ingresar al menos un parametro'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)    
        recepcionTiendaDao=RecepcionTiendaDaoCl()
        pedidoList=recepcionTiendaDao.getPedido(tienda, carga, pedido)
        serializer=PedidoTiendaSerializer(pedidoList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def getDetallePedidoTiendaCl(request, tienda, carga, pedido, contenedor, articulo, estatusContenedor):
    try:
        tienda=tienda.strip()
        carga=carga.strip()
        pedido=pedido.strip()
        contenedor=contenedor.strip()
        articulo=articulo.strip()
        estatusContenedor=estatusContenedor.strip()
        if tienda=='' and carga=='' and pedido =='' and contenedor=='' and articulo=='':
            return Response({'Debe ingresar al menos un parametro'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)    
        recepcionTiendaDao=RecepcionTiendaDaoCl()
        detallePedidoList=recepcionTiendaDao.getDetallePedido(tienda, carga, pedido, contenedor, articulo, estatusContenedor)
        serializer=DetallePedidoTiendaClSerializer(detallePedidoList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def getDetallePedidoTiendaFileCl(request, tienda, carga, pedido, contenedor, articulo, estatusContenedor):
    try:
        tienda=tienda.strip()
        carga=carga.strip()
        pedido=pedido.strip()
        contenedor=contenedor.strip()
        articulo=articulo.strip()
        estatusContenedor=estatusContenedor.strip()
        if tienda=='' and carga=='' and pedido =='' and contenedor=='' and articulo=='':
            return Response({'Debe ingresar al menos un parametro'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR) 
        
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'ESTATUS PEDIDO')
        worksheet.write(0, 1, 'TIENDA')
        worksheet.write(0, 2, 'CARGA')
        worksheet.write(0, 3, 'PEDIDO')
        worksheet.write(0, 4, 'CONTENEDOR')
        worksheet.write(0, 5, 'ITEM')
        worksheet.write(0, 6, 'DESCRIPCION ITEM')
        worksheet.write(0, 7, 'ESTATUS CONTENEDOR')
        worksheet.write(0, 8, 'PIEZAS')

        recepcionTiendaDao=RecepcionTiendaDaoCl()
        detallePedidoList=recepcionTiendaDao.getDetallePedido(tienda, carga, pedido, contenedor, articulo, estatusContenedor)
        
        row=1
        for detallePedido in detallePedidoList:
            worksheet.write(row, 0, detallePedido.estatusPedido)
            worksheet.write(row, 1, detallePedido.tienda)
            worksheet.write(row, 2, detallePedido.carga)
            worksheet.write(row, 3, detallePedido.pedido)
            worksheet.write(row, 4, detallePedido.contenedor)
            worksheet.write(row, 5, detallePedido.item)
            worksheet.write(row, 6, detallePedido.itemDescripcion)
            worksheet.write(row, 7, detallePedido.estatusContenedor)
            worksheet.write(row, 8, detallePedido.piezas)
            row=row+1
        workbook.close()

        output.seek(0)

        filename = 'DetallePedido.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def getSolicitudTrasladoCl(request, documento, origenSolicitud, destinoSolicitud, origenTraslado, destinoTraslado):
    try:
        logger.error("Acceso al servicio")
        documento=documento.strip()
        origenSolicitud=origenSolicitud.strip()
        destinoSolicitud=destinoSolicitud.strip()
        origenTraslado=origenTraslado.strip()
        destinoTraslado=destinoTraslado.strip()
        if documento=='' and origenSolicitud=='' and destinoSolicitud =='' and origenTraslado=='' and destinoTraslado=='':
            return Response({'Debe ingresar al menos un parametro'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)    
        recepcionTiendaDao=RecepcionTiendaDaoCl()
        solicitudTrasladoList=recepcionTiendaDao.getSolicitudTraslado(documento, origenSolicitud, destinoSolicitud, origenTraslado, destinoTraslado)
        serializer=SolicitudTrasladoSerializer(solicitudTrasladoList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getTablaTiendasPendientesCl(request, fecha):
    try:
        recepcionTiendaDao=RecepcionTiendaDaoCl()
        tablaTiendasPendientesList=recepcionTiendaDao.getTablaTiendasPendientes(fecha)
        serializer=TablaTiendaPendienteSerializer(tablaTiendasPendientesList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getTiendasPendientesAllCl(request):
    try:
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'SOLICITUD ESTATUS')
        worksheet.write(0, 1, 'CARGA')
        worksheet.write(0, 2, 'PEDIDO')
        worksheet.write(0, 3, 'NOMBRE ALMACEN')
        worksheet.write(0, 4, 'FECHA EMBARQUE')
        worksheet.write(0, 5, 'FECHA PLANEADA')
        worksheet.write(0, 6, 'TRANSITO')
        worksheet.write(0, 7, 'CROSS DOCK')
        worksheet.write(0, 8, 'FECHA ENTREGA')
        
        recepcionTiendaDao=RecepcionTiendaDaoCl()
        tiendasPendientesList=recepcionTiendaDao.getTiendasPendientesAll()
        
        row=1
        for tiendaPendiente in tiendasPendientesList:
            worksheet.write(row, 0, tiendaPendiente.solicitudEstatus)
            worksheet.write(row, 1, tiendaPendiente.carga)
            worksheet.write(row, 2, tiendaPendiente.pedido)
            worksheet.write(row, 3, tiendaPendiente.nombreAlmacen)
            worksheet.write(row, 4, tiendaPendiente.fechaEmbarque)
            worksheet.write(row, 5, tiendaPendiente.fechaPlaneada)
            worksheet.write(row, 6, tiendaPendiente.transito)
            worksheet.write(row, 7, tiendaPendiente.crossDock)
            worksheet.write(row, 8, tiendaPendiente.fechaEntrega)
            row=row+1
        workbook.close()

        output.seek(0)

        filename = 'tiendasPendientes.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getTiendasPendientesCorreoCl(request):
    try:
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        
        worksheet.set_column('A:A', 20)
        worksheet.set_column('B:C', 10)
        worksheet.set_column('D:D', 40)
        worksheet.set_column('E:I', 16)
        
        cell_formatRed = workbook.add_format()
        cell_formatRed.set_bold()
        cell_formatRed.set_font_color('#C00000')
        
        worksheet.write(0, 1, 'TIENDAS PENDIENTES DE CONFIRMAR', cell_formatRed)
        worksheet.write(0, 9, 'PEND', cell_formatRed)
        worksheet.write(1, 9, 'Σ')

        cell_formatEncabezado = workbook.add_format()
        cell_formatEncabezado.set_bg_color('#76933C')
        cell_formatEncabezado.set_font_color('#FFFFFF')
        worksheet.write(2, 0, 'SOLICITUD ESTATUS', cell_formatEncabezado)
        worksheet.write(2, 1, 'CARGA', cell_formatEncabezado)
        worksheet.write(2, 2, 'PEDIDO', cell_formatEncabezado)
        worksheet.write(2, 3, 'NOMBRE ALMACEN', cell_formatEncabezado)
        worksheet.write(2, 4, 'FECHA EMBARQUE', cell_formatEncabezado)
        worksheet.write(2, 5, 'FECHA PLANEADA', cell_formatEncabezado) 
        worksheet.write(2, 6, 'TRANSITO', cell_formatEncabezado)
        worksheet.write(2, 7, 'CROSS DOCK', cell_formatEncabezado)
        worksheet.write(2, 8, 'FECHA ENTREGA', cell_formatEncabezado)
        
        recepcionTiendaDao=RecepcionTiendaDaoCl()
        tiendasPendientesList=recepcionTiendaDao.getTiendasPendientesCorreo()
        piezasTransito=0
        piezasCrossDock=0
        pedidosTransito=0
        pedidosCrossDock=0

        cell_formatSolRec = workbook.add_format()
        cell_formatSolRec.set_bg_color('#FCD5B4')
        cell_formatSolRec.set_num_format('#,##0')

        cell_formatSolTra = workbook.add_format()
        cell_formatSolTra.set_bg_color('#A9D08E')

        cell_formatCarPedFec = workbook.add_format()
        cell_formatCarPedFec.set_bg_color('#C6E0B4')
        cell_formatCarPedFec.set_align('right')

        cell_formatFec = workbook.add_format()
        cell_formatFec.set_align('right')

        cell_formatNumTra = workbook.add_format()
        cell_formatNumTra.set_bg_color('#B8CCE4')
        cell_formatNumTra.set_num_format('#,##0')
        
        row=3
        for tiendaPendiente in tiendasPendientesList:
            if tiendaPendiente.solicitudEstatus=='REC PARCIAL':
                worksheet.write(row, 0, tiendaPendiente.solicitudEstatus, cell_formatSolRec)    
                worksheet.write(row, 6, tiendaPendiente.transito, cell_formatSolRec)
                worksheet.write(row, 7, tiendaPendiente.crossDock, cell_formatSolRec)
            else:
                worksheet.write(row, 0, tiendaPendiente.solicitudEstatus, cell_formatSolTra)    
                worksheet.write(row, 6, tiendaPendiente.transito, cell_formatNumTra)
                worksheet.write(row, 7, tiendaPendiente.crossDock, cell_formatNumTra)
                
            worksheet.write(row, 1, tiendaPendiente.carga, cell_formatCarPedFec)
            worksheet.write(row, 2, tiendaPendiente.pedido, cell_formatCarPedFec)
            worksheet.write(row, 3, tiendaPendiente.nombreAlmacen)
            worksheet.write(row, 4, tiendaPendiente.fechaEmbarque, cell_formatCarPedFec)
            worksheet.write(row, 5, tiendaPendiente.fechaPlaneada, cell_formatFec)
            worksheet.write(row, 8, tiendaPendiente.fechaEntrega, cell_formatFec)

            if tiendaPendiente.transito>0:
                piezasTransito=piezasTransito+tiendaPendiente.transito
                pedidosTransito=pedidosTransito+1

            if tiendaPendiente.crossDock>0:
                piezasCrossDock=piezasCrossDock+tiendaPendiente.crossDock
                pedidosCrossDock=pedidosCrossDock+1
            
            row=row+1
            
        cell_formatNum = workbook.add_format()
        cell_formatNum.set_num_format('#,##0')

        worksheet.write(0, 6, pedidosTransito)
        worksheet.write(0, 7, pedidosCrossDock)
        worksheet.write(0, 8, pedidosTransito+pedidosCrossDock)
        worksheet.write(1, 6, piezasTransito, cell_formatNum)
        worksheet.write(1, 7, piezasCrossDock, cell_formatNum)
        worksheet.write(1, 8, piezasTransito+piezasCrossDock, cell_formatNum)
        workbook.close()

        output.seek(0)

        filename = 'tiendasPendientesCorreo.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getTablaTiendasPendientesFileCl(request, fecha):
    try:
        recepcionTiendaDao=RecepcionTiendaDaoCl()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'ALMACEN')
        worksheet.write(0, 1, 'FECHA ENTREGA')
        worksheet.write(0, 2, 'NUMERO PEDIDOS')
        worksheet.write(0, 3, 'PIEZAS')
        worksheet.write(0, 4, 'CONTENEDORES')

        tablaTiendasPendientesList=recepcionTiendaDao.getTablaTiendasPendientes(fecha)
        
        row=1
        for tiendaPendiente in tablaTiendasPendientesList:
            worksheet.write(row, 0, tiendaPendiente.nombreAlmacen)
            worksheet.write(row, 1, tiendaPendiente.fechaEntrega)
            worksheet.write(row, 2, tiendaPendiente.numPedidos)
            worksheet.write(row, 3, tiendaPendiente.piezas)
            worksheet.write(row, 4, tiendaPendiente.contenedores)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'TablaTiendasPendientes.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getDiferenciasFileCl(request):
    try:
        
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 2, 'Cerradas SAP, pendientes en APP')
        worksheet.write(1, 0, 'PEDIDO')
        worksheet.write(1, 1, 'ESTATUS PEDIDO')
        worksheet.write(1, 2, 'TIENDA')
        worksheet.write(1, 3, 'CARGA')
        worksheet.write(1, 4, 'DOCUMENTO')
        worksheet.write(1, 5, 'ESTATUS DOCUMENTO')
        
        recepcionTiendaDao=RecepcionTiendaDaoCl()
        pedidosPorCerrarList=recepcionTiendaDao.getPedidosPorCerrar()
        
        row=2
        for pedidoPorCerrar in pedidosPorCerrarList:
            worksheet.write(row, 0, pedidoPorCerrar.pedido)
            worksheet.write(row, 1, pedidoPorCerrar.estatusPedido)
            worksheet.write(row, 2, pedidoPorCerrar.tienda)
            worksheet.write(row, 3, pedidoPorCerrar.carga)
            worksheet.write(row, 4, pedidoPorCerrar.documento)
            worksheet.write(row, 5, pedidoPorCerrar.estatusDocumento)
            row=row+1
        
        row=row+2
        worksheet.write(row, 2, 'Sin Solicitud TR')
        row=row+1
        worksheet.write(row, 0, 'PEDIDO')
        worksheet.write(row, 1, 'TIENDA')
        worksheet.write(row, 2, 'ALMACEN')
        worksheet.write(row, 3, 'BNEXT')
        worksheet.write(row, 4, 'DOCK ENTRY')

        recepcionTiendaDao=RecepcionTiendaDaoCl()
        pedidosSinTrList=recepcionTiendaDao.getPedidosSinTr()
        infoPedidosSinTrList=[]
#        sapDao=SAPDao()
#        infoPedidosSinTrList=sapDao.getInfoTransaccionTR(pedidosSinTrList)

        row=row+1
        for infoPedidoSinTr in infoPedidosSinTrList:
            worksheet.write(row, 0, infoPedidoSinTr.pedido)
            worksheet.write(row, 1, infoPedidoSinTr.tienda)
            worksheet.write(row, 2, infoPedidoSinTr.almacen)
            worksheet.write(row, 3, infoPedidoSinTr.bnext)
            worksheet.write(row, 4, infoPedidoSinTr.dockEntry)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = f'Diferencias_{date.today()}.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def storagesTemplatesCL(request):
    try:
        sapDao=SAPDao()
        storagesTemplatesList=sapDao.getStorageTemplatesCL('100')
        serializer=StorageTemplateSerializer(storagesTemplatesList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def descargaStoragesCL(request):
    try:
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'SKU')
        worksheet.write(0, 1, 'Storage Template')
        worksheet.write(0, 2, 'Grupo Logistico')
        worksheet.write(0, 3, 'Unidad')
        worksheet.write(0, 4, 'Familia')
        worksheet.write(0, 5, 'Subfamilia')
        worksheet.write(0, 6, 'Subsubfamilia')
        worksheet.write(0, 7, 'uSysCat4')
        worksheet.write(0, 8, 'uSysCat5')
        worksheet.write(0, 9, 'uSysCat6')
        worksheet.write(0, 10, 'uSysCat7')
        worksheet.write(0, 11, 'uSysCat8')
        worksheet.write(0, 12, 'Height')
        worksheet.write(0, 13, 'Width')
        worksheet.write(0, 14, 'Length')
        worksheet.write(0, 15, 'Volume')
        worksheet.write(0, 16, 'Weight')
        sapDao=SAPDao()
        storagesTemplatesList=sapDao.getStorageTemplatesCL('')

        row=1
        for storageTemplate in storagesTemplatesList:
            worksheet.write(row, 0, storageTemplate.itemCode)
            worksheet.write(row, 1, storageTemplate.storageTemplate)
            worksheet.write(row, 2, storageTemplate.grupoLogistico)
            worksheet.write(row, 3, storageTemplate.salUnitMsr)
            worksheet.write(row, 4, storageTemplate.familia)
            worksheet.write(row, 5, storageTemplate.subFamilia)
            worksheet.write(row, 6, storageTemplate.subSubFamilia)
            worksheet.write(row, 7, storageTemplate.uSysCat4)
            worksheet.write(row, 8, storageTemplate.uSysCat5)
            worksheet.write(row, 9, storageTemplate.uSysCat6)
            worksheet.write(row, 10, storageTemplate.uSysCat7)
            worksheet.write(row, 11, storageTemplate.uSysCat8)
            worksheet.write(row, 12, storageTemplate.height)
            worksheet.write(row, 13, storageTemplate.width)
            worksheet.write(row, 14, storageTemplate.length)
            worksheet.write(row, 15, storageTemplate.volume)
            worksheet.write(row, 16, storageTemplate.weight)
            row=row+1
        workbook.close()

        output.seek(0)

        filename = 'StorageTemplate.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getTransaccionesCL(request):
    try:
        wmsCLDao=WMSCLDao()
        transaccionesList=wmsCLDao.getTransaccionesPickPut(True)
        serializer=TransaccionesPickPutSerializer(transaccionesList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getTransaccionesFileCL(request):
    try:
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'ITEM')
        worksheet.write(0, 1, 'TRANSACCION')
        worksheet.write(0, 2, 'NOMBRE USUARIO')
        worksheet.write(0, 3, 'REFERENCIA')
        worksheet.write(0, 4, 'FECHA')
        worksheet.write(0, 5, 'TIPO TRAFICO')
        worksheet.write(0, 6, 'USUARIO')
        worksheet.write(0, 7, 'UBICACION')
        worksheet.write(0, 8, 'CANTIDAD')
        worksheet.write(0, 9, 'ANTES EN TRANSITO')
        worksheet.write(0, 10, 'DESPUES EN TRANSITO')
        worksheet.write(0, 11, 'ANTES A MANO')
        worksheet.write(0, 12, 'DESPUES A MANO')
        worksheet.write(0, 13, 'ANTES COMPROMETIDO')
        worksheet.write(0, 14, 'DESPUES COMPROMETIDO')
        worksheet.write(0, 15, 'ANTES SUSPENSO')
        worksheet.write(0, 16, 'DESPUES SUSPENSO')
        wmsCLDao=WMSCLDao()
        transaccionesList=wmsCLDao.getTransaccionesPickPut(False)

        row=1
        for transaccion in transaccionesList:
            worksheet.write(row, 0, transaccion.item)
            worksheet.write(row, 1, transaccion.transaccion)
            worksheet.write(row, 2, transaccion.nombreUsuario)
            worksheet.write(row, 3, transaccion.refrencia)
            worksheet.write(row, 4, transaccion.fecha)
            worksheet.write(row, 5, transaccion.tipoTrabajo)
            worksheet.write(row, 6, transaccion.usuario)
            worksheet.write(row, 7, transaccion.ubicacion)
            worksheet.write(row, 8, transaccion.cantidad)
            worksheet.write(row, 9, transaccion.antesEnTransito)
            worksheet.write(row, 10, transaccion.despuesEnTransito)
            worksheet.write(row, 11, transaccion.antesAMano)
            worksheet.write(row, 12, transaccion.despuesAMano)
            worksheet.write(row, 13, transaccion.antesComprometido)
            worksheet.write(row, 14, transaccion.despuesComprometido)
            worksheet.write(row, 15, transaccion.antesSuspenso)
            worksheet.write(row, 16, transaccion.despuesSuspenso)
            row=row+1
        workbook.close()

        output.seek(0)

        filename = 'TransaccionesPickPut.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getLineasOlaCl(request, ola):
    try:
        wmsCLDao=WMSCLDao()
        olaList=wmsCLDao.getLineasOla(True, ola)
        serializer=LineaOlaSerializer(olaList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getLineasOlaFileCl(request, ola):
    try:
        wmsCLDao=WMSCLDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'OLA')
        worksheet.write(0, 1, 'PEDIDO')
        worksheet.write(0, 2, 'ITEM')
        worksheet.write(0, 3, 'DESCRIPCION')
        worksheet.write(0, 4, 'TOTAL')
        worksheet.write(0, 5, 'STATUS')
        
        olaList=wmsCLDao.getLineasOla(False, ola)
        
        row=1
        for lineaOla in olaList:
            worksheet.write(row, 0, lineaOla.ola)
            worksheet.write(row, 1, lineaOla.pedido)
            worksheet.write(row, 2, lineaOla.item)
            worksheet.write(row, 3, lineaOla.descripcion)
            worksheet.write(row, 4, lineaOla.total)
            worksheet.write(row, 5, lineaOla.status)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'LineasOla'+ola+'.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getOlaPiezasContenedoresCl(request):
    try:
        wmsCLDao=WMSCLDao()
        olaList=wmsCLDao.getOlaPiezasContenedores()
        serializer=OlaPiezasContenedoresSerializer(olaList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getOlaPiezasContenedoresFileCl(request):
    try:
        wmsCLDao=WMSCLDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'OLA')
        worksheet.write(0, 1, 'NUMERO_PIEZAS')
        worksheet.write(0, 2, 'NUMERO_CONTENEDORES')
        
        olaList=wmsCLDao.getOlaPiezasContenedores()
        
        row=1
        for ola in olaList:
            worksheet.write(row, 0, ola.ola)
            worksheet.write(row, 1, ola.numPiezas)
            worksheet.write(row, 2, ola.numContenedores)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'OlaPiezasContenedoresCl.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getWaveAnalysisCl(request, wave):
    try:
        wmsCLDao=WMSCLDao()
        waveList=wmsCLDao.getWaveAnalysis(True, wave)
        serializer=WaveSerializer(waveList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getWaveAnalysisFileCl(request, wave):
    try:
        wmsCLDao=WMSCLDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'ITEM')
        worksheet.write(0, 1, 'DESCRIPCION')
        worksheet.write(0, 2, 'STORAGE TEMPLATE')
        worksheet.write(0, 3, 'SHIPMENT ID')
        worksheet.write(0, 4, 'LAUNCH NUM')
        worksheet.write(0, 5, 'STATUS')
        worksheet.write(0, 6, 'REQUESTED QTY')
        worksheet.write(0, 7, 'ALLOCATED QTY')
        worksheet.write(0, 8, 'AV')
        worksheet.write(0, 9, 'OH')
        worksheet.write(0, 10, 'AL')
        worksheet.write(0, 11, 'IT')
        worksheet.write(0, 12, 'SU')
        worksheet.write(0, 13, 'CUSTOMER')
        worksheet.write(0, 14, 'ITEM CATEGORY')
        worksheet.write(0, 15, 'CREATION DATE')
        worksheet.write(0, 16, 'SCHEDULED SHIP DATE')
        worksheet.write(0, 17, 'DIVISION')
        worksheet.write(0, 18, 'CONV')
        
        waveList=wmsCLDao.getWaveAnalysis(False, wave)
    
        row=1
        for wav in waveList:
            worksheet.write(row, 0, wav.item)
            worksheet.write(row, 1, wav.description)
            worksheet.write(row, 2, wav.storageTemplate)
            worksheet.write(row, 3, wav.shipmentId)
            worksheet.write(row, 4, wav.launchNum)
            worksheet.write(row, 5, wav.status)
            worksheet.write(row, 6, wav.requestedQty)
            worksheet.write(row, 7, wav.allocatedQty)
            worksheet.write(row, 8, wav.av)
            worksheet.write(row, 9, wav.oh)
            worksheet.write(row, 10, wav.al)
            worksheet.write(row, 11, wav.it)
            worksheet.write(row, 12, wav.su)
            worksheet.write(row, 13, wav.customer)
            worksheet.write(row, 14, wav.itemCategory)
            worksheet.write(row, 15, wav.creationDateTimeStamp)
            worksheet.write(row, 16, wav.scheduledShipDate)
            worksheet.write(row, 17, wav.division)
            worksheet.write(row, 18, wav.conv)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'WaveAnalysisCl'+wave+'.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getSplitsCl(request):
    try:
        wmsCLDao=WMSCLDao()
        splitList=wmsCLDao.getSplit()
        serializer=SplitClSerializer(splitList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getSplitsFileCl(request):
    try:
        wmsCLDao=WMSCLDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'PEDIDO')
        worksheet.write(0, 1, 'FECHA CREACIÓN')
        worksheet.write(0, 2, 'NUMERO CONTENEDORES')
        
        splitList=wmsCLDao.getSplit()
    
        row=1
        for split in splitList:
            worksheet.write(row, 0, split.pedido)
            worksheet.write(row, 1, split.fechaCreacion)
            worksheet.write(row, 2, split.numeroContenedores)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'SplitCl.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getUnitMesureLocation(request, location, item):
    try:
        wmsCLDao=WMSCLDao()
        unitMesure=wmsCLDao.getUnitMesureLocation(location, item)
        serializer=UnitMesureSerializer(unitMesure, many=False)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def preciosCl(request):
    try:
        sapDao=SAPDaoClII()
        preciosList=sapDao.getPrecios('100')

        serializer=PrecioClSerializer(preciosList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def descargaPreciosCl(request):
    try:
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'SKU')
        worksheet.write(0, 1, 'Codigos de Barras')
        worksheet.write(0, 2, 'Categoria')
        worksheet.write(0, 3, 'Subcategoria')
        worksheet.write(0, 4, 'Clase')
        worksheet.write(0, 5, 'Descripción')
        worksheet.write(0, 6, 'Storage Template')
        worksheet.write(0, 7, 'Storage Template User')
        worksheet.write(0, 8, 'Licencia')
        worksheet.write(0, 9, 'Height')
        worksheet.write(0, 10, 'Width')
        worksheet.write(0, 11, 'Length')
        worksheet.write(0, 12, 'Volume')
        worksheet.write(0, 13, 'Weight')
        worksheet.write(0, 14, 'Precio Sin Ivan')
        worksheet.write(0, 15, 'Precio Con Ivan')
        worksheet.write(0, 16, 'Precio Linea Sin Ivan')
        worksheet.write(0, 17, 'Precio Linea Con Ivan')
        worksheet.write(0, 18, 'Proveedor')
        sapDao=SAPDaoClII()
        preciosList=sapDao.getPrecios('')

        row=1
        for precio in preciosList:
            worksheet.write(row, 0, precio.itemCode)
            worksheet.write(row, 1, precio.codigoBarras)
            worksheet.write(row, 2, precio.categoria)
            worksheet.write(row, 3, precio.subcategoria)
            worksheet.write(row, 4, precio.clase)
            worksheet.write(row, 5, precio.itemName)
            worksheet.write(row, 6, precio.storageTemplate)
            worksheet.write(row, 7, precio.stUsr)
            worksheet.write(row, 8, precio.licencia)
            worksheet.write(row, 9, precio.height)
            worksheet.write(row, 10, precio.width)
            worksheet.write(row, 11, precio.length)
            worksheet.write(row, 12, precio.volume)
            worksheet.write(row, 13, precio.weight)
            worksheet.write(row, 14, precio.precioSinIva)
            worksheet.write(row, 15, precio.precioIva)
            worksheet.write(row, 16, precio.precioLineaSinIva)
            worksheet.write(row, 17, precio.precioLineaIva)
            worksheet.write(row, 18, precio.proveedor)
            row=row+1
        workbook.close()

        output.seek(0)

        filename = 'Precios.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getTareasReaSurtAbiertasFileCl(request):
    try:
        wmsDao=WMSCLDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'WORK_UNIT')
        worksheet.write(0, 1, 'INSTRUCTION_TYPE')
        worksheet.write(0, 2, 'WORK_TYPE')
        worksheet.write(0, 3, 'WAVEPACK')
        worksheet.write(0, 4, 'FAMILIA')
        worksheet.write(0, 5, 'CONDICION')
        worksheet.write(0, 6, 'ITEM')
        worksheet.write(0, 7, 'ITEM_DESC')
        worksheet.write(0, 8, 'REFERENCE_ID')
        worksheet.write(0, 9, 'FROM_LOC')
        worksheet.write(0, 10, 'FROM_QTY')
        worksheet.write(0, 11, 'TO_LOC')
        worksheet.write(0, 12, 'TO_QTY')
        worksheet.write(0, 13, 'OLA')
        worksheet.write(0, 14, 'NUMERO_INTERNO_INSTRUCCION')
        worksheet.write(0, 15, 'CONVERTED_QTY')
        worksheet.write(0, 16, 'CONTENEDOR')
        worksheet.write(0, 17, 'TIPO_CONTENEDOR')
        worksheet.write(0, 18, 'AGING_DATE_TIME')
        worksheet.write(0, 19, 'START_DATE_TIME')
        
        tareasList=wmsDao.getTareasReaSurtAbiertas()
        
        row=1
        for tarea in tareasList:
            worksheet.write(row, 0, tarea.workUnit)
            worksheet.write(row, 1, tarea.instructionType)
            worksheet.write(row, 2, tarea.workType)
            worksheet.write(row, 3, tarea.wavepack)
            worksheet.write(row, 4, tarea.familia)
            worksheet.write(row, 5, tarea.condicion)
            worksheet.write(row, 6, tarea.item)
            worksheet.write(row, 7, tarea.itemDesc)
            worksheet.write(row, 8, tarea.referenceId)
            worksheet.write(row, 9, tarea.fromLoc)
            worksheet.write(row, 10, tarea.fromQty)
            worksheet.write(row, 11, tarea.toLoc)
            worksheet.write(row, 12, tarea.toQty)
            worksheet.write(row, 13, tarea.ola)
            worksheet.write(row, 14, tarea.numeroInternoInstruccion)
            worksheet.write(row, 15, tarea.convertedQty)
            worksheet.write(row, 16, tarea.numConteneder)
            worksheet.write(row, 17, tarea.tipoContenedor)
            worksheet.write(row, 18, tarea.agingDateTime)
            worksheet.write(row, 19, tarea.startDateTime)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'TareasAbiertas.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getContenedoresFileCl(request):
    try:
        wmsDao=WMSCLDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'CONTAINER_ID')
        worksheet.write(0, 1, 'SHIPMENT_ID')
        worksheet.write(0, 2, 'CONTAINER_TYPE')
        worksheet.write(0, 3, 'LEADING_STS')
        worksheet.write(0, 4, 'ITEM')
        worksheet.write(0, 5, 'QUANTITY')
        worksheet.write(0, 6, 'CUSTOMER')
        worksheet.write(0, 7, 'LAUNCH_NUM')
        worksheet.write(0, 8, 'FECHA')
        
        contenedoresList=wmsDao.getContenedores()
        
        row=1
        for contenedor in contenedoresList:
            worksheet.write(row, 0, contenedor.containerId)
            worksheet.write(row, 1, contenedor.shipmentId)
            worksheet.write(row, 2, contenedor.containerType)
            worksheet.write(row, 3, contenedor.leadingSts)
            worksheet.write(row, 4, contenedor.item)
            worksheet.write(row, 5, contenedor.quantity)
            worksheet.write(row, 6, contenedor.customer)
            worksheet.write(row, 7, contenedor.launchNum)
            worksheet.write(row, 8, contenedor.fecha)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'Contenedores.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getDiferenciasDetalleReciboCl(request):
    try:
        monitoreoDao=MonitoreoDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'RECIBO')
        worksheet.write(0, 1, 'ITEM')
        worksheet.write(0, 2, 'CANTIDAD WMS')
        worksheet.write(0, 3, 'CANTIDAD SAP')
        worksheet.write(0, 4, 'DIFERENCIA')
        
        reciboDetalleList=monitoreoDao.getCuadrajeReciboDetalleCl()
        
        row=1
        for reciboDetalle in reciboDetalleList:
            worksheet.write(row, 0, reciboDetalle.recibo)
            worksheet.write(row, 1, reciboDetalle.item)
            worksheet.write(row, 2, reciboDetalle.cantidadWms)
            worksheet.write(row, 3, reciboDetalle.cantidadSap)
            worksheet.write(row, 4, (reciboDetalle.cantidadWms-reciboDetalle.cantidadSap))
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'DetalleRecibos.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getReciboPendientesCl(request):
    try:
        wmsDao=WMSCLDao()
        recibosPendientesList=wmsDao.getRecibosPendientes()
        serializer=ReciboPendienteSerializer(recibosPendientesList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def decargaReciboPendientesCl(request):
    try:
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'Identificador de Recibo')
        worksheet.write(0, 1, 'item')
        worksheet.write(0, 2, 'Descripción de Item')
        worksheet.write(0, 3, 'Cantidad Total')
        worksheet.write(0, 4, 'Cantidad Abierta')
        wmsDao=WMSCLDao()
        recibosPendientesList=wmsDao.getRecibosPendientes()

        row=1
        for reciboPendiente in recibosPendientesList:
            worksheet.write(row, 0, reciboPendiente.receiptId)
            worksheet.write(row, 1, reciboPendiente.item)
            worksheet.write(row, 2, reciboPendiente.itemDesc)
            worksheet.write(row, 3, reciboPendiente.totalQty)
            worksheet.write(row, 4, reciboPendiente.openQty)
            row=row+1
        workbook.close()

        output.seek(0)

        filename = 'RecibosPendientes.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getUbicacionesVaciasTopCl(request):
    try:
        wmsDao=WMSCLDao()
        ubicacionesList=wmsDao.getUbicacionesVaciasReservaTop()
        serializer=UbicacionVaciaSerializer(ubicacionesList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getUbicacionesVaciasFileCl(request):
    try:
        wmsDao=WMSCLDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'UBICACION')
        worksheet.write(0, 1, 'ESTATUS')
        worksheet.write(0, 2, 'ACTIVO')
        
        ubicacionesList=wmsDao.getUbicacionesVaciasAll()
        
        row=1
        for ubicacion in ubicacionesList:
            worksheet.write(row, 0, ubicacion.ubicacion)
            worksheet.write(row, 1, ubicacion.status)
            worksheet.write(row, 2, ubicacion.active)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'ubicacionesVacias.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getEstatusOlaCl(request):
    try:
        wmsDao=WMSCLDao()
        estatusContenedorList=wmsDao.getEstatusContenedores()
        serializer=EstatusContendorSerializer(estatusContenedorList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getEstatusOlaFileCl(request):
    try:
        wmsDao=WMSCLDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'SEMANA')
        worksheet.write(0, 1, 'OLA')
        worksheet.write(0, 2, 'PEDIDOS')
        worksheet.write(0, 3, 'CONTENEDORES')
        worksheet.write(0, 4, 'CAJAS')
        worksheet.write(0, 5, 'BOLSAS')
        worksheet.write(0, 6, 'PICKING PENDING')
        worksheet.write(0, 7, 'IN PICKING')
        worksheet.write(0, 8, 'PACKING PENDING')
        worksheet.write(0, 9, 'IN PACKING')
        worksheet.write(0, 10, 'STAGING PENDING')
        worksheet.write(0, 11, 'LOADING PEDING')
        worksheet.write(0, 12, 'SHIP CONFIRM PENDING')
        worksheet.write(0, 13, 'LOAD CONFIRM PENDING')
        worksheet.write(0, 14, 'CLOSED')
        worksheet.write(0, 15, 'CUMPLIMIENTO')
        
        estatusContenedorList=wmsDao.getEstatusContenedores()
        
        row=1
        for estatusOla in estatusContenedorList:
            worksheet.write(row, 0, estatusOla.semana)
            worksheet.write(row, 1, estatusOla.ola)
            worksheet.write(row, 2, estatusOla.pedidos)
            worksheet.write(row, 3, estatusOla.contenedores)
            worksheet.write(row, 4, estatusOla.carton)
            worksheet.write(row, 5, estatusOla.bolsa)
            worksheet.write(row, 6, estatusOla.pickingPending)
            worksheet.write(row, 7, estatusOla.inPicking)
            worksheet.write(row, 8, estatusOla.packingPending)
            worksheet.write(row, 9, estatusOla.inPacking)
            worksheet.write(row, 10, estatusOla.stagingPending)
            worksheet.write(row, 11, estatusOla.loadingPending)
            worksheet.write(row, 12, estatusOla.shipConfirmPending)
            worksheet.write(row, 13, estatusOla.loadConfirmPending)
            worksheet.write(row, 14, estatusOla.closed)
            worksheet.write(row, 15, str("{0:.2f}".format(estatusOla.closed*100/estatusOla.contenedores))+"%")
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'estatusOla.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getDetalleEstatusContenedoresCl(request):
    try:
        
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'OLA')
        worksheet.write(0, 1, 'PEDIDO')
        worksheet.write(0, 2, 'CONTENEDOR')
        worksheet.write(0, 3, 'ESTATUS')
        worksheet.write(0, 4, 'TIPO')
        
        wmsDao=WMSCLDao()
        detalleList=wmsDao.getDetalleEstatusContenedores()

        row=1
        for detalle in detalleList:
            worksheet.write(row, 0, detalle.ola)
            worksheet.write(row, 1, detalle.pedido)
            worksheet.write(row, 2, detalle.contenedor)
            worksheet.write(row, 3, detalle.estatus)
            worksheet.write(row, 4, detalle.tipo)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'DetalleEstatusContenedores.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getInventarioAlmacenajeFileCl(request):
    try:
        wmsDao=WMSCLDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'UBICACIÓN')
        worksheet.write(0, 1, 'PERMANENTE')
        worksheet.write(0, 2, 'ACTIVA')
        worksheet.write(0, 3, 'ZONA')
        worksheet.write(0, 4, 'ITEM')
        worksheet.write(0, 5, 'DESCRIPCIÓN')
        worksheet.write(0, 6, 'ESTATUS')
        worksheet.write(0, 7, 'DISPONIBLE')
        worksheet.write(0, 8, 'EXISTENTE')
        worksheet.write(0, 9, 'EN TRANSITO')
        worksheet.write(0, 10, 'COMPROMETIDO')
        worksheet.write(0, 11, 'SUSPENDIDO')
        worksheet.write(0, 12, 'FAMILIA')
        worksheet.write(0, 13, 'SUB FAMILIA')
        worksheet.write(0, 14, 'SUB SUB FAMILIA')
        
        inventarioList=wmsDao.getInventario()
        
        row=1
        for inventario in inventarioList:
            worksheet.write(row, 0, inventario.ubicacion)
            worksheet.write(row, 1, inventario.permanente)
            worksheet.write(row, 2, inventario.activa)
            worksheet.write(row, 3, inventario.zona)
            worksheet.write(row, 4, inventario.item)
            worksheet.write(row, 5, inventario.descripcion)
            worksheet.write(row, 6, inventario.estatus)
            worksheet.write(row, 7, inventario.disponible)
            worksheet.write(row, 8, inventario.existente)
            worksheet.write(row, 9, inventario.transito)
            worksheet.write(row, 10, inventario.comprometido)
            worksheet.write(row, 11, inventario.suspendido)
            worksheet.write(row, 12, inventario.familia)
            worksheet.write(row, 13, inventario.subfamila)
            worksheet.write(row, 14, inventario.subsubfamila)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'InventarioSurtido.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getRecibosTiendaCl(request, fechaInicio, fechaFin):
    try:
        recepcionTiendaDao=RecepcionTiendaDaoCl()
        recibosTiendaList = recepcionTiendaDao.getRecibosTienda(fechaInicio, fechaFin)
        serializer=ReciboTiendaSerializer(recibosTiendaList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getRecibosTiendaFileCl(request, fechaInicio, fechaFin):
    try:
        recepcionTiendaDao=RecepcionTiendaDaoCl()
        recibosTiendaList = recepcionTiendaDao.getRecibosTienda(fechaInicio, fechaFin)
        
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'CARGA')
        worksheet.write(0, 1, 'PEDIDO')
        worksheet.write(0, 2, 'LLEGADA DEL TRANSPORTISTA')
        worksheet.write(0, 3, 'INICIO DE SCANEO')
        worksheet.write(0, 4, 'FIN DE SCANEO')
        worksheet.write(0, 5, 'CIERRE DE CAMION')
        
        row=1
        for reciboTienda in recibosTiendaList:
            worksheet.write(row, 0, reciboTienda.carga)
            worksheet.write(row, 1, reciboTienda.pedido)
            worksheet.write(row, 2, reciboTienda.llegadaTransportista.strftime("%d/%m/%Y %H:%M:%S"))
            worksheet.write(row, 3, reciboTienda.inicioScaneo.strftime("%d/%m/%Y %H:%M:%S"))
            worksheet.write(row, 4, reciboTienda.finScaneo.strftime("%d/%m/%Y %H:%M:%S"))
            worksheet.write(row, 5, reciboTienda.cierreCamion.strftime("%d/%m/%Y %H:%M:%S"))
            row=row+1
        
        workbook.close()

        output.seek(0)

        filename = 'ReciboTienda.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getPorcentajeSKUsPrioritariosCl(request, container):
    try:
        wmsDao=WMSCLDao()
        porcentaje=wmsDao.getPorcentajeSKUsPrioritarios(container)
        serializer=PorcentajeSkusPrioritariosSerializer(porcentaje, many=False)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getTiendasCl(request):
    try:
        recepcionTiendaDaoCl=RecepcionTiendaDaoCl()
        listadoTiendas=recepcionTiendaDaoCl.getListadoTiendas()
        serializer=ListadoTiendasSerializer(listadoTiendas, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getAuditoriasTiendaCl(request, tienda, fechaInicio, fechaFin):
    try:
        recepcionTiendaDaoCl=RecepcionTiendaDaoCl()
        auditoriaList=recepcionTiendaDaoCl.getAuditoriaTienda(tienda, fechaInicio, fechaFin)
        serializer=AuditoriaTiendaClSerializerCl(auditoriaList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    

@api_view(['GET'])
def getdownloadAuditoriaTiendaCl(request, tienda, fechaInicio, fechaFin): #new
    try:
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet(tienda)

        worksheet.write(0, 0, 'TIENDA')
        # worksheet.write(0, 1, 'PEDIDO')
        # worksheet.write(0, 1, 'CARGA')
        worksheet.write(0, 1, 'FECHA RECEPCION')
        worksheet.write(0, 2, 'TOTAL CONTENEDORES')
        worksheet.write(0, 3, 'CONTENEDORES AUDITADOS')
        worksheet.write(0, 4, 'PORCENTAJE')

        
        recepcionTiendaDaoCl=RecepcionTiendaDaoCl()
        auditoriaList=recepcionTiendaDaoCl.getAuditoriaTienda(tienda, fechaInicio, fechaFin)
    
        row=1
        for auditoria in auditoriaList:
            worksheet.write(row, 0, auditoria.tienda)
            # worksheet.write(row, 1, auditoria.pedido)
            # worksheet.write(row, 1, auditoria.carga)
            worksheet.write(row, 1, auditoria.fechaRecepcion)
            worksheet.write(row, 2, auditoria.totalContenedores)
            worksheet.write(row, 3, auditoria.contenedoresAuditados)
            worksheet.write(row, 4, auditoria.porcentaje)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'Auditoria_' + tienda + '.xlsx' 
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
@api_view(['GET']) #New
def getConfirmacionesPendientesCl(request):
    try:
        recepcionTiendaDaoCl=RecepcionTiendaDaoCl()
        confirmList=recepcionTiendaDaoCl.getConfirmPending()
        serializer=ConfirmacionesPendientes(confirmList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

###############################################################################################################

@api_view(['GET'])
def getContenedoresSalida(request, contenedorSalida):
    try:
        traficoDao=TraficoDao()
        if contenedorSalida==' ':
            contenedorSalida=None
        contenedoresSalidaList=traficoDao.getLastTwentyContenedoresSalida(contenedorSalida)
        serializer=ContenedorSalidaSerializer(contenedoresSalidaList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getContedorDetalleFile(request, idContenedorSalida):
    try:
        traficoDao=TraficoDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'ITEM NO')
        worksheet.write(0, 1, 'FRACCION')
        worksheet.write(0, 2, 'CODIGO PROVEEDOR')
        worksheet.write(0, 3, 'NUMERO DE IDENTIFICACION')
        worksheet.write(0, 4, 'CODIGO SAT')
        worksheet.write(0, 5, 'DESCRIPCION')
        worksheet.write(0, 6, 'DGR')
        worksheet.write(0, 7, 'CANTIDAD')
        worksheet.write(0, 8, 'UNIDAD MEDIDA COMERCIAL')
        worksheet.write(0, 9, 'UNIDAD MEDIDA CAT SAT')
        worksheet.write(0, 10, 'PESO')
        worksheet.write(0, 11, 'MEDIDAS Y ALMACENAJE')
        worksheet.write(0, 12, 'EMBALAJE')
        worksheet.write(0, 13, 'PEDIMENTO')
        
        contenedorDetalleList=traficoDao.getContenedorSalidaById(idContenedorSalida)
    
        row=1
        for contenedor in contenedorDetalleList:
            worksheet.write(row, 0, row)
            worksheet.write(row, 1, contenedor.fraccion)
            worksheet.write(row, 2, contenedor.codProveedor)
            worksheet.write(row, 3, contenedor.noIdentificacion)
            worksheet.write(row, 4, contenedor.codigoSat)
            worksheet.write(row, 5, contenedor.noIdentificacion+' '+contenedor.descripcion)
            worksheet.write(row, 6, 'NO')
            worksheet.write(row, 7, contenedor.piezas)
            worksheet.write(row, 8, contenedor.umcDescripcion)
            worksheet.write(row, 9, contenedor.catSat)
            worksheet.write(row, 10, contenedor.peso)
            worksheet.write(row, 11, 'KGM')
            worksheet.write(row, 12, contenedor.claveSat)
            worksheet.write(row, 13, contenedor.pedimento)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'Contenedor_'+idContenedorSalida+'.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    #new >

@api_view(['GET'])
def getdownloadContainerQcCl(request):
    try:
        wmsClDao=WMSCLDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet("ContainerQc")
        worksheet.write(0, 0, 'CONTAINER_ID')
        worksheet.write(0, 1, 'WEIGHT')
        worksheet.write(0, 2, 'USER_DEF1')
        worksheet.write(0, 3, 'TOTAL_FREIGHT_CHARGE')
        worksheet.write(0, 4, 'BASE_FRAIGHT_CHARGE')
        worksheet.write(0, 5, 'FREIGHT_DISCOUNT')
        worksheet.write(0, 6, 'ACCESSORIAL_CHARGE')
        worksheet.write(0, 7, 'QC_ASSIGNMENT_ID')
        worksheet.write(0, 8, 'QC_STATUS')
        worksheet.write(0, 9, 'FECHA')
        
        containerQcList=wmsClDao.getContainerQc()

        row=1
        for container in containerQcList:
            worksheet.write(row, 0, container.container_id)
            worksheet.write(row, 1, container.weight)
            worksheet.write(row, 2, container.user_def1)
            worksheet.write(row, 3, container.total_freight_charge)
            worksheet.write(row, 4, container.base_freight_charge)
            worksheet.write(row, 5, container.freight_discount)
            worksheet.write(row, 6, container.accessorial_charge)
            worksheet.write(row, 7, container.qc_assignment_id)
            worksheet.write(row, 8, container.qc_status)
            worksheet.write(row, 9, container.fecha) 
            row=row+1
        workbook.close()

        output.seek(0)

        filename = 'ContainerQcCl.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def getdownloadItemContainerQcCl(request):
    try:
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'ITEM')
        worksheet.write(0, 1, 'NUMOCUR')
        worksheet.write(0, 2, 'TOTALCONT')
        
        wmsclDao=WMSCLDao()
        itemContainerQcList=wmsclDao.getItemContainerQc()

        row=1
        for container in itemContainerQcList:
            worksheet.write(row, 0, container.item)
            worksheet.write(row, 1, container.numocur)
            worksheet.write(row, 2, container.totalcont)
            row=row+1
        workbook.close()

        output.seek(0)

        filename = 'ItemContainerQcCl.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

######################################### COLOMBIA ############################################################

@api_view(['GET'])
def getSplitsCol(request):
    try:
        wmsCOLDao=WMSCOLDao()
        splitList=wmsCOLDao.getSplit()
        serializer=SplitClSerializer(splitList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getSplitsFileCol(request):
    try:
        wmsCOLDao=WMSCOLDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'PEDIDO')
        worksheet.write(0, 1, 'FECHA CREACIÓN')
        worksheet.write(0, 2, 'NUMERO CONTENEDORES')
        
        splitList=wmsCOLDao.getSplit()
    
        row=1
        for split in splitList:
            worksheet.write(row, 0, split.pedido)
            worksheet.write(row, 1, split.fechaCreacion)
            worksheet.write(row, 2, split.numeroContenedores)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'SplitCol.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET']) #Modify
def getTareasReaSurtAbiertasFileCol(request):
    try:
        print("Tareas abiertas Col")
        wmsColDao=WMSCOLDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'WORK_UNIT')
        worksheet.write(0, 1, 'INSTRUCTION_TYPE')
        worksheet.write(0, 2, 'WORK_TYPE')
        worksheet.write(0, 3, 'WAVEPACK')
        worksheet.write(0, 4, 'FAMILIA')
        worksheet.write(0, 5, 'CONDICION')
        worksheet.write(0, 6, 'ITEM')
        worksheet.write(0, 7, 'ITEM_DESC')
        worksheet.write(0, 8, 'REFERENCE_ID')
        worksheet.write(0, 9, 'FROM_LOC')
        worksheet.write(0, 10, 'FROM_QTY')
        worksheet.write(0, 11, 'TO_LOC')
        worksheet.write(0, 12, 'TO_QTY')
        worksheet.write(0, 13, 'OLA')
        worksheet.write(0, 14, 'NUMERO_INTERNO_INSTRUCCION')
        worksheet.write(0, 15, 'CONVERTED_QTY')
        worksheet.write(0, 16, 'CONTENEDOR')
        worksheet.write(0, 17, 'TIPO_CONTENEDOR')
        worksheet.write(0, 18, 'AGING_DATE_TIME')
        worksheet.write(0, 19, 'START_DATE_TIME')
        
        tareasList=wmsColDao.getTareasReaSurtAbiertas()
        
        row=1
        for tarea in tareasList:
            worksheet.write(row, 0, tarea.workUnit)
            worksheet.write(row, 1, tarea.instructionType)
            worksheet.write(row, 2, tarea.workType)
            worksheet.write(row, 3, tarea.wavepack)
            worksheet.write(row, 4, tarea.familia)
            worksheet.write(row, 5, tarea.condicion)
            worksheet.write(row, 6, tarea.item)
            worksheet.write(row, 7, tarea.itemDesc)
            worksheet.write(row, 8, tarea.referenceId)
            worksheet.write(row, 9, tarea.fromLoc)
            worksheet.write(row, 10, tarea.fromQty)
            worksheet.write(row, 11, tarea.toLoc)
            worksheet.write(row, 12, tarea.toQty)
            worksheet.write(row, 13, tarea.ola)
            worksheet.write(row, 14, tarea.numeroInternoInstruccion)
            worksheet.write(row, 15, tarea.convertedQty)
            worksheet.write(row, 16, tarea.numConteneder)
            worksheet.write(row, 17, tarea.tipoContenedor)
            worksheet.write(row, 18, tarea.agingDateTime)
            worksheet.write(row, 19, tarea.startDateTime)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'TareasAbiertas.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        print("Tareas abiertas col fin")
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getContenedoresFileCol(request):
    try:
        wmsColDao=WMSCOLDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'CONTAINER_ID')
        worksheet.write(0, 1, 'SHIPMENT_ID')
        worksheet.write(0, 2, 'CONTAINER_TYPE')
        worksheet.write(0, 3, 'LEADING_STS')
        worksheet.write(0, 4, 'ITEM')
        worksheet.write(0, 5, 'QUANTITY')
        worksheet.write(0, 6, 'CUSTOMER')
        worksheet.write(0, 7, 'LAUNCH_NUM')
        worksheet.write(0, 8, 'FECHA')
        
        contenedoresList=wmsColDao.getContenedores()
        
        row=1
        for contenedor in contenedoresList:
            worksheet.write(row, 0, contenedor.containerId)
            worksheet.write(row, 1, contenedor.shipmentId)
            worksheet.write(row, 2, contenedor.containerType)
            worksheet.write(row, 3, contenedor.leadingSts)
            worksheet.write(row, 4, contenedor.item)
            worksheet.write(row, 5, contenedor.quantity)
            worksheet.write(row, 6, contenedor.customer)
            worksheet.write(row, 7, contenedor.launchNum)
            worksheet.write(row, 8, contenedor.fecha)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'Contenedores.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getReciboPendientesCol(request):
    try:
        wmsColDao=WMSCOLDao()
        recibosPendientesList=wmsColDao.getRecibosPendientes()
        serializer=ReciboPendienteSerializer(recibosPendientesList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def decargaReciboPendientesCol(request):
    try:
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'Identificador de Recibo')
        worksheet.write(0, 1, 'item')
        worksheet.write(0, 2, 'Descripción de Item')
        worksheet.write(0, 3, 'Cantidad Total')
        worksheet.write(0, 4, 'Cantidad Abierta')
        wmsColDao=WMSCOLDao()
        recibosPendientesList=wmsColDao.getRecibosPendientes()

        row=1
        for reciboPendiente in recibosPendientesList:
            worksheet.write(row, 0, reciboPendiente.receiptId)
            worksheet.write(row, 1, reciboPendiente.item)
            worksheet.write(row, 2, reciboPendiente.itemDesc)
            worksheet.write(row, 3, reciboPendiente.totalQty)
            worksheet.write(row, 4, reciboPendiente.openQty)
            row=row+1
        workbook.close()

        output.seek(0)

        filename = 'RecibosPendientes.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
#New =>

@api_view(['GET'])
def getUnlockANDShorpick(request):
    try:
        wmsDao=WMSDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet("Shorpick")
        worksheet.write(0, 0, 'SEMANA')
        worksheet.write(0, 1, 'INTERNAL_ID')
        worksheet.write(0, 2, 'REFERENCE_ID')
        worksheet.write(0, 3, 'DATE_TIME_STAMP')
        worksheet.write(0, 4, 'TRANSACTION_TYPE')
        worksheet.write(0, 5, 'LOCATION')
        worksheet.write(0, 6, 'ITEM')
        worksheet.write(0, 7, 'COMPANY')
        worksheet.write(0, 8, 'LOT')
        worksheet.write(0, 9, 'QUANTITY')
        worksheet.write(0, 10, 'QUANTITY_UM')
        worksheet.write(0, 11, 'DIRECTION')
        worksheet.write(0, 12, 'WORK_TYPE')
        worksheet.write(0, 13, 'WORK_TEAM')
        worksheet.write(0, 14, 'INTERNAL_KEY_ID')
        worksheet.write(0, 15, 'WORK_UNIT')
        worksheet.write(0, 16, 'USER_NAME')
        worksheet.write(0, 17, 'USER_DEF1')
        worksheet.write(0, 18, 'EQUIPMENT_TYPE')
        worksheet.write(0, 19, 'warehouse')
        
        shorpickList=wmsDao.getShorpick()
        
        row=1
        for shortPick in shorpickList:
            worksheet.write(row, 0, shortPick.semana)
            worksheet.write(row, 1, shortPick.internal_id)
            worksheet.write(row, 2, shortPick.reference_id)
            worksheet.write(row, 3, shortPick.date_time_stamp)
            worksheet.write(row, 4, shortPick.transaction_type)
            worksheet.write(row, 5, shortPick.location)
            worksheet.write(row, 6, shortPick.item)
            worksheet.write(row, 7, shortPick.company)
            worksheet.write(row, 8, shortPick.lot)
            worksheet.write(row, 9, shortPick.quantity)
            worksheet.write(row, 10, shortPick.quantity_um)
            worksheet.write(row, 11, shortPick.direction)
            worksheet.write(row, 12, shortPick.work_type)
            worksheet.write(row, 13, shortPick.work_team)
            worksheet.write(row, 14, shortPick.internal_key_id)
            worksheet.write(row, 15, shortPick.work_unit)
            worksheet.write(row, 16, shortPick.user_name)
            worksheet.write(row, 17, shortPick.user_def1)
            worksheet.write(row, 18, shortPick.equipment_type)
            worksheet.write(row, 19, shortPick.warehouse)        
            row=row+1

        worksheet = workbook.add_worksheet("Unlock")

        worksheet.write(0, 0, 'INTERNAL_ID')
        worksheet.write(0, 1, 'REFERENCE_ID')
        worksheet.write(0, 2, 'DATE_TIME_STAMP')
        worksheet.write(0, 3, 'TRANSACTION_TYPE')
        worksheet.write(0, 4, 'LOCATION')
        worksheet.write(0, 5, 'ITEM')
        worksheet.write(0, 6, 'COMPANY')
        worksheet.write(0, 7, 'LOT')
        worksheet.write(0, 8, 'QUANTITY')
        worksheet.write(0, 9, 'QUANTITY_UM')
        worksheet.write(0, 10, 'USER_DEF1')
        worksheet.write(0, 11, 'WORK_TYPE')
        worksheet.write(0, 12, 'CONTAINER_ID')
        worksheet.write(0, 13, 'INTERNAL_KEY_ID')
        worksheet.write(0, 14, 'WORK_UNIT')
        worksheet.write(0, 15, 'USER_NAME')
        worksheet.write(0, 16, 'WORK_TEAM')
        worksheet.write(0, 17, 'EQUIPMENT_TYPE')
        worksheet.write(0, 18, 'warehouse')
        worksheet.write(0, 19, 'QUANTITY_Positive')
        
        unlockList=wmsDao.getUnlock()
        
        row=1
        for unlock in unlockList:
            worksheet.write(row, 0, unlock.internal_id)
            worksheet.write(row, 1, unlock.reference_id)
            worksheet.write(row, 2, unlock.date_time_stamp)
            worksheet.write(row, 3, unlock.transaction_type)
            worksheet.write(row, 4, unlock.location)
            worksheet.write(row, 5, unlock.item)
            worksheet.write(row, 6, unlock.company)
            worksheet.write(row, 7, unlock.lot)
            worksheet.write(row, 8, unlock.quantity)
            worksheet.write(row, 9, unlock.quantity_um)
            worksheet.write(row, 10, unlock.user_def1)
            worksheet.write(row, 11, unlock.work_type)
            worksheet.write(row, 12, unlock.container_id)
            worksheet.write(row, 13, unlock.internal_key_id)
            worksheet.write(row, 14, unlock.work_unit)
            worksheet.write(row, 15, unlock.user_name)
            worksheet.write(row, 16, unlock.work_team)
            worksheet.write(row, 17, unlock.equipment_type)
            worksheet.write(row, 18, unlock.warehouse)
            worksheet.write(row, 19, unlock.quantity_positive)            
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'Unlock&Shorpick.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
def getdownloadContainerQc(request):
    try:
        wmsDao=WMSDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet("ContainerQc")
        worksheet.write(0, 0, 'CONTAINER_ID')
        worksheet.write(0, 1, 'WEIGHT')
        worksheet.write(0, 2, 'USER_DEF1')
        worksheet.write(0, 3, 'TOTAL_FREIGHT_CHARGE')
        worksheet.write(0, 4, 'BASE_FRAIGHT_CHARGE')
        worksheet.write(0, 5, 'FREIGHT_DISCOUNT')
        worksheet.write(0, 6, 'ACCESSORIAL_CHARGE')
        worksheet.write(0, 7, 'QC_ASSIGNMENT_ID')
        worksheet.write(0, 8, 'QC_STATUS')
        worksheet.write(0, 9, 'FECHA')
        
        containerQcList=wmsDao.getContainerQc()

        row=1
        for container in containerQcList:
            worksheet.write(row, 0, container.container_id)
            worksheet.write(row, 1, container.weight)
            worksheet.write(row, 2, container.user_def1)
            worksheet.write(row, 3, container.total_freight_charge)
            worksheet.write(row, 4, container.base_freight_charge)
            worksheet.write(row, 5, container.freight_discount)
            worksheet.write(row, 6, container.accessorial_charge)
            worksheet.write(row, 7, container.qc_assignment_id)
            worksheet.write(row, 8, container.qc_status)
            worksheet.write(row, 9, container.fecha) 
            row=row+1
        workbook.close()

        output.seek(0)

        filename = 'ContainerQc.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def getdownloadItemContainerQc(request):
    try:
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'ITEM')
        worksheet.write(0, 1, 'NUMOCUR')
        worksheet.write(0, 2, 'TOTALCONT')
        
        wmsDao=WMSDao()
        itemContainerQcList=wmsDao.getItemContainerQc()

        row=1
        for container in itemContainerQcList:
            worksheet.write(row, 0, container.item)
            worksheet.write(row, 1, container.numocur)
            worksheet.write(row, 2, container.totalcont)
            row=row+1
        workbook.close()

        output.seek(0)

        filename = 'ItemContainerQc.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET']) #New
def getConfirmacionesPendientes(request):
    try:
        recepcionTiendaDao=RecepcionTiendaDao()
        confirmList=recepcionTiendaDao.getConfirmPending()
        serializer=ConfirmacionesPendientes(confirmList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET']) #New
def getConsultaKardex(request,item,container_id,location,user_stamp,work_type,dateStar, dateEnd, transaction):
    try:
        print("getConsultaKardex")
        print(f"item: {item}")
        print(f"container_id: {container_id}")
        print(f"location: {location}")
        print(f"user_stamp: {user_stamp}")
        print(f"work_type: {work_type}")
        print(f"dateStart: {dateStar}")
        print(f"dateEnd: {dateEnd}")
        print(dateStar)
        print(dateEnd)
        print(transaction)
        wmsDao=WMSDao()
        kardexList=wmsDao.getConsultKardex(item,container_id,location,user_stamp,work_type,dateStar,dateEnd,transaction)
        serializer=ConsultaKardex(kardexList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        print(exception)
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def getKardexDownload(request,item,container_id,location,user_stamp,work_type,dateStar, dateEnd,transaction):
    
    wmsDao=WMSDao()
    cont = wmsDao.timeDownloadKardex(item, container_id, location, user_stamp, work_type, dateStar, dateEnd,transaction)
    hourK = datetime.now()

    if (cont < 10000 or hourK.hour >= 22):
        try:
            print(f"Kardex: No: {cont} Hora: {hourK.hour}")
            output = io.BytesIO()

            workbook = xlsxwriter.Workbook(output)
            worksheet = workbook.add_worksheet()
            worksheet.write(0, 0, 'ITEM')
            worksheet.write(0, 1, 'TRANSACTION_TYPE')
            worksheet.write(0, 2, 'DESCRIPTION')
            worksheet.write(0, 3, 'LOCATION')
            worksheet.write(0, 4, 'CONTAINER_ID')
            worksheet.write(0, 5, 'REFERENCE_ID')
            worksheet.write(0, 6, 'REFERENCE_TYPE')
            worksheet.write(0, 7, 'WORK_TYPE')
            worksheet.write(0, 8, 'DATE_STAMP')
            worksheet.write(0, 9, 'USER_STAMP')
            worksheet.write(0, 10, 'QUANTITY')
            worksheet.write(0, 11, 'BEFORE_STS')
            worksheet.write(0, 12, 'AFTER_STS')
            worksheet.write(0, 13, 'BEFORE_ON_HAND_QTY')
            worksheet.write(0, 14, 'AFTER_ON_HAND_QTY')
            worksheet.write(0, 15, 'BEFORE_IN_TRANSIT_QTY')
            worksheet.write(0, 16, 'AFTER_IN_TRANSIT_QTY')
            worksheet.write(0, 17, 'BEFORE_SUSPENSE_QTY')
            worksheet.write(0, 18, 'AFTER_SUSPENSE_QTY')
            worksheet.write(0, 19, 'BEFORE_ALLOC_QTY')
            worksheet.write(0, 20, 'AFTER_ALLOC_QTY')
            worksheet.write(0, 21, 'DIRECTION')
            
            kardexList=wmsDao.getDownloadKardex(item, container_id, location, user_stamp, work_type, dateStar, dateEnd,transaction)
            
            row=1
            for kardex in kardexList:
                worksheet.write(row, 0, kardex.item)
                worksheet.write(row, 1, kardex.transaction_type)
                worksheet.write(row, 2, kardex.description)
                worksheet.write(row, 3, kardex.location)
                worksheet.write(row, 4, kardex.container_id)
                worksheet.write(row, 5, kardex.reference_id)
                worksheet.write(row, 6, kardex.reference_type)
                worksheet.write(row, 7, kardex.work_type)
                worksheet.write(row, 8, kardex.date_stamp)
                worksheet.write(row, 9, kardex.user_stamp)
                worksheet.write(row, 10, kardex.quantity)
                worksheet.write(row, 11, kardex.before_sts)
                worksheet.write(row, 12, kardex.after_sts)
                worksheet.write(row, 13, kardex.before_on_hand_qty)
                worksheet.write(row, 14, kardex.after_on_hand_qty)
                worksheet.write(row, 15, kardex.before_in_transit_qty)
                worksheet.write(row, 16, kardex.after_in_transit_qty)
                worksheet.write(row, 17, kardex.before_suspense_qty)
                worksheet.write(row, 18, kardex.after_suspense_qty)
                worksheet.write(row, 19, kardex.before_alloc_qty)
                worksheet.write(row, 20, kardex.after_alloc_qty)
                worksheet.write(row, 21, kardex.direction)
                row=row+1

            workbook.close()

            output.seek(0)

            filename = 'Kardex.xlsx'
            response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = 'attachment; filename=%s' % filename
            return response
        except Exception as exception:
            logger.error(f'Se presento una incidencia: {exception}')
            return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
    else:
        return JsonResponse({'message': 'No'})

#New Colombia 

@api_view(['GET'])
def storagesTemplatesCOL(request):
    try:
        sapDaoCol = SapDaoCol()
        storagesTemplatesList=sapDaoCol.getStorageTemplatesCol('100')
        serializer=StorageTemplateSerializer(storagesTemplatesList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def descargaStoragesCOL(request):
    try:
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'SKU')
        worksheet.write(0, 1, 'Storage Template')
        worksheet.write(0, 2, 'Grupo Logistico')
        worksheet.write(0, 3, 'Unidad')
        worksheet.write(0, 4, 'Familia')
        worksheet.write(0, 5, 'Subfamilia')
        worksheet.write(0, 6, 'Subsubfamilia')
        worksheet.write(0, 7, 'uSysLote')
        worksheet.write(0, 8, 'uSysCat4')
        worksheet.write(0, 9, 'uSysCat5')
        worksheet.write(0, 10, 'uSysCat6')
        worksheet.write(0, 11, 'uSysCat7')
        worksheet.write(0, 12, 'uSysCat8')
        worksheet.write(0, 13, 'Height')
        worksheet.write(0, 14, 'Width')
        worksheet.write(0, 15, 'Length')
        worksheet.write(0, 16, 'Volume')
        worksheet.write(0, 17, 'Weight')
        sapDaoCol = SapDaoCol()
        storagesTemplatesList=sapDaoCol.getStorageTemplatesCol('')

        row=1
        for storageTemplate in storagesTemplatesList:
            worksheet.write(row, 0, storageTemplate.itemCode)
            worksheet.write(row, 1, storageTemplate.storageTemplate)
            worksheet.write(row, 2, storageTemplate.grupoLogistico)
            worksheet.write(row, 3, storageTemplate.salUnitMsr)
            worksheet.write(row, 4, storageTemplate.familia)
            worksheet.write(row, 5, storageTemplate.subFamilia)
            worksheet.write(row, 6, storageTemplate.subSubFamilia)
            worksheet.write(row, 7, storageTemplate.uSysLote)
            worksheet.write(row, 8, storageTemplate.uSysCat4)
            worksheet.write(row, 9, storageTemplate.uSysCat5)
            worksheet.write(row, 10, storageTemplate.uSysCat6)
            worksheet.write(row, 11, storageTemplate.uSysCat7)
            worksheet.write(row, 12, storageTemplate.uSysCat8)
            worksheet.write(row, 13, storageTemplate.height)
            worksheet.write(row, 14, storageTemplate.width)
            worksheet.write(row, 15, storageTemplate.length)
            worksheet.write(row, 16, storageTemplate.volume)
            worksheet.write(row, 17, storageTemplate.weight)
            row=row+1
        workbook.close()

        output.seek(0)

        filename = 'StorageTemplate.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def getPreciosCol(request):
    try:
        sapDaoCol=SapDaoCol()
        preciosList=sapDaoCol.getPreciosCol('100')

        serializer=PreciosSerializerCol(preciosList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def downloadPreciosCol(request):
    try:
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'Item')
        worksheet.write(0, 1, 'Codigos de Barras')
        worksheet.write(0, 2, 'Categoria')
        worksheet.write(0, 3, 'Subcategoria')
        worksheet.write(0, 4, 'Clase')
        worksheet.write(0, 5, 'Descripción')
        worksheet.write(0, 6, 'Storage Template')
        worksheet.write(0, 7, 'Storage Template User')
        worksheet.write(0, 8, 'Licencia')
        worksheet.write(0, 9, 'Height')
        worksheet.write(0, 10, 'Width')
        worksheet.write(0, 11, 'Length')
        worksheet.write(0, 12, 'Volume')
        worksheet.write(0, 13, 'Weight')
        worksheet.write(0, 14, 'Estandar Sin Ivan')
        worksheet.write(0, 15, 'Estandar Con Ivan')
        worksheet.write(0, 16, 'Adicional Sin Ivan')
        worksheet.write(0, 17, 'Adicional Con Ivan')
        worksheet.write(0, 18, 'Aeropuerto Sin Ivan')
        worksheet.write(0, 19, 'Aeropuerto Con Ivan')
        worksheet.write(0, 20, 'Proveedor')

        sapDaoCol=SapDaoCol()
        preciosList=sapDaoCol.getPreciosCol('')

        row=1
        for precio in preciosList:
            worksheet.write(row, 0, precio.itemCode)
            worksheet.write(row, 1, precio.codigoBarras)
            worksheet.write(row, 2, precio.categoria)
            worksheet.write(row, 3, precio.subcategoria)
            worksheet.write(row, 4, precio.clase)
            worksheet.write(row, 5, precio.itemName)
            worksheet.write(row, 6, precio.storageTemplate)
            worksheet.write(row, 7, precio.stUsr)
            worksheet.write(row, 8, precio.licencia)
            worksheet.write(row, 9, precio.height)
            worksheet.write(row, 10, precio.width)
            worksheet.write(row, 11, precio.length)
            worksheet.write(row, 12, precio.volume)
            worksheet.write(row, 13, precio.weight)
            worksheet.write(row, 14, precio.estandarSinIva)
            worksheet.write(row, 15, precio.estandarConIva)
            worksheet.write(row, 16, precio.adicionalSinIva)
            worksheet.write(row, 17, precio.adicionalConIva)
            worksheet.write(row, 18, precio.aeropuertoSinIva)
            worksheet.write(row, 19, precio.aeropuertoConIva)
            worksheet.write(row, 20, precio.proveedor)
            row=row+1
        workbook.close()

        output.seek(0)

        filename = 'Precios.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def getHuellaDigital(request):
    try:
        sapDaoII=SAPDaoII()
        huellaList=sapDaoII.getHuellaDigitalConsult('100')

        serializer=HuellaDigitalSerializer(huellaList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def downloadHuellaDigital(request):
    try:
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'FrozenFor')
        worksheet.write(0, 1, 'ItemCode')
        worksheet.write(0, 2, 'ItemName')
        worksheet.write(0, 3, 'Familia')
        worksheet.write(0, 4, 'SubFamilia')
        worksheet.write(0, 5, 'SubSubFamilia')
        worksheet.write(0, 6, 'Fragil')
        worksheet.write(0, 7, 'Movimiento')
        worksheet.write(0, 8, 'AltoValor')
        worksheet.write(0, 9, 'Bolsa')
        worksheet.write(0, 10, 'Flujo')
        worksheet.write(0, 11, 'BcdCode')
        worksheet.write(0, 12, 'Volume')
        worksheet.write(0, 13, 'Height')
        worksheet.write(0, 14, 'Width')
        worksheet.write(0, 15, 'Lenght')
        worksheet.write(0, 16, 'Unidad Medida')
        worksheet.write(0, 17, 'GrupoUMLogistico')
        worksheet.write(0, 18, 'Weight')
        worksheet.write(0, 19, 'GrupoUMCompas')

        sapDaoII = SAPDaoII()
        huellas=sapDaoII.getHuellaDigitalConsult('')

        row=1
        for huella in huellas:
            worksheet.write(row, 0, huella.frozenFor)
            worksheet.write(row, 1, huella.itemCode)
            worksheet.write(row, 2, huella.itemName)
            worksheet.write(row, 3, huella.familia)
            worksheet.write(row, 4, huella.subFamilia)
            worksheet.write(row, 5, huella.subSubFamilia)
            worksheet.write(row, 6, huella.fragil)
            worksheet.write(row, 7, huella.movimiento)
            worksheet.write(row, 8, huella.altoValor)
            worksheet.write(row, 9, huella.bolsa)
            worksheet.write(row, 10, huella.flujo)
            worksheet.write(row, 11, huella.bcdCode)
            worksheet.write(row, 12, huella.u_sys_unid)
            worksheet.write(row, 13, huella.u_sys_alto)
            worksheet.write(row, 14, huella.u_sys_anch)
            worksheet.write(row, 15, huella.u_sys_long)
            worksheet.write(row, 16, huella.u_sys_volu)
            worksheet.write(row, 17, huella.grupoUMLogistico)
            worksheet.write(row, 18, huella.u_sys_peso)
            worksheet.write(row, 19, huella.grupoUMCompas)
            row=row+1
        workbook.close()

        output.seek(0)

        filename = 'Huella_Digital.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def getWaveAnalysisCol(request, wave):
    try:
        wmsCOLDao=WMSCOLDao()
        waveList=wmsCOLDao.getWaveAnalysis(True, wave)
        serializer=WaveSerializer(waveList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getWaveAnalysisFileCol(request, wave):
    try:
        wmsCOLDao=WMSCOLDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'ITEM')
        worksheet.write(0, 1, 'DESCRIPCION')
        worksheet.write(0, 2, 'STORAGE TEMPLATE')
        worksheet.write(0, 3, 'SHIPMENT ID')
        worksheet.write(0, 4, 'LAUNCH NUM')
        worksheet.write(0, 5, 'STATUS')
        worksheet.write(0, 6, 'REQUESTED QTY')
        worksheet.write(0, 7, 'ALLOCATED QTY')
        worksheet.write(0, 8, 'AV')
        worksheet.write(0, 9, 'OH')
        worksheet.write(0, 10, 'AL')
        worksheet.write(0, 11, 'IT')
        worksheet.write(0, 12, 'SU')
        worksheet.write(0, 13, 'CUSTOMER')
        worksheet.write(0, 14, 'ITEM CATEGORY')
        worksheet.write(0, 15, 'CREATION DATE')
        worksheet.write(0, 16, 'SCHEDULED SHIP DATE')
        worksheet.write(0, 17, 'DIVISION')
        worksheet.write(0, 18, 'CONV')
        
        waveList=wmsCOLDao.getWaveAnalysis(False, wave)
    
        row=1
        for wav in waveList:
            worksheet.write(row, 0, wav.item)
            worksheet.write(row, 1, wav.description)
            worksheet.write(row, 2, wav.storageTemplate)
            worksheet.write(row, 3, wav.shipmentId)
            worksheet.write(row, 4, wav.launchNum)
            worksheet.write(row, 5, wav.status)
            worksheet.write(row, 6, wav.requestedQty)
            worksheet.write(row, 7, wav.allocatedQty)
            worksheet.write(row, 8, wav.av)
            worksheet.write(row, 9, wav.oh)
            worksheet.write(row, 10, wav.al)
            worksheet.write(row, 11, wav.it)
            worksheet.write(row, 12, wav.su)
            worksheet.write(row, 13, wav.customer)
            worksheet.write(row, 14, wav.itemCategory)
            worksheet.write(row, 15, wav.creationDateTimeStamp)
            worksheet.write(row, 16, wav.scheduledShipDate)
            worksheet.write(row, 17, wav.division)
            worksheet.write(row, 18, wav.conv)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'WaveAnalysisCol'+wave+'.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def inventarioWmsErpCol(request):
    try:
        scaleIntColDao=ScaleIntColDao()
        wmsErpList=scaleIntColDao.getWmsErp()
        serializer=InventarioWmsErpSerializer(wmsErpList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def inventarioItemsCol(request):
    try:
        print('inventarioItemsCol')
        scaleIntColDao=ScaleIntColDao()
        itemsList=scaleIntColDao.getItems()
        serializer=InventarioItemSerializer(itemsList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    

@api_view(['GET'])
def topOneHundredCol(request):
    try:
        scaleIntColDao=ScaleIntColDao()
        TopList=scaleIntColDao.getTopOneHundred(True)
        serializer=InventarioTopSerializer(TopList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def inventarioDetalleErpWmsCol(request, item):
    try:
        scaleIntColDao=ScaleIntColDao()
        detallesErpWmsList=scaleIntColDao.getInventarioDetalleErpWms(item)
        serializer=InventarioDetalleErpWmsSerializer(detallesErpWmsList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def executeInvetarioCedisCol(request):
    try:
        monitoreoDao=MonitoreoDao()
        monitoreoDao.executeInvetarioCedisCol()
        return Response(status=status.HTTP_200_OK)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    
@api_view(['GET'])
def inventarioFileCol(request):
    try:
        scaleIntDao=ScaleIntDao()
        monitoreoDao=MonitoreoDao()
        
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'WAREHOUSE')
        worksheet.write(0, 1, 'WMS ONHAND')
        worksheet.write(0, 2, 'ERP ONHAND')
        worksheet.write(0, 3, 'DIFERENCIA')
        worksheet.write(0, 4, 'DIFERENCIA ABS')
        worksheet.write(0, 5, 'WMS INTRANSIT')
        worksheet.write(0, 6, '#ITEMS WMS')
        worksheet.write(0, 7, '#ITEMS ERP')
        worksheet.write(0, 8, '#ITEMS DIF')
        
        wmsErpList=scaleIntDao.getWmsErp()
        row=1
        for wmsErp in wmsErpList:
            worksheet.write(row, 0, wmsErp.warehouse)
            worksheet.write(row, 1, wmsErp.wmsOnHand)
            worksheet.write(row, 2, wmsErp.erpOnHand)
            worksheet.write(row, 3, wmsErp.diferencia)
            worksheet.write(row, 4, wmsErp.diferenciaAbsoluta)
            worksheet.write(row, 5, wmsErp.wmsInTransit)
            worksheet.write(row, 6, wmsErp.numItemsWms)
            worksheet.write(row, 7, wmsErp.numItemsErp)
            worksheet.write(row, 8, wmsErp.numItemsDif)
            row=row+1

        row=row+1
        worksheet.write(row, 0, 'WAREHOUSE')
        worksheet.write(row, 1, 'WMS ONHAND')
        worksheet.write(row, 2, 'ERP ONHAND')
        worksheet.write(row, 3, '#ITEMS')
        
        itemsList=scaleIntDao.getItems()
        row=row+1
        for item in itemsList:
            worksheet.write(row, 0, item.warehouse)
            worksheet.write(row, 1, item.wmsOnHand)
            worksheet.write(row, 2, item.erpOnHand)
            worksheet.write(row, 3, item.numItems)
            row=row+1
        
        row=row+1
        worksheet.write(row, 0, 'WAREHOUSE CODE')
        worksheet.write(row, 1, 'Solicitado')
        worksheet.write(row, 2, 'OnHand')
        worksheet.write(row, 3, 'Comprometido')
        worksheet.write(row, 4, 'Disponible')
        worksheet.write(row, 5, 'SKU SOL')
        worksheet.write(row, 6, 'SKU OHD')
        worksheet.write(row, 7, 'SKU CMP')
        worksheet.write(row, 8, 'Fecha Actualizacion')
        
        inventariosWmsList=monitoreoDao.getInventarioWms()
        row=row+1
        for inventarioWms in inventariosWmsList:
            worksheet.write(row, 0, inventarioWms.warehouseCode)
            worksheet.write(row, 1, inventarioWms.solicitado)
            worksheet.write(row, 2, inventarioWms.onHand)
            worksheet.write(row, 3, inventarioWms.comprometido)
            worksheet.write(row, 4, inventarioWms.disponible)
            worksheet.write(row, 5, inventarioWms.skuSolicitado)
            worksheet.write(row, 6, inventarioWms.skuOnHand)
            worksheet.write(row, 7, inventarioWms.skuComprometido)
            worksheet.write(row, 8, inventarioWms.fechaActualizacion)
            row=row+1
        
        workbook.close()

        output.seek(0)

        filename = 'Inventario.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def topOneHundredFileCol(request):
    try:
        scaleIntColDao=ScaleIntColDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'DATE TIME')
        worksheet.write(0, 1, 'ITEM')
        worksheet.write(0, 2, 'WAREHOUSE')
        worksheet.write(0, 3, 'WMS SUSPENSE')
        worksheet.write(0, 4, 'WMS INTRANSIT')
        worksheet.write(0, 5, 'WMS ONHAND')
        worksheet.write(0, 6, 'ERP ONHAND')
        worksheet.write(0, 7, 'DIF ONHAND')
        worksheet.write(0, 8, 'DIF OH ABS')
        
        TopList=scaleIntColDao.getTopOneHundred(False)
        
        row=1
        for top in TopList:
            worksheet.write(row, 0, top.fecha.strftime("%m/%d/%Y %H:%M:%S"))
            worksheet.write(row, 1, top.item)
            worksheet.write(row, 2, top.warehouse)
            worksheet.write(row, 3, top.wmsComprometido)
            worksheet.write(row, 4, top.wmsTransito)
            worksheet.write(row, 5, top.wmsOnHand)
            worksheet.write(row, 6, top.erpOnHand)
            worksheet.write(row, 7, top.difOnHand)
            worksheet.write(row, 8, top.difOnHandAbsolute)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'TopOneHundredCol.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    

@api_view(['GET'])
def inventarioWmsCol(request):
    try:
        monitoreoDao=MonitoreoDao()
        inventariosWmsList=monitoreoDao.getInventarioWmsCol()
        serializer=InventarioWmsSerializer(inventariosWmsList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def inventarioFileCol(request):
    try:
        scaleIntColDao=ScaleIntColDao()
        monitoreoDao=MonitoreoDao()
        
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'WAREHOUSE')
        worksheet.write(0, 1, 'WMS ONHAND')
        worksheet.write(0, 2, 'ERP ONHAND')
        worksheet.write(0, 3, 'DIFERENCIA')
        worksheet.write(0, 4, 'DIFERENCIA ABS')
        worksheet.write(0, 5, 'WMS INTRANSIT')
        worksheet.write(0, 6, '#ITEMS WMS')
        worksheet.write(0, 7, '#ITEMS ERP')
        worksheet.write(0, 8, '#ITEMS DIF')
        
        wmsErpList=scaleIntColDao.getWmsErp()
        row=1
        for wmsErp in wmsErpList:
            worksheet.write(row, 0, wmsErp.warehouse)
            worksheet.write(row, 1, wmsErp.wmsOnHand)
            worksheet.write(row, 2, wmsErp.erpOnHand)
            worksheet.write(row, 3, wmsErp.diferencia)
            worksheet.write(row, 4, wmsErp.diferenciaAbsoluta)
            worksheet.write(row, 5, wmsErp.wmsInTransit)
            worksheet.write(row, 6, wmsErp.numItemsWms)
            worksheet.write(row, 7, wmsErp.numItemsErp)
            worksheet.write(row, 8, wmsErp.numItemsDif)
            row=row+1

        row=row+1
        worksheet.write(row, 0, 'WAREHOUSE')
        worksheet.write(row, 1, 'WMS ONHAND')
        worksheet.write(row, 2, 'ERP ONHAND')
        worksheet.write(row, 3, '#ITEMS')
        
        itemsList=scaleIntColDao.getItems()
        row=row+1
        for item in itemsList:
            worksheet.write(row, 0, item.warehouse)
            worksheet.write(row, 1, item.wmsOnHand)
            worksheet.write(row, 2, item.erpOnHand)
            worksheet.write(row, 3, item.numItems)
            row=row+1
        
        row=row+1
        worksheet.write(row, 0, 'WAREHOUSE CODE')
        worksheet.write(row, 1, 'Solicitado')
        worksheet.write(row, 2, 'OnHand')
        worksheet.write(row, 3, 'Comprometido')
        worksheet.write(row, 4, 'Disponible')
        worksheet.write(row, 5, 'SKU SOL')
        worksheet.write(row, 6, 'SKU OHD')
        worksheet.write(row, 7, 'SKU CMP')
        worksheet.write(row, 8, 'Fecha Actualizacion')
        
        inventariosWmsList=monitoreoDao.getInventarioWmsCol()
        row=row+1
        for inventarioWms in inventariosWmsList:
            worksheet.write(row, 0, inventarioWms.warehouseCode)
            worksheet.write(row, 1, inventarioWms.solicitado)
            worksheet.write(row, 2, inventarioWms.onHand)
            worksheet.write(row, 3, inventarioWms.comprometido)
            worksheet.write(row, 4, inventarioWms.disponible)
            worksheet.write(row, 5, inventarioWms.skuSolicitado)
            worksheet.write(row, 6, inventarioWms.skuOnHand)
            worksheet.write(row, 7, inventarioWms.skuComprometido)
            worksheet.write(row, 8, inventarioWms.fechaActualizacion)
            row=row+1
        
        workbook.close()

        output.seek(0)

        filename = 'InventarioCol.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def getCuadrajeCol(request):
    try:
        monitoreoDao=MonitoreoDao()
        cuadrajesList=monitoreoDao.getDatosCuadrajeCol()
        serializer=CuadrajeSerializer(cuadrajesList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    

@api_view(['GET'])
def getPendienteSemanaCol(request):
    try:
        monitoreoDao=MonitoreoDao()
        pendientesSemanaList=monitoreoDao.getPendienteSemanaCol()
        serializer=PendienteSemanaSerializer(pendientesSemanaList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    

@api_view(['GET'])
def executeCuadrajeCol(request):
    try:
        monitoreoDao=MonitoreoDao()
        monitoreoDao.executeCuadrajeCol()
        return Response(status=status.HTTP_200_OK)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    

@api_view(['GET'])
def executeUpdateDiferenciasCol(request):
    try:
        monitoreoDao=MonitoreoDao()
        monitoreoDao.executeUpdateDiferenciasCol()
        return Response(status=status.HTTP_200_OK)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def getReciboSapCol(request, recibos):
    try:
        recibosSplit=recibos.split(',')
        indice =True
        busqueda=''
        for recibo in recibosSplit:
            if(indice==False):
                busqueda=busqueda + ","
            busqueda=busqueda+"'"+recibo.strip()+"'"
            if(indice):
                indice=False
        monitoreoDao=MonitoreoDao()
        logger.error("Entro a getReciboSap Col"+busqueda)
        recibosList=monitoreoDao.getReciboSapCol(busqueda)
        serializer=ReciboSapSerializer(recibosList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
def getReciboSapByValorCol(request, valor):
    try:
        monitoreoDao=MonitoreoDao()
        recibosList=monitoreoDao.getReciboSapByValorCol(valor)
        serializer=ReciboSapSerializer(recibosList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
def getPedidoSapCol(request, pedidos):
    try:
        pedidosSplit=pedidos.split(',')
        indice =True
        busqueda=''
        for pedido in pedidosSplit:
            if(indice==False):
                busqueda=busqueda + ","
            busqueda=busqueda+"'"+pedido.strip()+"'"
            if(indice):
                indice=False
        monitoreoDao=MonitoreoDao()
        pedidosList=monitoreoDao.getPedidoSapCol(busqueda)
        serializer=PedidoSapSerializer(pedidosList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
def getPedidoSapByValorCol(request, valor):
    try:
        monitoreoDao=MonitoreoDao()
        if valor == 'O':
            pedidosList=monitoreoDao.getPedidoSapAbiertosCol()
        else:
            pedidosList=monitoreoDao.getPedidoSapByValorCol(valor, True)
        serializer=PedidoSapSerializer(pedidosList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    

@api_view(['GET'])
def getCuadrajeFileCol(request):
    try:
        monitoreoDao=MonitoreoDao()
        cuadrajesList=monitoreoDao.getDatosCuadrajeCol()
        cuadraje=cuadrajesList[0]
        
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 1, 'RECIBO')
        worksheet.write(0, 2, 'PEDIDO')
        worksheet.write(1, 0, 'TOTAL DE FOLIOS')
        worksheet.write(1, 1, cuadraje.recibosTotal)
        worksheet.write(1, 2, cuadraje.pedidosTotal)
        worksheet.write(2, 0, 'FOLIOS OK')
        worksheet.write(2, 1, cuadraje.recibosOk)
        worksheet.write(2, 2, cuadraje.pedidosOk)
        worksheet.write(3, 0, 'FOLIOS CON DIFERENCIA EN CANTIDAD')
        worksheet.write(3, 1, cuadraje.recibosQty)
        worksheet.write(3, 2, cuadraje.pedidosQty)
        worksheet.write(4, 0, 'FOLIOS PENDINETES DE CERRAR EN ERP')
        worksheet.write(4, 1, cuadraje.recibosCloseErp)
        worksheet.write(4, 2, cuadraje.pedidosCloseErp)
        worksheet.write(5, 0, 'FOLIOS PENDIENTES DE CERRAR EN ERP Y WMS')
        worksheet.write(5, 1, cuadraje.recibosRev)
        worksheet.write(5, 2, cuadraje.pedidosRev)
        
        pendientesSemanaList=monitoreoDao.getPendienteSemanaCol()
        
        worksheet.write(7, 0, 'MES-AÑO')
        worksheet.write(7, 1, 'SEMANA')
        worksheet.write(7, 2, 'NUMERO')
        worksheet.write(7, 3, 'TIENDAS')
        worksheet.write(7, 4, 'RE-ETIQUETADO')
        row=8
        for pendienteSemana in pendientesSemanaList:
            worksheet.write(row, 0, pendienteSemana.fecha)
            worksheet.write(row, 1, pendienteSemana.shipDate)
            worksheet.write(row, 2, pendienteSemana.numeroRegistros)
            worksheet.write(row, 3, pendienteSemana.piezas)
            worksheet.write(row, 4, pendienteSemana.reetiquetado)
            row=row+1
        worksheet.write(row, 1, 'PEDIDOS ABIERTOS SOLO EN ERP')
        worksheet.write(row, 2, cuadraje.pedidosAbiertos)
        worksheet.write(row, 3, cuadraje.pedidosAbiertosNum)    
        
        workbook.close()

        output.seek(0)

        filename = 'CuadrajeCol.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename

        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    

@api_view(['GET'])
def insertSapReceiptValCol(request, idReceipt):
    try:
        monitoreoDao=MonitoreoDao()
        monitoreoDao.insertSapReceiptValCol(idReceipt)
        return Response(status=status.HTTP_200_OK)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def insertSapShipmentValCol(request, idShipment):
    try:
        monitoreoDao=MonitoreoDao()
        monitoreoDao.insertSapShipmentValCol(idShipment)
        return Response(status=status.HTTP_200_OK)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def OrderAudiCl(request, tienda, fecha):
    try:
        print(f"{tienda}, {fecha}")
        recepcionTiendaDaoCl=RecepcionTiendaDaoCl()
        orderList=recepcionTiendaDaoCl.getOrderAudi(tienda, fecha)
        serializer=AuditoriaOrderClSerializer(orderList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    

@api_view(['GET'])
def subFamilyOrdersCl(request, tienda, fecha):
    try:
        recepcionTiendaDaoCl=RecepcionTiendaDaoCl()
        subfamily=recepcionTiendaDaoCl.getSubFamilyOrders(tienda, fecha)
        serializer=subFamilyOrderClSerializer(subfamily, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@csrf_exempt
@api_view(['POST'])
def handle_request(request, action):
    logger.info("Entrando en handle_request con acción: %s", action)
    print("Entrando en handle_request con acción:", action)
    if(action == ''):
        action = 'add'
    if request.method == 'POST':
        logger.info("Método POST recibido")
        print("Método POST recibido")
        try:
            file = request.FILES['file']
            logger.info("Archivo recibido: %s", file.name)
            print("Archivo recibido:", file.name)
            dataframe = pd.read_excel(file)
            logger.info("Dataframe cargado con éxito")
            print("Dataframe cargado con éxito")

            validation_error = ContainerService.validate_columns(dataframe, action)
            if validation_error:
                logger.warning("Error de validación: %s", validation_error)
                print("Error de validación:", validation_error)
                return JsonResponse({"error": validation_error}, status=400)

            data = dataframe.to_dict(orient='records')
            logger.info("Datos recibidos: %s", data)

            if action == 'add':
                row_status = ContainerService.registrar_contenedores(data)
            elif action == 'delete':
                row_status = ContainerService.eliminar_contenedores(data)
            elif action == 'update':
                row_status = ContainerService.actualizar_unidad_de_medida(data)

            output = ContainerService.create_excel(row_status, f'resultado_{action}.xlsx')
            logger.info("Creación del archivo de salida completada")
            print("Creación del archivo de salida completada")
            return FileResponse(output, as_attachment=True, filename=f'resultado_{action}.xlsx')

        except Exception as e:
            logger.error("Error al procesar la solicitud: %s", e, exc_info=True)
            print("Error al procesar la solicitud:", e)
            return JsonResponse({"error": str(e)}, status=500)

        
@api_view(['GET'])
def getWorkUnitAssorted(request, launch_num):
    try:
        wmsDao=WMSDao()
        recibosList=wmsDao.getAssortedWorkUnit(launch_num)
        serializer=AssortedWorkUnitSerializer(recibosList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
       
@api_view(['GET'])
def getWorkUnitAssortedFile(request,launch_num):
    try:
        wmsDao=WMSDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'Contenedor Id')
        worksheet.write(0, 1, 'Tipo de contenedor')
        worksheet.write(0, 2, 'Unidad de trabajo')
        worksheet.write(0, 3, 'Ubicacion Origen')
        worksheet.write(0, 4, 'Articulo')
        worksheet.write(0, 5, 'Cantidad')
        
        TopList=wmsDao.getAssortedWorkUnit(launch_num)
        
        row=1
        for top in TopList:
            worksheet.write(row, 0, top.container_id)
            worksheet.write(row, 1, top.container_type)
            worksheet.write(row, 2, top.work_unit)
            worksheet.write(row, 3, top.from_loc)
            worksheet.write(row, 4, top.item)
            worksheet.write(row, 5, top.quantity)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'UnidadTrabajoSurtidoReserva.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


db_helper = ContainerService()

expected_columns = {
    'add': ['CONTAINER_TYPE', 'DESCRIPTION', 'CONTAINER_CLASS', 'EMPTY_WEIGHT', 'WEIGHT_UM', 'LENGTH', 'WIDTH', 'HEIGHT', 'ACTIVE', 'WEIGHT_TOLERANCE', 'MAXIMUM_WEIGHT'],
    'delete': ['CONTAINER_TYPE'],
    'update': ['SKU', 'UM', 'LONGITUD', 'ANCHURA', 'ALTURA', 'PESO'],
    'promotion':['CÓDIGO', 'COD DE BARRAS', 'DESCRIPCIÓN', 'CATEGORIA', 'CLASE', 'promoción final', 'precio normal', 'precio promoción', 'Inicio Vigencia', 'FIN', 'TIPO DE PRODUCTO']
}

@csrf_exempt
@api_view(['POST'])
def registrar_contenedores(request):
    if request.method == 'POST':
        try:
            print("Agregando contenedores...")
            file = request.FILES['file']
            dataframe = pd.read_excel(file)

            # Cambiando tipo de datos
            dataframe['CONTAINER_TYPE'] = dataframe['CONTAINER_TYPE'].astype(str)
            dataframe['DESCRIPTION'] = dataframe['DESCRIPTION'].astype(str)
            dataframe['CONTAINER_CLASS'] = dataframe['CONTAINER_CLASS'].astype(str)
            dataframe['EMPTY_WEIGHT'] = dataframe['EMPTY_WEIGHT'].astype(float)
            dataframe['WEIGHT_UM'] = dataframe['WEIGHT_UM'].astype(str)
            dataframe['LENGTH'] = dataframe['LENGTH'].astype(float)
            dataframe['WIDTH'] = dataframe['WIDTH'].astype(float)
            dataframe['HEIGHT'] = dataframe['HEIGHT'].astype(float)
            dataframe['ACTIVE'] = dataframe['ACTIVE'].astype(str)
            dataframe['WEIGHT_TOLERANCE'] = dataframe['WEIGHT_TOLERANCE'].astype(float)
            dataframe['MAXIMUM_WEIGHT'] = dataframe['MAXIMUM_WEIGHT'].astype(float)

            missing_columns = [col for col in expected_columns['add'] if col not in dataframe.columns]
            if missing_columns:
                return JsonResponse({"error": f"Faltan las siguientes columnas: {', '.join(missing_columns)}"}, status=400)

            validation_error = db_helper.validate_columns(dataframe, 'add', expected_columns)
            if validation_error:
                return JsonResponse({"error": validation_error}, status=400)

            data = dataframe.to_dict(orient='records')

            container_types = list(set(row['CONTAINER_TYPE'] for row in data))
            existing_containers = db_helper.check_existing_containers(container_types)

            output = io.BytesIO()
            workbook = xlsxwriter.Workbook(output)
            worksheet = workbook.add_worksheet()
            worksheet.write(0, 0, 'CONTAINER_TYPE')
            worksheet.write(0, 1, 'Mensaje')

            values_to_insert = []
            row_status = []
            seen_containers = set()

            for row in data:
                if row['CONTAINER_TYPE'] in seen_containers or row['CONTAINER_TYPE'] in existing_containers:
                    row_status.append((row['CONTAINER_TYPE'], 'Error: Contenedor duplicado'))
                else:
                    seen_containers.add(row['CONTAINER_TYPE'])
                    values_to_insert.append((row['CONTAINER_TYPE'], row['DESCRIPTION'], row['CONTAINER_CLASS'], row['EMPTY_WEIGHT'],
                                             row['WEIGHT_UM'], row['LENGTH'], row['WIDTH'], row['HEIGHT'], row['ACTIVE'],
                                             row['WEIGHT_TOLERANCE'], row['MAXIMUM_WEIGHT']))
                    row_status.append((row['CONTAINER_TYPE'], 'Insertado correctamente'))

            insertion_result = db_helper.insert_containers(values_to_insert)
            if isinstance(insertion_result, str):
                print("Error agregando contenedores")
                return JsonResponse({"error": f"Error al insertar contenedores: {insertion_result}"}, status=500)

            
            # Verificar si todos los contenedores se insertaron correctamente
            for attempt in range(10):
                inserted_containers = db_helper.check_existing_containers([row['CONTAINER_TYPE'] for row in data])
                not_inserted = [row for row in data if row['CONTAINER_TYPE'] not in inserted_containers]
                if not not_inserted:
                    break
                insertion_result = db_helper.insert_containers([(row['CONTAINER_TYPE'], row['DESCRIPTION'], row['CONTAINER_CLASS'], row['EMPTY_WEIGHT'],
                                                                row['WEIGHT_UM'], row['LENGTH'], row['WIDTH'], row['HEIGHT'], row['ACTIVE'],
                                                                row['WEIGHT_TOLERANCE'], row['MAXIMUM_WEIGHT']) for row in not_inserted])
                if isinstance(insertion_result, str):
                    print("Error agregando contenedores")
                    return JsonResponse({"error": f"Error al insertar contenedores: {insertion_result}"}, status=500)

            # Si después de 10 intentos aún hay contenedores no insertados, agregar mensaje de error
            if not_inserted:
                for row in not_inserted:
                    row_status.append((row['CONTAINER_TYPE'], 'Error: No se pudo insertar después de 10 intentos'))

            for idx, status in enumerate(row_status):
                worksheet.write(idx + 1, 0, status[0])
                worksheet.write(idx + 1, 1, status[1])

            workbook.close()
            output.seek(0)
            if output:
                print(output)
                print("Contenedores agregados correctamente")
            else:
                print(output)
            # return FileResponse(output, as_attachment=True, filename='resultado_inserciones.xlsx')
        
            response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = 'attachment; filename=resultado_inserciones.xlsx'
            return response

        except Exception as e:
            print("Error:", str(e))
            return JsonResponse({"error": str(e)}, status=500)

@csrf_exempt
@api_view(['POST'])
def eliminar_contenedores(request):
    print("Depurando contenedores...")
    if request.method == 'POST':
        try:
            file = request.FILES['file']
            dataframe = pd.read_excel(file)
            # dataframe['CONTAINER_TYPE'] = dataframe['CONTAINER_TYPE'].astype(str)
            missing_columns = [col for col in expected_columns['delete'] if col not in dataframe.columns]
            if missing_columns:
                return JsonResponse({"error": f"Faltan las siguientes columnas: {', '.join(missing_columns)}"}, status=400)
            
            # validation_error = db_helper.validate_columns(dataframe, 'delete', expected_columns)
            # if validation_error:
            #     return JsonResponse({"error": validation_error}, status=400)

            data = dataframe.to_dict(orient='records')
            container_types = list(set(row['CONTAINER_TYPE'] for row in data))
            existing_containers = db_helper.check_existing_containers(container_types)
            output = io.BytesIO()
            workbook = xlsxwriter.Workbook(output)
            worksheet = workbook.add_worksheet()
            worksheet.write(0, 0, 'CONTAINER_TYPE')
            worksheet.write(0, 1, 'Mensaje')
            row_status = []
            seen_containers = set() 
            
            for row in data:
                if row['CONTAINER_TYPE'] in seen_containers:
                    row_status.append((row['CONTAINER_TYPE'], 'Error: Contenedor no encontrado'))
                elif row['CONTAINER_TYPE'] not in existing_containers:
                    row_status.append((row['CONTAINER_TYPE'], 'Error: Contenedor no encontrado'))
                else:
                    seen_containers.add(row['CONTAINER_TYPE'])
                    row_status.append((row['CONTAINER_TYPE'], 'Eliminado correctamente'))

            deletion_result = db_helper.delete_containers(container_types)
            if isinstance(deletion_result, str):
                print("Error borrando contenedores")
                return JsonResponse({"error": f"Error al eliminar contenedores: {deletion_result}"}, status=500)

            for idx, status in enumerate(row_status):
                worksheet.write(idx + 1, 0, status[0])
                worksheet.write(idx + 1, 1, status[1])
            workbook.close()
            output.seek(0)
            print("Contenedores eliminados correctamente")
            # return FileResponse(output, as_attachment=True, filename='resultado_eliminaciones.xlsx')
        
            response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = 'attachment; filename=resultado_eliminaciones.xlsx'
            return response
        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)

@csrf_exempt
@api_view(['POST'])
def actualizar_unidad_de_medida(request):
    print("Actualizando dimensiones...")
    if request.method == 'POST':
        try:
            file = request.FILES['file']
            dataframe = pd.read_excel(file, dtype={'SKU': str, 'UM': str})

            # Cambiando tipo de datos
            dataframe['SKU'] = dataframe['SKU'].astype(str)
            dataframe['UM'] = dataframe['UM'].astype(str)
            dataframe['LONGITUD'] = dataframe['LONGITUD'].astype(float)
            dataframe['ANCHURA'] = dataframe['ANCHURA'].astype(float)
            dataframe['ALTURA'] = dataframe['ALTURA'].astype(float)
            dataframe['PESO'] = dataframe['PESO'].astype(float)

            validation_error = db_helper.validate_columns(dataframe, 'update', expected_columns)
            missing_columns = [col for col in expected_columns['update'] if col not in dataframe.columns]
            print(validation_error)
            if validation_error:
                return JsonResponse({"error": validation_error}, status=400)
            data = dataframe.to_dict(orient='records')
            logger.info("Datos recibidos: %s", data)

            update_result = db_helper.actualizar_unidad_de_medida(data)
            if isinstance(update_result, str):
                print("Error al actualizar las dimensiones")
                return JsonResponse({"error": update_result}, status=500)
            
            no_encontrados = [row for row in data if (row['SKU'], row['UM']) not in db_helper.check_existing_items([row['SKU']], [row['UM']])]
            output = io.BytesIO()
            workbook = xlsxwriter.Workbook(output)
            worksheet = workbook.add_worksheet()
            worksheet.write(0, 0, 'SKU')
            worksheet.write(0, 1, 'UM')
            worksheet.write(0, 2, 'Mensaje')

            for idx, row in enumerate(data):
                if row in no_encontrados:
                    worksheet.write(idx + 1, 0, row['SKU'])
                    worksheet.write(idx + 1, 1, row['UM'])
                    worksheet.write(idx + 1, 2, 'No encontrado')
                else:
                    worksheet.write(idx + 1, 0, row['SKU'])
                    worksheet.write(idx + 1, 1, row['UM'])
                    worksheet.write(idx + 1, 2, 'Actualizado correctamente')

            workbook.close()
            output.seek(0)
            logger.info("Archivo Excel creado con éxito")
            print("Dimensiones actualizadas correctamente")
            # return FileResponse(output, as_attachment=True, filename='resultado_actualizaciones.xlsx')
        
            response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = 'attachment; filename=resultado_actualizaciones.xlsx'
            return response

        except Exception as e:
            logger.error("Error: %s", e)
            return JsonResponse({"error": str(e)}, status=500)
        
@api_view(['GET'])
def getInsertItemUbicacion(request, item, location):
    try:
        print("Item Location: ")
        wmsDao = WMSDao()
        itemFound = wmsDao.getItemLocationInsert(item, location)
        if itemFound:
            return Response({'message': 'Artículo encontrado correctamente.'})
        else:
            return Response({'message': 'Artículo no encontrado.'}, status=status.HTTP_404_NOT_FOUND)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def getItemLocation(request):
    try:
        wmsDao=WMSDao()
        recibosList=wmsDao.getItemLocation()
        serializer=ItemLocationSerializer(recibosList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

       
@api_view(['GET'])
def getDownloadItemLocation(request,date):
    try:
        date = str(date)
        # print(date)
        wmsDao=WMSDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'ID')
        worksheet.write(0, 1, 'Articulo')
        worksheet.write(0, 2, 'Ubicacion')
        worksheet.write(0, 3, 'Encontrado')
        worksheet.write(0, 4, 'Fecha')
        
        itemList=wmsDao.getItemLocation(date)
        
        row=1
        for item in itemList:
            worksheet.write(row, 0, item.id)
            worksheet.write(row, 1, item.item)
            worksheet.write(row, 2, item.location)
            worksheet.write(row, 3, item.found)
            worksheet.write(row, 4, item.date)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'ItemLocation.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    

db_helper_cl = ContainerServiceCl()

@csrf_exempt
@api_view(['POST'])
def registrar_contenedores_cl(request):
    if request.method == 'POST':
        try:
            print("Agregando contenedores...")
            file = request.FILES['file']
            dataframe = pd.read_excel(file)

            # Cambiando tipo de datos
            dataframe['CONTAINER_TYPE'] = dataframe['CONTAINER_TYPE'].astype(str)
            dataframe['DESCRIPTION'] = dataframe['DESCRIPTION'].astype(str)
            dataframe['CONTAINER_CLASS'] = dataframe['CONTAINER_CLASS'].astype(str)
            dataframe['EMPTY_WEIGHT'] = dataframe['EMPTY_WEIGHT'].astype(float)
            dataframe['WEIGHT_UM'] = dataframe['WEIGHT_UM'].astype(str)
            dataframe['LENGTH'] = dataframe['LENGTH'].astype(float)
            dataframe['WIDTH'] = dataframe['WIDTH'].astype(float)
            dataframe['HEIGHT'] = dataframe['HEIGHT'].astype(float)
            dataframe['ACTIVE'] = dataframe['ACTIVE'].astype(str)
            dataframe['WEIGHT_TOLERANCE'] = dataframe['WEIGHT_TOLERANCE'].astype(float)
            dataframe['MAXIMUM_WEIGHT'] = dataframe['MAXIMUM_WEIGHT'].astype(float)

            missing_columns = [col for col in expected_columns['add'] if col not in dataframe.columns]
            if missing_columns:
                return JsonResponse({"error": f"Faltan las siguientes columnas: {', '.join(missing_columns)}"}, status=400)

            # validation_error = db_helper_cl.validate_columns(dataframe, 'add', expected_columns)
            # if validation_error:
            #     return JsonResponse({"error": validation_error}, status=400)

            data = dataframe.to_dict(orient='records')

            container_types = list(set(row['CONTAINER_TYPE'] for row in data))
            existing_containers = db_helper_cl.check_existing_containers(container_types)

            output = io.BytesIO()
            workbook = xlsxwriter.Workbook(output)
            worksheet = workbook.add_worksheet()
            worksheet.write(0, 0, 'CONTAINER_TYPE')
            worksheet.write(0, 1, 'Mensaje')

            values_to_insert = []
            row_status = []
            seen_containers = set()

            for row in data:
                if row['CONTAINER_TYPE'] in seen_containers or row['CONTAINER_TYPE'] in existing_containers:
                    row_status.append((row['CONTAINER_TYPE'], 'Error: Contenedor duplicado'))
                else:
                    seen_containers.add(row['CONTAINER_TYPE'])
                    values_to_insert.append((row['CONTAINER_TYPE'], row['DESCRIPTION'], row['CONTAINER_CLASS'], row['EMPTY_WEIGHT'],
                                             row['WEIGHT_UM'], row['LENGTH'], row['WIDTH'], row['HEIGHT'], row['ACTIVE'],
                                             row['WEIGHT_TOLERANCE'], row['MAXIMUM_WEIGHT']))
                    row_status.append((row['CONTAINER_TYPE'], 'Insertado correctamente'))

            insertion_result = db_helper_cl.insert_containers(values_to_insert)
            if isinstance(insertion_result, str):
                print("Error agregando contenedores")
                return JsonResponse({"error": f"Error al insertar contenedores: {insertion_result}"}, status=500)

            # Verificar si todos los contenedores se insertaron correctamente
            for attempt in range(10):
                inserted_containers = db_helper.check_existing_containers([row['CONTAINER_TYPE'] for row in data])
                not_inserted = [row for row in data if row['CONTAINER_TYPE'] not in inserted_containers]
                if not not_inserted:
                    break
                insertion_result = db_helper.insert_containers([(row['CONTAINER_TYPE'], row['DESCRIPTION'], row['CONTAINER_CLASS'], row['EMPTY_WEIGHT'],
                                                                row['WEIGHT_UM'], row['LENGTH'], row['WIDTH'], row['HEIGHT'], row['ACTIVE'],
                                                                row['WEIGHT_TOLERANCE'], row['MAXIMUM_WEIGHT']) for row in not_inserted])
                if isinstance(insertion_result, str):
                    print("Error agregando contenedores")
                    return JsonResponse({"error": f"Error al insertar contenedores: {insertion_result}"}, status=500)

            # Si después de 10 intentos aún hay contenedores no insertados, agregar mensaje de error
            if not_inserted:
                for row in not_inserted:
                    row_status.append((row['CONTAINER_TYPE'], 'Error: No se pudo insertar después de 10 intentos'))

            for idx, status in enumerate(row_status):
                worksheet.write(idx + 1, 0, status[0])
                worksheet.write(idx + 1, 1, status[1])

            workbook.close()
            output.seek(0)
            if output:
                print(output)
                print("Contenedores agregados correctamente")
            else:
                print(output)
            # return FileResponse(output, as_attachment=True, filename='resultado_inserciones.xlsx')
        
            response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = 'attachment; filename=resultado_inserciones.xlsx'
            return response

        except Exception as e:
            print("Error:", str(e))
            return JsonResponse({"error": str(e)}, status=500)

@csrf_exempt
@api_view(['POST'])
def eliminar_contenedores_cl(request):
    print("Depurando contenedores...")
    if request.method == 'POST':
        try:
            file = request.FILES['file']
            dataframe = pd.read_excel(file)
            missing_columns = [col for col in expected_columns['delete'] if col not in dataframe.columns]
            if missing_columns:
                return JsonResponse({"error": f"Faltan las siguientes columnas: {', '.join(missing_columns)}"}, status=400)

            validation_error = db_helper_cl.validate_columns(dataframe, 'delete', expected_columns)
            if validation_error:
                return JsonResponse({"error": validation_error}, status=400)

            data = dataframe.to_dict(orient='records')
            container_types = list(set(row['CONTAINER_TYPE'] for row in data))
            existing_containers = db_helper_cl.check_existing_containers(container_types)

            output = io.BytesIO()
            workbook = xlsxwriter.Workbook(output)
            worksheet = workbook.add_worksheet()
            worksheet.write(0, 0, 'CONTAINER_TYPE')
            worksheet.write(0, 1, 'Mensaje')
            row_status = []
            seen_containers = set() 
            
            for row in data:
                if row['CONTAINER_TYPE'] in seen_containers:
                    row_status.append((row['CONTAINER_TYPE'], 'Error: Contenedor no encontrado'))
                elif row['CONTAINER_TYPE'] not in existing_containers:
                    row_status.append((row['CONTAINER_TYPE'], 'Error: Contenedor no encontrado'))
                else:
                    seen_containers.add(row['CONTAINER_TYPE'])
                    row_status.append((row['CONTAINER_TYPE'], 'Eliminado correctamente'))

            deletion_result = db_helper_cl.delete_containers(container_types)
            if isinstance(deletion_result, str):
                print("Error eliminando contenedores")
                return JsonResponse({"error": f"Error al eliminar contenedores: {deletion_result}"}, status=500)

            for idx, status in enumerate(row_status):
                worksheet.write(idx + 1, 0, status[0])
                worksheet.write(idx + 1, 1, status[1])
            workbook.close()
            output.seek(0)
            print("Contenedores eliminados correctamente")
            # return FileResponse(output, as_attachment=True, filename='resultado_eliminaciones.xlsx')

            response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = 'attachment; filename=resultado_eliminaciones.xlsx'
            return response
        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)

@csrf_exempt
@api_view(['POST'])
def actualizar_unidad_de_medida_cl(request):
    print("Actualizando dimensiones...")
    if request.method == 'POST':
        try:
            file = request.FILES['file']
            dataframe = pd.read_excel(file, dtype={'SKU': str, 'UM': str})

            # Cambiando tipo de datos
            dataframe['SKU'] = dataframe['SKU'].astype(str)
            dataframe['UM'] = dataframe['UM'].astype(str)
            dataframe['LONGITUD'] = dataframe['LONGITUD'].astype(float)
            dataframe['ANCHURA'] = dataframe['ANCHURA'].astype(float)
            dataframe['ALTURA'] = dataframe['ALTURA'].astype(float)
            dataframe['PESO'] = dataframe['PESO'].astype(float)
            
            # Validar columnas
            validation_error = db_helper_cl.validate_columns(dataframe, 'update', expected_columns)
            if validation_error:
                return JsonResponse({"error": validation_error}, status=400)
            
            data = dataframe.to_dict(orient='records')
            logger.info("Datos recibidos: %s", data)

            # Actualizar unidad de medida
            update_result = db_helper_cl.actualizar_unidad_de_medida(data)
            if isinstance(update_result, str):
                print("Error al actualizar dimensiones")
                return JsonResponse({"error": update_result}, status=500)
            
            # Verificar elementos no encontrados
            no_encontrados = [row for row in data if (row['SKU'], row['UM']) not in db_helper_cl.check_existing_items([row['SKU']], [row['UM']])]
            output = io.BytesIO()
            workbook = xlsxwriter.Workbook(output)
            worksheet = workbook.add_worksheet()
            worksheet.write(0, 0, 'SKU')
            worksheet.write(0, 1, 'UM')
            worksheet.write(0, 2, 'Mensaje')

            for idx, row in enumerate(data):
                if row in no_encontrados:
                    worksheet.write(idx + 1, 0, row['SKU'])
                    worksheet.write(idx + 1, 1, row['UM'])
                    worksheet.write(idx + 1, 2, 'No encontrado')
                else:
                    worksheet.write(idx + 1, 0, row['SKU'])
                    worksheet.write(idx + 1, 1, row['UM'])
                    worksheet.write(idx + 1, 2, 'Actualizado correctamente')

            workbook.close()
            output.seek(0)
            logger.info("Archivo Excel creado con éxito")
            print("Dimensiones actualizadas correctamente")

            response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = 'attachment; filename=resultado_actualizaciones_cl.xlsx'
            return response
        except Exception as e:
            logger.error("Error: %s", e)
            return JsonResponse({"error": str(e)}, status=500)
        
@csrf_exempt
@api_view(['POST'])
def registrar_promociones(request):
    if request.method == 'POST':
        try:
            print("Agregando promociones...")
            file = request.FILES['file']
            dataframe = pd.read_excel(file)

            required_fields = ['CÓDIGO', 'COD DE BARRAS', 'DESCRIPCIÓN', 'CATEGORIA', 'CLASE', 'promoción final', 'precio normal', 'precio promoción', 'Inicio Vigencia', 'FIN', 'TIPO DE PRODUCTO']
            missing_columns = [col for col in required_fields if col not in dataframe.columns]
            if missing_columns:
                return JsonResponse({"error": f"Faltan las siguientes columnas: {', '.join(missing_columns)}"}, status=400)
            else:
                print("Columnas correctas")

            data = dataframe.to_dict(orient='records')

            update_promotion = UpdatePromotion()

            if not update_promotion.validate_promotions():
                return JsonResponse({"error": "Error en la validación de promociones"}, status=400)
            else:
                print("Depuración completa")

            batch_size = 1000
            results = []

            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]

                batch_results = update_promotion.verificar_promociones(batch)
                results.extend(batch_results)

            results_df = pd.DataFrame(results, columns=["Respuesta", "Codigo", "Precio promoción", "Inicio Vigencia", "Fin Vigencia"])

            results_df["Codigo"] = results_df["Codigo"].astype(str)

            output = BytesIO()
            writer = pd.ExcelWriter(output, engine='xlsxwriter')
            results_df.to_excel(writer, index=False, sheet_name='Resultados')

            workbook = writer.book
            worksheet = writer.sheets['Resultados']

            text_format = workbook.add_format({'num_format': '@'})
            worksheet.set_column('B:B', None, text_format)

            writer.close()
            output.seek(0)

            print("Promociones agregadas")

            response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = 'attachment; filename=promociones_resultados.xlsx'
            return response

        except Exception as e:
            logger.error(f"Error al registrar promociones: {e}")
            print(e)
            return JsonResponse({"error": str(e)}, status=500)
        
@api_view(['GET'])
def getDescripcionTransacciones(request):
    try:
        wmsDao=WMSDao()
        transactionList=wmsDao.getTransactionIdentifier()
        serializer=DescriptionTransactions(transactionList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def getShorpacks(request,date):
    try:
        date = str(date)
        wmsDao=WMSDao()
        shorpackList=wmsDao.getShorpack(date)
        serializer=Shorpacks(shorpackList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        print(exception)
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getDownloadShorpack(request,date):
    try:
        date = str(date)
        wmsDao=WMSDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'WavePack')
        worksheet.write(0, 1, 'Tienda')
        worksheet.write(0, 2, 'Articulo')
        worksheet.write(0, 3, 'Recibo')
        worksheet.write(0, 4, 'Piezas solicitadas')
        worksheet.write(0, 5, 'Piezas recibidas')
        worksheet.write(0, 6, 'Piezas rechazadas')
        worksheet.write(0, 7, 'Piezas faltantes')
        worksheet.write(0, 8, 'Fecha')
        
        shorpackList=wmsDao.getShorpack(date)
        
        row=1
        for shorpack in shorpackList:
            worksheet.write(row, 0, shorpack.pickWaveCode)
            worksheet.write(row, 1, shorpack.clientCode)
            worksheet.write(row, 2, shorpack.productCode)
            worksheet.write(row, 3, shorpack.documentCode)
            worksheet.write(row, 4, shorpack.request_qty)
            worksheet.write(row, 5, shorpack.total_qty)
            worksheet.write(row, 6, shorpack.rechazadas)
            worksheet.write(row, 7, shorpack.pzasFaltantes)
            worksheet.write(row, 8, shorpack.fecha)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'shorpack_' + date + '.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        print("Shorpack descargado")
        return response
    except Exception as exception:
        print(exception)
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def getInventoryAvailable(request,date):
    try:
        one = "100"
        wmsDao=WMSDao()
        inventoryAvailableList=wmsDao.getInventoryAvailableDaily(date,one)
        serializer=InventoryAvailable(inventoryAvailableList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def getDownloadInventoryAvailable(request,date):
    try:
        date = str(date)
        wmsDao=WMSDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'Item')
        worksheet.write(0, 1, 'On Hand')
        worksheet.write(0, 2, 'In Transit')
        worksheet.write(0, 3, 'Allocated')
        worksheet.write(0, 4, 'Suspense')
        worksheet.write(0, 5, 'Requested')
        worksheet.write(0, 6, 'Quantity')
        worksheet.write(0, 7, 'Real Disponible')
        worksheet.write(0, 8, 'Fecha')
        
        inventoryList=wmsDao.getInventoryAvailableDaily(date)
        
        row=1
        for inventory in inventoryList:
            worksheet.write(row, 0, inventory.item)
            worksheet.write(row, 1, inventory.on_hand)
            worksheet.write(row, 2, inventory.in_transit)
            worksheet.write(row, 3, inventory.allocated)
            worksheet.write(row, 4, inventory.suspense)
            worksheet.write(row, 5, inventory.requested)
            worksheet.write(row, 6, inventory.quantity)
            worksheet.write(row, 7, inventory.real_available)
            worksheet.write(row, 8, inventory.date_time)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'Inventory_Available_' + date + '.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        print("Inventory available descargado")
        return response
    except Exception as exception:
        print(exception)
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@csrf_exempt
@api_view(['POST'])
def actualizar_unidad_de_medida_col(request):
    print("Actualizando dimensiones...")
    if request.method == 'POST':
        try:
            file = request.FILES['file']
            dataframe = pd.read_excel(file, dtype={'SKU': str, 'UM': str})

            # Cambiando tipo de datos
            dataframe['SKU'] = dataframe['SKU'].astype(str)
            dataframe['UM'] = dataframe['UM'].astype(str)
            dataframe['LONGITUD'] = dataframe['LONGITUD'].astype(float)
            dataframe['ANCHURA'] = dataframe['ANCHURA'].astype(float)
            dataframe['ALTURA'] = dataframe['ALTURA'].astype(float)
            dataframe['PESO'] = dataframe['PESO'].astype(float)
            
            db_helper_col = ContainerServiceCol()

            # Validar columnas
            validation_error = db_helper_col.validate_columns(dataframe, 'update', expected_columns)
            if validation_error:
                return JsonResponse({"error": validation_error}, status=400)
            
            data = dataframe.to_dict(orient='records')
            logger.info("Datos recibidos: %s", data)

            # Actualizar unidad de medida
            update_result = db_helper_col.actualizar_unidad_de_medida(data)
            if isinstance(update_result, str):
                print("Error al actualizar dimensiones")
                return JsonResponse({"error": update_result}, status=500)
            
            # Verificar elementos no encontrados
            no_encontrados = [row for row in data if (row['SKU'], row['UM']) not in db_helper_col.check_existing_items([row['SKU']], [row['UM']])]
            output = io.BytesIO()
            workbook = xlsxwriter.Workbook(output)
            worksheet = workbook.add_worksheet()
            worksheet.write(0, 0, 'SKU')
            worksheet.write(0, 1, 'UM')
            worksheet.write(0, 2, 'Mensaje')

            for idx, row in enumerate(data):
                if row in no_encontrados:
                    worksheet.write(idx + 1, 0, row['SKU'])
                    worksheet.write(idx + 1, 1, row['UM'])
                    worksheet.write(idx + 1, 2, 'No encontrado')
                else:
                    worksheet.write(idx + 1, 0, row['SKU'])
                    worksheet.write(idx + 1, 1, row['UM'])
                    worksheet.write(idx + 1, 2, 'Actualizado correctamente')

            workbook.close()
            output.seek(0)
            logger.info("Archivo Excel creado con éxito")
            print("Dimensiones actualizadas correctamente")

            response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = 'attachment; filename=resultado_actualizaciones_col.xlsx'
            return response
        except Exception as e:
            logger.error("Error: %s", e)
            return JsonResponse({"error": str(e)}, status=500)

@api_view(['GET'])
def getDownloadSkuPriority(request):
    try:
        tiendaDao=RecepcionTiendaDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'SKUPrioritarioITEM')
        worksheet.write(0, 1, 'SKUPrioritarioTDA')
        
        inventoryList=tiendaDao.getSkuPriority()
        
        row=1
        for inventory in inventoryList:
            worksheet.write(row, 0, inventory.sKUPrioritarioITEM)
            worksheet.write(row, 1, inventory.sKUPrioritarioTDA)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'SkuPrioritario.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        print("Sku prioritatio descargado")
        return response
    except Exception as exception:
        print(exception)
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def locationType(request):
    try:
        wmsDao=WMSDao()
        locationList=wmsDao.getLocationType()
        serializer=OneValueSerializer(locationList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
def locationZone(request):
    try:
        wmsDao=WMSDao()
        locationList=wmsDao.getLocationZone()
        serializer=OneValueSerializer(locationList, many=True)
        return Response(serializer.data)
    except Exception as exception:
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
def getDownloadInventoryAParams(request,typeP,zone):
    try:
        tP = []
        zP = []
        typeP = typeP.split(",")
        for i in typeP:
            i = i.replace("_"," ")
            tP.append(i)
        
        zone = zone.split(",")        
        for i in zone:
            i = i.replace("_"," ")
            zP.append(i)
            
        print(tP,zP)

        wmsDao=WMSDao()
        output = io.BytesIO()

        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'Item')
        worksheet.write(0, 1, 'On Hand')
        worksheet.write(0, 2, 'In Transit')
        worksheet.write(0, 3, 'Allocated')
        worksheet.write(0, 4, 'Suspense')
        worksheet.write(0, 5, 'Requested')
        worksheet.write(0, 6, 'Quantity')
        worksheet.write(0, 7, 'Real Disponible')
        
        inventoryList=wmsDao.getInventoryAvailableDailyParams(tP,zP)
        
        row=1
        for inventory in inventoryList:
            worksheet.write(row, 0, inventory.item)
            worksheet.write(row, 1, inventory.on_hand)
            worksheet.write(row, 2, inventory.in_transit)
            worksheet.write(row, 3, inventory.allocated)
            worksheet.write(row, 4, inventory.suspense)
            worksheet.write(row, 5, inventory.requested)
            worksheet.write(row, 6, inventory.quantity)
            worksheet.write(row, 7, inventory.real_available)
            row=row+1

        workbook.close()

        output.seek(0)

        filename = 'Inventory_Available_.xlsx'
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        print("Inventory available descargado")
        return response
    except Exception as exception:
        print(exception)
        logger.error(f'Se presento una incidencia: {exception}')
        return Response({'Error': f'{exception}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)