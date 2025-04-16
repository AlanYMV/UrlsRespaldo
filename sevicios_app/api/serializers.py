from rest_framework import serializers

class PedidoSerializer(serializers.Serializer):
    numero=serializers.CharField()
    
class CargaSerializer(serializers.Serializer):
    numero=serializers.CharField()
    
class ArticuloSerializer(serializers.Serializer):
    sku=serializers.CharField()
    descripcionSku=serializers.CharField()
    codigoSat=serializers.CharField()
    codigoProveedor=serializers.CharField()
    proveedor=serializers.CharField()
    unidadMedidaCompra=serializers.CharField()
    claveUnidad=serializers.CharField()
    pesoArticulo=serializers.CharField()
    itemsUnidadCompra=serializers.CharField()
    cantidadPaquete=serializers.CharField()
    
class DetallePedidoSerializer(serializers.Serializer):
    cantidad=serializers.CharField()
    idUnidadEmbalaje=serializers.CharField()
    descripcionMaterialCarga=serializers.CharField()
    pesoArticulo=serializers.CharField()
    idUnidadPeso=serializers.CharField()
    claveProductoServicio=serializers.CharField()
    claveUnidadMedidaEmbalaje=serializers.CharField()
    claveUnidad=serializers.CharField()
    materialPeligroso=serializers.CharField()
    pedido=serializers.CharField()
    tienda=serializers.CharField()
    nombreTienda=serializers.CharField()
    codigoPostal=serializers.CharField()
    pais=serializers.CharField()
    estado=serializers.CharField()
    direccion=serializers.CharField()
    unidadMedida=serializers.CharField()
    idOrigen=serializers.CharField()
    idDestino=serializers.CharField()
    volumen=serializers.CharField()
    unidadVolumen=serializers.CharField()

class DetalleCargaSerializer(serializers.Serializer):
    cantidad=serializers.CharField()
    idUnidadEmbalaje=serializers.CharField()
    descripcionMaterialCarga=serializers.CharField()
    pesoArticulo=serializers.CharField()
    idUnidadPeso=serializers.CharField()
    claveProductoServicio=serializers.CharField()
    claveUnidadMedidaEmbalaje=serializers.CharField()
    claveUnidad=serializers.CharField()
    materialPeligroso=serializers.CharField()
    pedido=serializers.CharField()
    tienda=serializers.CharField()
    nombreTienda=serializers.CharField()
    codigoPostal=serializers.CharField()
    pais=serializers.CharField()
    estado=serializers.CharField()
    direccion=serializers.CharField()
    unidadMedida=serializers.CharField()
    idOrigen=serializers.CharField()
    idDestino=serializers.CharField()
    volumen=serializers.CharField()
    unidadVolumen=serializers.CharField()

class StorageTemplateSerializer(serializers.Serializer):
    itemCode=serializers.CharField()
    storageTemplate=serializers.CharField()
    salUnitMsr=serializers.CharField()
    familia=serializers.CharField()
    subFamilia=serializers.CharField()
    subSubFamilia=serializers.CharField()
    uSysCat4=serializers.CharField()
    uSysCat5=serializers.CharField()
    uSysCat6=serializers.CharField()
    uSysCat7=serializers.CharField()
    uSysCat8=serializers.CharField()
    height=serializers.CharField()
    width=serializers.CharField()
    length=serializers.CharField()
    volume=serializers.CharField()
    weight=serializers.CharField()

class PreciosSerializer(serializers.Serializer):
    itemCode=serializers.CharField()
    codigoBarras=serializers.CharField()
    categoria=serializers.CharField()
    subcategoria=serializers.CharField()
    clase=serializers.CharField()
    itemName=serializers.CharField()
    storageTemplate=serializers.CharField()
    stUsr=serializers.CharField()
    licencia=serializers.CharField()
    height=serializers.CharField()
    width=serializers.CharField()
    length=serializers.CharField()
    volume=serializers.CharField()
    weight=serializers.CharField()
    fvEstandar=serializers.CharField()
    fvAptosCdmx=serializers.CharField()
    fvAptosForaneos=serializers.CharField()
    fvAptosFronterizos=serializers.CharField()
    fvOutlet=serializers.CharField()
    fvFrontera=serializers.CharField()
    proveedor=serializers.CharField()

class PrecioClSerializer(serializers.Serializer):
    itemCode=serializers.CharField()
    codigoBarras=serializers.CharField()
    categoria=serializers.CharField()
    subcategoria=serializers.CharField()
    clase=serializers.CharField()
    itemName=serializers.CharField()
    storageTemplate=serializers.CharField()
    stUsr=serializers.CharField()
    licencia=serializers.CharField()
    height=serializers.CharField()
    width=serializers.CharField()
    length=serializers.CharField()
    volume=serializers.CharField()
    weight=serializers.CharField()
    precioSinIva=serializers.CharField()
    precioIva=serializers.CharField()
    precioLineaSinIva=serializers.CharField()
    precioLineaIva=serializers.CharField()
    proveedor=serializers.CharField()

class InventarioWmsErpSerializer(serializers.Serializer):
    warehouse=serializers.CharField()
    wmsOnHand=serializers.CharField()
    erpOnHand=serializers.CharField()
    diferencia=serializers.CharField()
    diferenciaAbsoluta=serializers.CharField()
    wmsInTransit=serializers.CharField()
    numItemsWms=serializers.CharField()
    numItemsErp=serializers.CharField()
    numItemsDif=serializers.CharField()

class InventarioItemSerializer(serializers.Serializer):
    warehouse=serializers.CharField()
    wmsOnHand=serializers.CharField()
    erpOnHand=serializers.CharField()
    numItems=serializers.CharField()
    
class InventarioDetalleErpWmsSerializer(serializers.Serializer):
    fecha=serializers.CharField()
    item=serializers.CharField()
    warehouse=serializers.CharField()
    wmsComprometido=serializers.CharField()
    wmsTransito=serializers.CharField()
    wmsOnHand=serializers.CharField()
    erpOnHand=serializers.CharField()
    diferenciaOnHand=serializers.CharField()
    diferenciaOnHandAbsoluta=serializers.CharField()

class InventarioTopSerializer(serializers.Serializer):
    fecha=serializers.CharField()
    item=serializers.CharField()
    warehouse=serializers.CharField()
    wmsComprometido=serializers.CharField()
    wmsTransito=serializers.CharField()
    wmsOnHand=serializers.CharField()
    erpOnHand=serializers.CharField()
    difOnHand=serializers.CharField()
    difOnHandAbsolute=serializers.CharField()
    
class InventarioWmsSerializer(serializers.Serializer):
    warehouseCode=serializers.CharField()
    solicitado=serializers.CharField()
    onHand=serializers.CharField()
    comprometido=serializers.CharField()
    disponible=serializers.CharField()
    skuSolicitado=serializers.CharField()
    skuOnHand=serializers.CharField()
    skuComprometido=serializers.CharField()
    fechaActualizacion=serializers.CharField()

class PlaneacionSerializer(serializers.Serializer):
    subindice=serializers.CharField()
    unidad=serializers.CharField()
    orden=serializers.CharField()
    numTienda=serializers.CharField()
    tienda=serializers.CharField()
    ola=serializers.CharField()
    cantidadSolicitada=serializers.CharField()
    volumen=serializers.CharField()
    numeroContenedores=serializers.CharField()
    diaCarga=serializers.DateTimeField()
    diaArribo=serializers.DateTimeField()
    horaArribo=serializers.CharField()
    inicioDescarga=serializers.CharField()
    finDescarga=serializers.CharField()
    finProcesoAdmin=serializers.CharField()

class ReciboPendienteSerializer(serializers.Serializer):
    receiptId=serializers.CharField()
    item=serializers.CharField()
    itemDesc=serializers.CharField()
    totalQty=serializers.CharField()
    openQty=serializers.CharField()

class SplitSerializer(serializers.Serializer):
    pedido=serializers.CharField()
    contenedor=serializers.CharField()
    fechaCreacion=serializers.CharField()
    numeroPiezas=serializers.CharField()
    status=serializers.CharField()
    usuario=serializers.CharField()

class TiendaPendienteSerializer(serializers.Serializer):
    solicitudEstatus=serializers.CharField()
    carga=serializers.CharField()
    pedido=serializers.CharField()
    nombreAlmacen=serializers.CharField()
    fechaEmbarque=serializers.CharField()
    fechaPlaneada=serializers.CharField()
    transito=serializers.CharField()
    crossDock=serializers.CharField()
    fechaEntrega=serializers.CharField()
    
class PedidoTiendaSerializer(serializers.Serializer):
    anio=serializers.CharField()
    estatus=serializers.CharField()
    mes=serializers.CharField()
    fechaCedis=serializers.CharField()
    carga=serializers.CharField()
    tienda=serializers.CharField()
    pedido=serializers.CharField()
    tipoPedido=serializers.CharField()
    solicitudTraslado=serializers.CharField()
    contenedoresPendientes=serializers.CharField()
    contenedoresRecibidos=serializers.CharField()
    itemsPendientes=serializers.CharField()
    piezasPendientes=serializers.CharField()
    itemsRecibidos=serializers.CharField()
    piezasRecibidas=serializers.CharField()
    
class DetallePedidoTiendaSerializer(serializers.Serializer):
    estatusPedido=serializers.CharField()
    tienda=serializers.CharField()
    carga=serializers.CharField()
    pedido=serializers.CharField()
    contenedor=serializers.CharField()
    item=serializers.CharField()
    itemDescripcion=serializers.CharField()
    estatusContenedor=serializers.CharField()
    piezas=serializers.CharField()
    qc=serializers.CharField()
    usuarioPicking=serializers.CharField()
    fechaPicking=serializers.CharField()
    usuarioQc=serializers.CharField()
    fechaQc=serializers.CharField()

class DetallePedidoTiendaClSerializer(serializers.Serializer):
    estatusPedido=serializers.CharField()
    tienda=serializers.CharField()
    carga=serializers.CharField()
    pedido=serializers.CharField()
    contenedor=serializers.CharField()
    item=serializers.CharField()
    itemDescripcion=serializers.CharField()
    estatusContenedor=serializers.CharField()
    piezas=serializers.CharField()
    qc=serializers.CharField()
   
class SolicitudTrasladoSerializer(serializers.Serializer):
    documentoSolicitud=serializers.CharField()
    status=serializers.CharField()
    origenSolicitud=serializers.CharField()
    destinoSolicitud=serializers.CharField()
    articulosSolicitud=serializers.CharField()
    cantidadSolicitud=serializers.CharField()
    comentarios=serializers.CharField()
    fechaSolicitud=serializers.CharField()
    fechaVencimiento=serializers.CharField()
    origenTraslado=serializers.CharField()
    destinoTraslado=serializers.CharField()
    articulosTraslado=serializers.CharField()
    cantidadTraslado=serializers.CharField()

class TablaTiendaPendienteSerializer(serializers.Serializer):
    nombreAlmacen=serializers.CharField()
    fechaEntrega=serializers.CharField()
    numPedidos=serializers.CharField()
    piezas=serializers.CharField()
    contenedores=serializers.CharField()
    
class TiendaPendienteFechaSerializer(serializers.Serializer):
    carga=serializers.CharField()
    pedido=serializers.CharField()
    nombreAlmacen=serializers.CharField()
    fechaEmbarque=serializers.CharField()
    fechaEntrega=serializers.CharField()
    

class PedidoPorCerrarSerializer(serializers.Serializer):
    pedido=serializers.CharField()
    estatusPedido=serializers.CharField()
    tienda=serializers.CharField()
    carga=serializers.CharField()
    documento=serializers.CharField()
    estatusDocumento=serializers.CharField()

class InfoPedidoSinTrSerializer(serializers.Serializer):
    pedido=serializers.CharField()
    tienda=serializers.CharField()
    almacen=serializers.CharField()
    bnext=serializers.CharField()
    dockEntry=serializers.CharField()

class ReciboSapSerializer(serializers.Serializer):
    docSDT=serializers.CharField()
    closeQtySdt=serializers.CharField()
    closeQtyTr=serializers.CharField()
    openQtySdt=serializers.CharField()
    openQtyTr=serializers.CharField()
    totalQtySdt=serializers.CharField()
    totalQtyTr=serializers.CharField()
    wms=serializers.CharField()
    dif=serializers.CharField()
    closeDate=serializers.CharField()
    stsWms=serializers.CharField()
    val=serializers.CharField()

class PedidoSapSerializer(serializers.Serializer):
    docSDT=serializers.CharField()
    closeQtySdt=serializers.CharField()
    closeQtyTr=serializers.CharField()
    openQtySdt=serializers.CharField()
    openQtyTr=serializers.CharField()
    totalQtySdt=serializers.CharField()
    totalQtyTr=serializers.CharField()
    wmsClose=serializers.CharField()
    dif=serializers.CharField()
    shipDate=serializers.CharField()
    stsWms=serializers.CharField()
    val=serializers.CharField()
    openSap=serializers.CharField()

class CuadrajeSerializer(serializers.Serializer):
    recibosTotal=serializers.CharField()
    recibosOk=serializers.CharField()
    recibosQty=serializers.CharField()
    recibosCloseErp=serializers.CharField()
    recibosRev=serializers.CharField()
    recibosTotalNum=serializers.CharField()
    recibosOkNum=serializers.CharField()
    recibosQtyNum=serializers.CharField()
    recibosCloseErpNum=serializers.CharField()
    recibosRevNum=serializers.CharField()
    pedidosTotal=serializers.CharField()
    pedidosOk=serializers.CharField()
    pedidosQty=serializers.CharField()
    pedidosCloseErp=serializers.CharField()
    pedidosRev=serializers.CharField()
    pedidosTotalNum=serializers.CharField()
    pedidosOkNum=serializers.CharField()
    pedidosQtyNum=serializers.CharField()
    pedidosCloseErpNum=serializers.CharField()
    pedidosRevNum=serializers.CharField()
    pedidosAbiertos=serializers.CharField()
    pedidosAbiertosNum=serializers.CharField()


class PendienteSemanaSerializer(serializers.Serializer):
    fecha=serializers.CharField()
    shipDate=serializers.CharField()
    numeroRegistros=serializers.CharField()
    piezas=serializers.CharField()
    reetiquetado=serializers.CharField()

class ContenedorEpqSerializer(serializers.Serializer):
    contenedor=serializers.CharField()
    activityDateTime=serializers.CharField()
    pedido=serializers.CharField()
    ola=serializers.CharField()

class ReciboTiendaSerializer(serializers.Serializer):
    carga=serializers.CharField()
    pedido=serializers.CharField()
    llegadaTransportista=serializers.CharField()
    inicioScaneo=serializers.CharField()
    finScaneo=serializers.CharField()
    cierreCamion=serializers.CharField()

class DatoDashSerializer(serializers.Serializer):
    mes = serializers.CharField()
    gastoDistribucion = serializers.CharField()
    venta = serializers.CharField()
    contenedoresEmbarcados = serializers.CharField()
    pedidosEmbarcados = serializers.CharField()
    rentaMensual = serializers.CharField()
    inventarioMensual = serializers.CharField()
    dias = serializers.CharField()
    ontime = serializers.CharField()
    fillRate = serializers.CharField()
    leadTime = serializers.CharField()
    dato1RatioEntradas = serializers.CharField()
    dato2RatioEntradas = serializers.CharField()
    dato1RatioSalidas = serializers.CharField()
    dato2RatioSalidas = serializers.CharField()
    ticketsReportados = serializers.CharField()
    piezasReportadas = serializers.CharField()
    rotacionStocks = serializers.CharField()
    stockBajaRotacion = serializers.CharField()
    stockSinRotacion = serializers.CharField()

class PedidoPlaneacionSerializer(serializers.Serializer):
    docNum = serializers.CharField()
    itemCode = serializers.CharField()
    quantity = serializers.CharField()
    docDate = serializers.CharField()
    docDueDate = serializers.CharField()
    
class UbicacionVaciaSerializer(serializers.Serializer):
    ubicacion = serializers.CharField()
    status = serializers.CharField()
    active = serializers.CharField()
    
class EstatusContendorSerializer(serializers.Serializer):
    ola = serializers.CharField()
    semana = serializers.CharField()
    pedidos = serializers.CharField()
    contenedores = serializers.CharField()
    pickingPending = serializers.CharField()
    inPicking = serializers.CharField()
    packingPending = serializers.CharField()
    inPacking = serializers.CharField()
    stagingPending = serializers.CharField()
    loadingPending = serializers.CharField()
    shipConfirmPending = serializers.CharField()
    loadConfirmPending = serializers.CharField()
    closed = serializers.CharField()
    carton = serializers.CharField()
    bolsa = serializers.CharField()

class OlaPiezasContenedoresSerializer(serializers.Serializer):
    ola = serializers.CharField()
    numPiezas = serializers.CharField()
    numContenedores = serializers.CharField()

class DetalleContenedorOlaSerializer(serializers.Serializer):
    ola=serializers.CharField()
    pedido=serializers.CharField()
    contenedor=serializers.CharField()
    status=serializers.CharField()
    estatus=serializers.CharField()
    tipo=serializers.CharField()

class LineaOlaSerializer(serializers.Serializer):
    ola=serializers.CharField()
    pedido=serializers.CharField()
    item=serializers.CharField()
    descripcion=serializers.CharField()
    total=serializers.CharField()
    status=serializers.CharField()

class TransaccionesPickPutSerializer(serializers.Serializer):
    item=serializers.CharField()
    transaccion=serializers.CharField()
    nombreUsuario=serializers.CharField()
    refrencia=serializers.CharField()
    fecha=serializers.CharField()
    tipoTrabajo=serializers.CharField()
    usuario=serializers.CharField()
    ubicacion=serializers.CharField()
    cantidad=serializers.CharField()
    antesEnTransito=serializers.CharField()
    despuesEnTransito=serializers.CharField()
    antesAMano=serializers.CharField()
    despuesAMano=serializers.CharField()
    antesComprometido=serializers.CharField()
    despuesComprometido=serializers.CharField()
    antesSuspenso=serializers.CharField()
    despuesSuspenso=serializers.CharField()

class CantidadCajasSerializer(serializers.Serializer):
    item=serializers.CharField()
    cantidad=serializers.CharField()

class PorcentajeSkusPrioritariosSerializer(serializers.Serializer):
    container=serializers.CharField()
    porcentaje=serializers.CharField()

class WaveSerializer(serializers.Serializer):
    item=serializers.CharField()
    description=serializers.CharField()
    storageTemplate=serializers.CharField()
    shipmentId=serializers.CharField()
    launchNum=serializers.CharField()
    status=serializers.CharField()
    requestedQty=serializers.CharField()
    allocatedQty=serializers.CharField()
    av=serializers.CharField()
    oh=serializers.CharField()
    al=serializers.CharField()
    it=serializers.CharField()
    su=serializers.CharField()
    customer=serializers.CharField()
    itemCategory=serializers.CharField()
    creationDateTimeStamp=serializers.CharField()
    scheduledShipDate=serializers.CharField()
    division=serializers.CharField()
    conv=serializers.CharField()

class SplitClSerializer(serializers.Serializer):
    pedido=serializers.CharField()
    fechaCreacion=serializers.CharField()
    numeroContenedores=serializers.CharField()

class UnitMesureSerializer(serializers.Serializer):
    descripcion=serializers.CharField()
    piezasInner=serializers.CharField()
    piezasCaja=serializers.CharField()
    ubicaciones=serializers.CharField()
    coinciden=serializers.CharField()

class TiendaCorreoSerializer(serializers.Serializer):
    nombreTienda=serializers.CharField()

class ContenedorSalidaSerializer(serializers.Serializer):
    contenedorSalida=serializers.CharField()

class PesoContenedorSerializer(serializers.Serializer):
    peso=serializers.CharField()
    tolerancia=serializers.CharField()
    tipoContenedor=serializers.CharField()

class ListadoTiendasSerializer(serializers.Serializer):
    claveAlmacen=serializers.CharField()
    nombreAlmacen=serializers.CharField()

class AuditoriaTiendaSerializer(serializers.Serializer):
        tienda=serializers.CharField() #new
        pedido=serializers.CharField()
        carga=serializers.CharField()
        fechaRecepcion=serializers.CharField()
        totalContenedores=serializers.CharField()
        contenedoresAuditados=serializers.CharField()
        porcentaje=serializers.CharField()

class ConfirmacionesPendientes(serializers.Serializer):
        carga = serializers.CharField()
        pedido = serializers.CharField()
        numContenedores = serializers.CharField()
        fecha = serializers.CharField()

class ConsultaKardex(serializers.Serializer):
        item = serializers.CharField()
        location = serializers.CharField()
        date_stamp = serializers.CharField()
        user_tamp = serializers.CharField()
        quantity = serializers.CharField()
        before_on_hand_qty = serializers.CharField()
        after_on_hand_qty = serializers.CharField()
        before_in_transit_qty = serializers.CharField()
        after_in_transit_qty = serializers.CharField()
        before_alloc_qty = serializers.CharField()
        after_alloc_qty = serializers.CharField()

class DownloadKarde(serializers.Serializer):
    item = serializers.CharField() 
    transaction_type = serializers.CharField() 
    location = serializers.CharField() 
    container_id = serializers.CharField()
    reference_id = serializers.CharField() 
    reference_type = serializers.CharField() 
    work_type = serializers.CharField() 
    date_stamp = serializers.CharField()
    user_stamp = serializers.CharField() 
    quantity = serializers.CharField() 
    before_sts = serializers.CharField() 
    after_sts = serializers.CharField() 
    before_on_hand_qty = serializers.CharField() 
    after_on_hand_qty = serializers.CharField() 
    before_in_transit_qty = serializers.CharField() 
    after_in_transit_qty = serializers.CharField() 
    before_suspense_qty = serializers.CharField() 
    after_suspense_qty = serializers.CharField() 
    before_alloc_qty = serializers.CharField() 
    after_alloc_qty = serializers.CharField() 
    direction = serializers.CharField()

class PreciosSerializerCol(serializers.Serializer):
    itemCode=serializers.CharField()
    codigoBarras=serializers.CharField()
    categoria=serializers.CharField()
    subcategoria=serializers.CharField()
    clase=serializers.CharField()
    itemName=serializers.CharField()
    storageTemplate=serializers.CharField()
    stUsr=serializers.CharField()
    licencia=serializers.CharField()
    height=serializers.CharField()
    width=serializers.CharField()
    length=serializers.CharField()
    volume=serializers.CharField()
    weight=serializers.CharField()
    estandarSinIva=serializers.CharField()
    estandarConIva=serializers.CharField()
    adicionalSinIva=serializers.CharField()
    adicionalConIva=serializers.CharField()
    aeropuertoSinIva=serializers.CharField()
    aeropuertoConIva=serializers.CharField()
    proveedor=serializers.CharField()

class HuellaDigitalSerializer(serializers.Serializer):
    frozenFor = serializers.CharField()  
    itemCode = serializers.CharField()  
    itemName = serializers.CharField()  
    familia = serializers.CharField()  
    subFamilia = serializers.CharField()  
    subSubFamilia = serializers.CharField()  
    fragil = serializers.CharField()  
    movimiento = serializers.CharField()  
    altoValor = serializers.CharField()  
    bolsa = serializers.CharField()  
    flujo = serializers.CharField()  
    bcdCode = serializers.CharField()  
    u_sys_unid = serializers.CharField()
    u_sys_alto = serializers.CharField()
    u_sys_anch = serializers.CharField()
    u_sys_long = serializers.CharField()
    u_sys_volu = serializers.CharField()
    grupoUMLogistico = serializers.CharField()
    u_sys_peso = serializers.CharField()
    grupoUMCompas = serializers.CharField()

class AuditoriaTiendaClSerializerCl(serializers.Serializer): #Remove pedido and carga
        tienda=serializers.CharField() 
        fechaRecepcion=serializers.CharField()
        totalContenedores=serializers.CharField()
        contenedoresAuditados=serializers.CharField()
        porcentaje=serializers.CharField()
        
class AuditoriaOrderClSerializer(serializers.Serializer): 
        order=serializers.CharField() 
           
class subFamilyOrderClSerializer(serializers.Serializer): 
        tienda = serializers.CharField() 
        sub = serializers.CharField()
        totalContenedor = serializers.CharField() 
        auditado = serializers.CharField() 
        porcentaje = serializers.CharField()

class AssortedWorkUnitSerializer(serializers.Serializer):
        container_id = serializers.CharField()
        container_type = serializers.CharField()
        work_unit = serializers.CharField()
        from_loc = serializers.CharField()
        item = serializers.CharField()
        quantity = serializers.CharField()

class ItemLocationSerializer(serializers.Serializer):
        id = serializers.CharField()
        item = serializers.CharField()
        location = serializers.CharField()
        found = serializers.CharField()
        date = serializers.CharField()
        
class DescriptionTransactions(serializers.Serializer):
        identifier = serializers.CharField()
        description = serializers.CharField()

class Shorpacks(serializers.Serializer):
        pickWaveCode = serializers.CharField()
        clientCode = serializers.CharField()
        productCode = serializers.CharField()
        documentCode = serializers.CharField()
        request_qty = serializers.CharField()
        total_qty = serializers.CharField()
        rechazadas = serializers.CharField()
        pzasFaltantes = serializers.CharField()
        fecha = serializers.CharField()

class InventoryAvailable(serializers.Serializer):
        item=serializers.CharField()
        on_hand=serializers.CharField()
        in_transit=serializers.CharField()
        allocated=serializers.CharField()
        suspense=serializers.CharField()
        requested=serializers.CharField()
        quantity=serializers.CharField()
        real_available=serializers.CharField()
        date_time=serializers.CharField()

class OneValueSerializer(serializers.Serializer):
        value=serializers.CharField()    
    
class ReportBugsAvailableSerializer(serializers.Serializer):
        description=serializers.CharField()
        status=serializers.CharField()
        comments=serializers.CharField()
        
class ReportDeviceRSerializer(serializers.Serializer):
        total_devices=serializers.CharField()
        location=serializers.CharField()
        status=serializers.CharField()
        estimated_date=serializers.CharField()
        comments=serializers.CharField()

class ContainerPzaSerializer(serializers.Serializer):
        container = serializers.CharField()
        pza = serializers.CharField()
        tda = serializers.CharField()
        
class InventoryAvailableCL(serializers.Serializer):
        item=serializers.CharField()
        item_desc=serializers.CharField()
        available=serializers.CharField()
        on_hand=serializers.CharField()
        allocated=serializers.CharField()
        in_transit=serializers.CharField()
        suspense=serializers.CharField()
        family=serializers.CharField()
        subfamily=serializers.CharField()
        subsubfamily=serializers.CharField()
        date_time=serializers.CharField()