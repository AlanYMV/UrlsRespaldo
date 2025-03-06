from django.urls import path,re_path
from sevicios_app.api.views import *

urlpatterns = [
    path('pedidos/', pedidos_list, name='pedidos-list'),
    path('cargas/', cargas_list, name='cargas-list'),
    path('articles/', articulosList, name='articulosList'),
    path('pedido/<str:idPedido>/', pedido_detalle, name='pedido-detalle'),
    path('carga/<str:idCarga>/', carga_detalle, name='carga-detalle'),
    path('descarga_articulos/', descargaArticulos, name='descargaArticulos'),
    path('descarga_pedido/<str:idPedido>/', descargaPedido, name='descargaPedido'),
    path('descarga_carga/<str:idCarga>/', descargaCarga, name='descargaCarga'),
    path('storage_template/', storagesTemplates, name='storagesTemplates'),
    path('update_codigos_sat/', actualizarCodigosSat, name='actualizarCodigosSat'),
    path('precios/', precios, name='precios'),
    path('descarga_storage/', descargaStorages, name='descargaStorages'),
    path('descarga_precios/', descargaPrecios, name='descargaPrecios'),
    path('inventario_items/', inventarioItems, name='inventarioItems'),
    path('inventario_wmserp/', inventarioWmsErp, name='inventarioWmsErp'),
    path('inventario_wms/', inventarioWms, name='inventarioWms'),
    path('top_one_hundred/', topOneHundred, name='topOneHundred'),
    path('inventario_detalle_wmserp/<str:item>/', inventarioDetalleErpWms, name='inventarioDetalleErpWms'),
    path('planeacion/', getPlaneacion, name='getPlaneacion'),
    path('recibos_pendientes/', getReciboPendientes, name='getReciboPendientes'),
    path('descarga_recibos_pendientes/', decargaReciboPendientes, name='decargaReciboPendientes'),
    path('split/<str:fecha>/', getSplits, name='getSplits'),
    path('update_tablas_tienda/', executeActualizarTablasTiendas, name='executeActualizarTablasTiendas'),
    path('envio_correos/<str:tienda>', executeEnvioCorreos, name='executeEnvioCorreos'),
    path('tienda_pendiente/', getTiendasPendientes, name='getTiendasPendientes'),
    path('tienda_pendiente_all/', getTiendasPendientesAll, name='getTiendasPendientesAll'),
    path('tienda_pendiente_correo/', getTiendasPendientesCorreo, name='getTiendasPendientesCorreo'),
    path('pedido_tienda/<str:tienda>/<str:carga>/<str:pedido>/', getPedidoTienda, name='getPedidoTienda'),
    path('detalle_pedido_tienda/<str:tienda>/<str:carga>/<str:pedido>/<str:contenedor>/<str:articulo>/<str:estatusContenedor>/', getDetallePedidoTienda, name='getDetallePedidoTienda'),
    path('solicitud_traslado/<str:documento>/<str:origenSolicitud>/<str:destinoSolicitud>/<str:origenTraslado>/<str:destinoTraslado>/', getSolicitudTraslado, name='getSolicitudTraslado'),
    path('tabla_tiendas/<str:fecha>/', getTablaTiendasPendientes, name='getTablaTiendasPendientes'),
    path('tiendas_fecha/', getTiendasPendientesFecha, name='getTiendasPendientesFecha'),
    path('insert_fecha_trafico/<str:fecha>/<str:carga>/<str:pedido>/', insertFechaTrafico, name='insertFechaTrafico'),
    path('pedido_cerrar/', getPedidosPorCerrar, name='pedidoCerrar'),
    path('cerrar_pedidos/', cerrarPedidos, name='cerrarPedidos'),
    path('pedido_sin_tr/', getPedidosSinTr, name='getPedidosSinTr'),
    path('diferencias_file/', getDiferenciasFile, name='getDiferenciasFile'),
    path('recibo_sap/<str:recibos>/', getReciboSap, name='getReciboSap'),
    path('recibo_sap_valor/<str:valor>/', getReciboSapByValor, name='getReciboSapByValor'),
    path('pedido_sap/<str:pedidos>/', getPedidoSap, name='getPedidoSap'),
    path('pedido_sap_valor/<str:valor>/', getPedidoSapByValor, name='getPedidoSapByValor'),
    path('pedido_sap_valor_file/<str:valor>/', getPedidoSapByValorFile, name='getPedidoSapByValorFile'),
    path('cuadraje/', getCuadraje, name='getCuadraje'),
    path('pendiente_semana/', getPendienteSemana, name='getPendienteSemana'),
    path('execute_cuadraje/', executeCuadraje, name='executeCuadraje'),
    path('insert_sapreceipt/<str:idReceipt>/', insertSapReceiptVal, name='insertSapReceiptVal'),
    path('insert_sapshipment/<str:idShipment>/', insertSapShipmentVal, name='insertSapShipmentVal'),
    path('contenedor_epq_day/', getContenedoresEpqDay, name='getContenedoresEpqDay'),
    path('contenedor_epq/', getContenedoresEpqAll, name='getContenedoresEpqAll'),
    path('execute_inventario/', executeInvetarioCedis, name='executeInvetarioCedis'),
    path('detalle_pedido_tienda_file/<str:tienda>/<str:carga>/<str:pedido>/<str:contenedor>/<str:articulo>/<str:estatusContenedor>/', getDetallePedidoTiendaFile, name='getDetallePedidoTiendaFile'),
    path('update_diferencias/', executeUpdateDiferencias, name='executeUpdateDiferencias'),
    path('cuadraje_file/', getCuadrajeFile, name='getCuadrajeFile'),
    path('inventario_file/', inventarioFile, name='inventarioFile'),
    path('tabla_tiendas_file/<str:fecha>/', getTablaTiendasPendientesFile, name='getTablaTiendasPendientesFile'),
    path('top_one_hundred_file/', topOneHundredFile, name='topOneHundredFile'),
    path('recibo_tienda/<str:fechaInicio>/<str:fechaFin>/', getRecibosTienda, name='getRecibosTienda'),
    path('recibo_tienda_file/<str:fechaInicio>/<str:fechaFin>/', getRecibosTiendaFile, name='getRecibosTiendaFile'),
    path('datos_dash/', getDatosDash, name='getDatosDash'),
    path('update_vacaciones/<str:folio>/<str:estatus>/', updateSolicitudVacaciones, name='updateSolicitudVacaciones'),
    path('update_permiso/<str:folio>/<str:estatus>/', updateSolicitudPermiso, name='updateSolicitudPermiso'),
    path('pedidos_planeacion_file/<str:fechaInicio>/<str:fechaFin>/', getPedidosPlaneacion, name='getPedidosPlaneacion'),
    path('pedidos_planeacion/<str:fechaInicio>/<str:fechaFin>/', getPedidosPlaneacionTop, name='getPedidosPlaneacionTop'),
    path('ubicaciones_vacias/', getUbicacionesVaciasTop, name='getUbicacionesVaciasTop'),
    path('ubicaciones_vacias_file/', getUbicacionesVaciasFile, name='getUbicacionesVaciasFile'),
    path('estatus_ola/', getEstatusOla, name='getEstatusOla'),
    path('estatus_ola_file/', getEstatusOlaFile, name='getEstatusOlaFile'),
    path('ola_pza_cont/', getOlaPiezasContenedores, name='getOlaPiezasContenedores'),
    path('ola_pza_cont_file/', getOlaPiezasContenedoresFile, name='getOlaPiezasContenedoresFile'),
    path('detalle_ola/', getDetalleEstatusContenedores, name='getDetalleEstatusContenedores'),
    path('lineas_ola/<str:ola>', getLineasOla, name='getLineasOla'),
    path('lineas_ola_file/<str:ola>', getLineasOlaFile, name='getLineasOlaFile'),
    path('tareas_abiertas_file/', getTareasReaSurtAbiertasFile, name='getTareasReaSurtAbiertasFile'),
    path('contenedores_file/', getContenedoresFile, name='getContenedoresFile'),
    path('inventario_almacenaje_file/', getInventarioAlmacenajeFile, name='getInventarioAlmacenajeFile'),
    #New =>
    path('unlock&shorpick/', getUnlockANDShorpick, name='getUnlockANDShorpick'),
    path('container_qc/', getdownloadContainerQc, name='getdownloadContainerQc'),
    path('item_container_qc/', getdownloadItemContainerQc, name='getdownloadItemContainerQc'),
    re_path(r'^consult_kardex/?(?P<item>[^/]*)/?(?P<container_id>[^/]*)/?(?P<location>[^/]*)/?(?P<user_stamp>[^/]*)/?(?P<work_type>[^/]*)/?(?P<dateStar>[^/]*)/?(?P<dateEnd>[^/]*)/?(?P<transaction>[^/]*)/?$', getConsultaKardex, name='getConsultaKardex'),
    re_path(r'^download_kardex/?(?P<item>[^/]*)/?(?P<container_id>[^/]*)/?(?P<location>[^/]*)/?(?P<user_stamp>[^/]*)/?(?P<work_type>[^/]*)/?(?P<dateStar>[^/]*)/?(?P<dateEnd>[^/]*)/?(?P<transaction>[^/]*)/?$', getKardexDownload, name='getKardexDownload'),
    path('huella_digital/', getHuellaDigital, name='getHuellaDigital'),
    path('download_huella_digital/', downloadHuellaDigital, name='downloadHuellaDigital'),
    #^
    path('cantidad_cajas/<str:item>', getCantidadCajas, name='getCantidadCajas'),
    path('prioritarios/<str:container>', getPorcentajeSKUsPrioritarios, name='getPorcentajeSKUsPrioritarios'),
    path('inventario_tiendas/', getInventarioTienda, name='getInventarioTienda'),
    path('wave_analysis/<str:wave>', getWaveAnalysis, name='getWaveAnalysis'),
    path('wave_analysis_file/<str:wave>', getWaveAnalysisFile, name='getWaveAnalysisFile'),
    path('tiendas_correo/', getTiendasCorreo, name='getTiendasCorreo'),
    path('recibos_detalle_file/', getDiferenciasDetalleRecibo, name='getDiferenciasDetalleRecibo'),
    path('peso_caja/<str:contenedor>', getPesoCaja, name='getPesoCaja'),
    path('consulta_test/<str:fechaInicio>', getconsultaTest, name='getconsultaTest'),
    path('ventas_fecha/<str:fechaInicio>/<str:fechaFin>', getVentasXFecha, name='getVentasXFecha'),
    #################################################Chile####################################################
    #################################################Inventario#################################################
    path('inventario_cl_items/', inventarioClItems, name='inventarioClItems'),
    path('inventario_cl_wmserp/', inventarioClWmsErp, name='inventarioClWmsErp'),
    path('inventario_cl_wms/', inventarioClWms, name='inventarioClWms'),
    path('top_one_hundred_cl/', topOneHundredCl, name='topOneHundredCl'),
    path('inventario_cl_detalle_wmserp/<str:item>/', inventarioClDetalleErpWms, name='inventarioClDetalleErpWms'),
    path('inventario_cl_file/', inventarioClFile, name='inventarioClFile'),
    path('top_one_hundred_cl_file/', topOneHundredClFile, name='topOneHundredClFile'),
    path('execute_inventario_cl/', executeInvetarioClCedis, name='executeInvetarioClCedis'),
    #new >
    path('container_qc_cl/', getdownloadContainerQcCl, name='getdownloadContainerQcCl'),
    path('item_container_qc_cl/', getdownloadItemContainerQcCl, name='getdownloadItemContainerQcCl'),
    #################################################Cuadraje#################################################
    path('cuadraje_cl/', getCuadrajeCl, name='getCuadrajeCl'),
    path('pendiente_semana_cl/', getPendienteSemanaCl, name='getPendienteSemanaCl'),
    path('execute_cuadraje_cl/', executeCuadrajeCl, name='executeCuadrajeCl'),
    path('insert_sapreceipt_cl/<str:idReceipt>/', insertSapReceiptValCl, name='insertSapReceiptValCl'),
    path('insert_sapshipment_cl/<str:idShipment>/', insertSapShipmentValCl, name='insertSapShipmentValCl'),
    path('update_diferencias_cl/', executeUpdateDiferenciasCl, name='executeUpdateDiferenciasCl'),
    path('recibo_sap_cl/<str:recibos>/', getReciboSapCl, name='getReciboSapCl'),
    path('recibo_sap_valor_cl/<str:valor>/', getReciboSapByValorCl, name='getReciboSapByValorCl'),
    path('pedido_sap_cl/<str:pedidos>/', getPedidoSapCl, name='getPedidoSapCl'),
    path('pedido_sap_valor_cl/<str:valor>/', getPedidoSapByValorCl, name='getPedidoSapByValorCl'),
    path('cuadraje_file_cl/', getCuadrajeFileCl, name='getCuadrajeFileCl'),
    #################################################Reporte Tiendas#################################################
    path('tienda_pendiente_cl/', getTiendasPendientesCl, name='getTiendasPendientesCl'),
    path('tiendas_fecha_cl/', getTiendasPendientesFechaCl, name='getTiendasPendientesFechaCl'),
    path('pedido_cerrar_cl/', getPedidosPorCerrarCl, name='pedidoCerrarCl'),
    path('pedido_sin_tr_cl/', getPedidosSinTrCl, name='getPedidosSinTrCl'),
    path('update_tablas_tienda_cl/', executeActualizarTablasTiendasCl, name='executeActualizarTablasTiendasCl'),
    path('cerrar_pedidos_cl/', cerrarPedidosCl, name='cerrarPedidosCl'),
    path('insert_fecha_trafico_cl/<str:fecha>/<str:carga>/<str:pedido>/', insertFechaTraficoCl, name='insertFechaTraficoCl'),
    path('pedido_tienda_cl/<str:tienda>/<str:carga>/<str:pedido>/', getPedidoTiendaCl, name='getPedidoTiendaCl'),
    path('detalle_pedido_tienda_cl/<str:tienda>/<str:carga>/<str:pedido>/<str:contenedor>/<str:articulo>/<str:estatusContenedor>/', getDetallePedidoTiendaCl, name='getDetallePedidoTiendaCl'),
    path('detalle_pedido_tienda_file_cl/<str:tienda>/<str:carga>/<str:pedido>/<str:contenedor>/<str:articulo>/<str:estatusContenedor>/', getDetallePedidoTiendaFileCl, name='getDetallePedidoTiendaFileCl'),
    path('solicitud_traslado_cl/<str:documento>/<str:origenSolicitud>/<str:destinoSolicitud>/<str:origenTraslado>/<str:destinoTraslado>/', getSolicitudTrasladoCl, name='getSolicitudTrasladoCl'),
    path('tabla_tiendas_cl/<str:fecha>/', getTablaTiendasPendientesCl, name='getTablaTiendasPendientesCl'),
    path('tienda_pendiente_all_cl/', getTiendasPendientesAllCl, name='getTiendasPendientesAllCl'),
    path('tienda_pendiente_correo_cl/', getTiendasPendientesCorreoCl, name='getTiendasPendientesCorreoCl'),
    path('tabla_tiendas_file_cl/<str:fecha>/', getTablaTiendasPendientesFileCl, name='getTablaTiendasPendientesFileCl'),
    path('diferencias_file_cl/', getDiferenciasFileCl, name='getDiferenciasFileCl'),
  
    #################################################STORAGE TEMPLATE#################################################
    path('storage_templateCL/', storagesTemplatesCL, name='storagesTemplatesCL'),
    path('descarga_storageCL/', descargaStoragesCL, name='descargaStoragesCL'),
    path('transacciones_pick_putCL/', getTransaccionesCL, name='getTransaccionesCL'),
    path('transacciones_pick_put_fileCL/', getTransaccionesFileCL, name='getTransaccionesFileCL'),
    
    #################################################WAVE ANALYSIS#################################################
    path('lineas_ola_cl/<str:ola>', getLineasOlaCl, name='getLineasOlaCl'),
    path('lineas_ola_file_cl/<str:ola>', getLineasOlaFileCl, name='getLineasOlaFileCl'),
    path('ola_pza_cont_cl/', getOlaPiezasContenedoresCl, name='getOlaPiezasContenedoresCl'),
    path('ola_pza_cont_file_cl/', getOlaPiezasContenedoresFileCl, name='getOlaPiezasContenedoresFileCl'),
    path('wave_analysis_cl/<str:wave>', getWaveAnalysisCl, name='getWaveAnalysisCl'),
    path('wave_analysis_file_cl/<str:wave>', getWaveAnalysisFileCl, name='getWaveAnalysisFileCl'),

    #################################################OTROS#################################################
    path('splits_cl/', getSplitsCl, name='getSplitsCl'),
    path('splits_file_cl/', getSplitsFileCl, name='getSplitsFileCl'),
    path('unit_mesure_location_cl/<str:location>/<str:item>', getUnitMesureLocation, name='getUnitMesureLocation'),
    path('precios_cl/', preciosCl, name='preciosCl'),
    path('descarga_precios_cl/', descargaPreciosCl, name='descargaPreciosCl'),
    path('tareas_abiertas_file_cl/', getTareasReaSurtAbiertasFileCl, name='getTareasReaSurtAbiertasFileCl'),
    path('contenedores_file_cl/', getContenedoresFileCl, name='getContenedoresFileCl'),
    path('recibos_detalle_file_cl/', getDiferenciasDetalleReciboCl, name='getDiferenciasDetalleReciboCl'),
    path('recibos_pendientes_cl/', getReciboPendientesCl, name='getReciboPendientesCl'),
    path('descarga_recibos_pendientes_cl/', decargaReciboPendientesCl, name='decargaReciboPendientesCl'),
    path('ubicaciones_vacias_cl/', getUbicacionesVaciasTopCl, name='getUbicacionesVaciasTopCl'),
    path('ubicaciones_vacias_file_cl/', getUbicacionesVaciasFileCl, name='getUbicacionesVaciasFileCl'),
    path('estatus_ola_cl/', getEstatusOlaCl, name='getEstatusOlaCl'),
    path('estatus_ola_file_cl/', getEstatusOlaFileCl, name='getEstatusOlaFileCl'),
    path('detalle_ola_cl/', getDetalleEstatusContenedoresCl, name='getDetalleEstatusContenedoresCl'),
    path('inventario_almacenaje_file_cl/', getInventarioAlmacenajeFileCl, name='getInventarioAlmacenajeFileCl'),
    path('recibo_tienda_cl/<str:fechaInicio>/<str:fechaFin>/', getRecibosTiendaCl, name='getRecibosTiendaCl'),
    path('recibo_tienda_file_cl/<str:fechaInicio>/<str:fechaFin>/', getRecibosTiendaFileCl, name='getRecibosTiendaFileCl'),
    path('listado_tiendas_cl/', getTiendasCl, name='getTiendasCl'),  
    path('auditorias_tienda_cl/<str:tienda>/<str:fechaInicio>/<str:fechaFin>/', getAuditoriasTiendaCl, name='getAuditoriasTiendaCl'),
    path('auditoria_tienda_descarga_cl/<str:tienda>/<str:fechaInicio>/<str:fechaFin>/', getdownloadAuditoriaTiendaCl, name='getdownloadAuditoriaTiendaCl'), #new
    path('contenedores_salida/<str:contenedorSalida>', getContenedoresSalida, name='getContenedoresSalida'),
    path('contenedor_detalle_file/<str:idContenedorSalida>', getContedorDetalleFile, name='getContedorDetalleFile'),
    path('prioritarios_cl/<str:container>', getPorcentajeSKUsPrioritariosCl, name='getPorcentajeSKUsPrioritariosCl'),
    path('confirmacion_pendientes_cl/', getConfirmacionesPendientesCl, name='getConfirmacionesPendientesCl'),
    path('auditoria_orders_cl/<str:tienda>/<str:fecha>/', OrderAudiCl, name='OrderAudiCl'), 
    path('sub_family_order_cl/<str:tienda>/<str:fecha>/', subFamilyOrdersCl, name='subFamilyOrdersCl'),

    #################################################Colombia####################################################
    #################################################Inventario#################################################
    path('inventario_col_items/', inventarioItemsCol, name='inventarioItemsCol'), # /
    path('inventario_col_wmserp/', inventarioWmsErpCol, name='inventarioWmsErpCol'), # /
    path('inventario_col_wms/', inventarioWmsCol, name='inventarioWmsCol'), # /
    path('top_one_hundred_col/', topOneHundredCol, name='topOneHundredCol'), # /
    path('inventario_col_detalle_wmserp/<str:item>/', inventarioDetalleErpWmsCol, name='inventarioDetalleErpWmsCol'), # /
    path('inventario_col_file/', inventarioFileCol, name='inventarioFile'), # /
    path('top_one_hundred_col_file/', topOneHundredFileCol, name='topOneHundredFileCol'), # /
    path('execute_inventario_col/', executeInvetarioCedisCol, name='executeInvetarioCedisCol'), # /
    #################################################Cuadraje#################################################
    path('cuadraje_col/', getCuadrajeCol, name='getCuadrajeCol'), # /
    path('pendiente_semana_col/', getPendienteSemanaCol, name='getPendienteSemanaCol'), # /
    path('execute_cuadraje_col/', executeCuadrajeCol, name='executeCuadrajeCol'), # /
    path('insert_sapreceipt_col/<str:idReceipt>/', insertSapReceiptValCol, name='insertSapReceiptValCol'), # /
    path('insert_sapshipment_col/<str:idShipment>/', insertSapShipmentValCol, name='insertSapShipmentValCol'), # /
    path('update_diferencias_col/', executeUpdateDiferenciasCol, name='executeUpdateDiferenciasCol'), # /
    path('recibo_sap_col/<str:recibos>/', getReciboSapCol, name='getReciboSapCol'), # /
    path('recibo_sap_valor_col/<str:valor>/', getReciboSapByValorCol, name='getReciboSapByValorCol'), # /
    path('pedido_sap_col/<str:pedidos>/', getPedidoSapCol, name='getPedidoSapCol'), # /
    path('pedido_sap_valor_col/<str:valor>/', getPedidoSapByValorCol, name='getPedidoSapByValorCol'), # /
    path('cuadraje_file_col/', getCuadrajeFileCol, name='getCuadrajeFileCol'), # /

    #################################################STORAGE TEMPLATE#################################################
    path('storage_templateCol/', storagesTemplatesCOL, name='storagesTemplatesCOL'), #Modify 
    path('descarga_storageCol/', descargaStoragesCOL, name='descargaStoragesCOL'), #Modify
    path('transacciones_pick_putCL/', getTransaccionesCL, name='getTransaccionesCL'),
    path('transacciones_pick_put_fileCL/', getTransaccionesFileCL, name='getTransaccionesFileCL'),
    
    #################################################WAVE ANALYSIS#################################################
    path('lineas_ola_cl/<str:ola>', getLineasOlaCl, name='getLineasOlaCl'),
    path('lineas_ola_file_cl/<str:ola>', getLineasOlaFileCl, name='getLineasOlaFileCl'),
    path('ola_pza_cont_cl/', getOlaPiezasContenedoresCl, name='getOlaPiezasContenedoresCl'),
    path('ola_pza_cont_file_cl/', getOlaPiezasContenedoresFileCl, name='getOlaPiezasContenedoresFileCl'),
    path('wave_analysis_col/<str:wave>', getWaveAnalysisCol, name='getWaveAnalysisCol'),
    path('wave_analysis_file_col/<str:wave>', getWaveAnalysisFileCol, name='getWaveAnalysisFileCol'),

    #################################################OTROS#################################################
    path('splits_col/', getSplitsCol, name='getSplitsCol'),
    path('splits_file_col/', getSplitsFileCol, name='getSplitsFileCol'),
    #path('unit_mesure_location_col/<str:location>/<str:item>', getUnitMesureLocation, name='getUnitMesureLocation'),
    path('precios_col/', getPreciosCol, name='getPreciosCol'), 
    path('descarga_precios_col/', downloadPreciosCol, name='downloadPreciosCol'), 
    path('tareas_abiertas_file_col/', getTareasReaSurtAbiertasFileCol, name='getTareasReaSurtAbiertasFileCol'),
    path('contenedores_file_col/', getContenedoresFileCol, name='getContenedoresFileCol'),
    path('recibos_detalle_file_cl/', getDiferenciasDetalleReciboCl, name='getDiferenciasDetalleReciboCl'),
    path('recibos_pendientes_col/', getReciboPendientesCol, name='getReciboPendientesCol'),
    path('descarga_recibos_pendientes_col/', decargaReciboPendientesCol, name='decargaReciboPendientesCol'),
    path('ubicaciones_vacias_cl/', getUbicacionesVaciasTopCl, name='getUbicacionesVaciasTopCl'),
    path('ubicaciones_vacias_file_cl/', getUbicacionesVaciasFileCl, name='getUbicacionesVaciasFileCl'),
    path('estatus_ola_cl/', getEstatusOlaCl, name='getEstatusOlaCl'),
    path('estatus_ola_file_cl/', getEstatusOlaFileCl, name='getEstatusOlaFileCl'),
    path('detalle_ola_cl/', getDetalleEstatusContenedoresCl, name='getDetalleEstatusContenedoresCl'),
    path('inventario_almacenaje_file_cl/', getInventarioAlmacenajeFileCl, name='getInventarioAlmacenajeFileCl'),
    path('confirmacion_pendientes/', getConfirmacionesPendientes, name='getConfirmacionesPendientes'),
    # path('registrar_contenedores_mx/', registrar_contenedores, name='registrar_contenedores'), #Alta contenedor
    # path('eliminar_contenedores_mx/', eliminar_contenedores, name='eliminar_contenedores'), #Baja contenedor
    path('actualizar_dimensiones_mx/', actualizar_unidad_de_medida, name='actualizar_unidad_de_medida'), #Actualizacion Dimensiones Item
    path('unidad_trabajo_surtido_reserva/<str:launch_num>', getWorkUnitAssorted, name='getWorkUnitAssorted'),
    path('unidad_trabajo_surtido_reserva_descarga/<str:launch_num>', getWorkUnitAssortedFile, name='getWorkUnitAssortedFile'),
    path('agregar_articulo_ubicacion/<str:item>/<str:location>', getInsertItemUbicacion, name='getInsertItemUbicacion'),
    path('articulos_ubicacion/', getItemLocation, name='getItemLocation'),
    path('descargar_articulos_ubicacion/<str:date>', getDownloadItemLocation, name='getDownloadItemLocation'),
    # path('registrar_contenedores_cl/', registrar_contenedores_cl, name='registrar_contenedores_cl'),
    # path('eliminar_contenedores_cl/', eliminar_contenedores_cl, name='eliminar_contenedores_cl'), 
    path('actualizar_dimensiones_cl/', actualizar_unidad_de_medida_cl, name='actualizar_unidad_de_medida_cl'), 
    path('precio_promociones/', registrar_promociones, name='registrar_promociones'), #Update Promotions 
    path('descripcion_transacciones/', getDescripcionTransacciones, name='getDescripcionTransacciones'), 
    path('shorpack/<str:date>', getShorpacks, name='getShorpacks'),
    path('descargar_shorpack/<str:date>', getDownloadShorpack, name='getDownloadShorpack'),
    path('inventario_disponible/<str:date>', getInventoryAvailable, name='getInventoryAvailable'),
    path('descargar_inventario_disponible/<str:date>', getDownloadInventoryAvailable, name='getDownloadInventoryAvailable'),
    path('actualizar_dimensiones_col/', actualizar_unidad_de_medida_col, name='actualizar_unidad_de_medida_col'), 
    path('sku_prioritario/', getDownloadSkuPriority, name='getDownloadSkuPriority'), 
    path('tipo_localizacion/', locationType, name='locationType'), 
    path('zona_localizacion/', locationZone, name='locationZone'),  
    path('inventario_disponible_parametros/<str:typeP>/<str:zone>/', getDownloadInventoryAParams, name='getDownloadInventoryAParams'),
]