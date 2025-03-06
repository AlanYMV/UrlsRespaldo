'''from django.shortcuts import render
from django.http import JsonResponse
import json
from sevicios_app.clases.Pedido import Pedido

# Create your views here.

def pedidos_list(request):
    pedido = Pedido()
    pedido.setName('123456')
    data={
        'pedido':'pedido'
    }
    print(json.dumps(data))
    return JsonResponse(data)'''
    
