#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 26 17:28:31 2017

@author: nacho
"""

import scrapy
import pandas as pd

#def corregir_nombres(lista):
#        l2= [ l.lstrip() for l in lista] # eliminar espacio izquierda
#        l2= [ l.rstrip() for l in lista] # eliminar espacio derecha
#        l2= [ l.replace('"','') for l in lista] # eliminar caracter "
#        l2= [ l.replace(' ','+') for l in lista] # cambiar espacios por caracter +
#        return l2

def corregir_nombres(nombre):
        nombre=nombre.lstrip() # eliminar espacio izquierda
        nombre=nombre.rstrip() # eliminar espacio derecha
        nombre=nombre.replace('"','') # eliminar caracter "
        nombre=nombre.replace(' ','+') # cambiar espacios por caracter +
        return nombre  

def crear_url(nombres,ciudad):
    url=['http://es.qdq.com/nombre/' + n +'/' + c +'/tipo-0/calle-/num-/pag-/rows-/' for (n,c) in zip(nombres,ciudad)]
    return url[0:2]

class NegociosSpider(scrapy.Spider):
    
    name = 'qdq'
    file_name =''
    data = pd.DataFrame([],columns=['i','url','name','address','postalcode','localitation','region'])
    
    def start_requests(self):

        self.file_name=input('Fichero csv de datos sin geolocalizar: ')

        # Leer fichero con las empresas sin direcciones
        datos=pd.read_table(self.file_name,header=0,sep=',')
        # quedarnos solo con los registros sin datos de dirección
        datos=datos[datos.direccion.isnull()].copy()
        
        # Crear url con los nombres y la ciudad de búsqueda
        nombres=datos['nombre'].apply(corregir_nombres)
        busqueda=datos['busqueda'].apply(corregir_nombres)
    
        # Lista de urls
        urls=crear_url(nombres.values,busqueda.values)
        self.data=self.data.append(pd.DataFrame(list(zip(urls,datos.i.values)),columns=['url','i']))
               
        for url in urls:
            #print('url ',url)
            yield scrapy.Request(url=url, callback=self.parse)
       
    def parse(self, response):
        SET_SELECTOR = '.resultado'
        for negocio in response.css(SET_SELECTOR):

            NAME_SELECTOR = 'h2[itemprop="name"] em ::text'
            ADDRESS_SELECTOR= 'span[itemprop="street-address"] ::text'
            POSTALCODE_SELECTOR= 'span[itemprop="postal-code"] ::text'
            LOCALITY_SELECTOR= 'span[itemprop="locality"] ::text'
#            REGION_SELECTOR='span[itemprop="region"] ::text'
#            yield {
#                'search': response.url,
#                'name': ' '.join(negocio.css(NAME_SELECTOR).extract()),
#                'address': negocio.css(ADDRESS_SELECTOR).extract_first(), 
#                'postalcode': negocio.css(POSTALCODE_SELECTOR).extract_first(), 
#                'locality': negocio.css(LOCALITY_SELECTOR).extract_first(), 
#                'region': negocio.css(REGION_SELECTOR).extract_first(),                       
#            }

            i=self.data[self.data.url==response.url].index[0] #['url'].values[0] # index 
            name=' '.join(negocio.css(NAME_SELECTOR).extract())
            address= negocio.css(ADDRESS_SELECTOR).extract_first() 
            postalcode= negocio.css(POSTALCODE_SELECTOR).extract_first() 
            locality= negocio.css(LOCALITY_SELECTOR).extract_first() 
            #region= negocio.css(REGION_SELECTOR).extract_first()                       

            d=pd.DataFrame([[int(self.data.ix[i]['i']),response.url,name,address,postalcode,locality]],
                  columns=['i','url','name','address','postalcode','localitation'])
            with open(self.file_name.replace(".csv","_completado.csv"), 'a') as f:
                d.to_csv(f,header=False)           
           

#        NEXT_PAGE_SELECTOR = '.next a ::attr(href)'
#        next_page = response.css(NEXT_PAGE_SELECTOR).extract_first()
#        if next_page:
#            yield scrapy.Request(
#                response.urljoin(next_page),
#                callback=self.parse
#            )
