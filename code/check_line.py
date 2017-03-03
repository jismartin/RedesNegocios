# -*- coding: utf-8 -*-
"""
@author: nacho
"""

import pandas as pd
import re
import unicodedata

def elimina_tildes(s):
   return ''.join((c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn'))

def normaliza_texto1(s):
    #s=s.decode('utf8')
    s=s.strip()
    s=elimina_tildes(s)
    s=s.lower()
    return re.sub('[\s+]','',s)
    
def normaliza_texto2(s):
    s=elimina_tildes(s)
    s=re.sub(';','',s)
    s=re.sub('&nbsp','',s)
    return s.lower()

    
ruta='./test/Burgos_45331.csv'
ruta='./RESTO/DESCARGAS CATCHAPAGE BARCELONA/BARCELONA7211b.csv'
ruta='Barcelona_bares_08031.csv'

# Número de tipo de negocio
numeros = [ int(s) for s in re.findall(r'\d+',ruta)]
clase = numeros[0]

# Subclase de negocio
subclase=re.split('\d+',ruta)[1]
# Eliminar caracteres
subclase=re.sub('_','',subclase)
subclase=re.sub('.csv','',subclase)
if len(subclase)>0:
    subclase=normaliza_texto1(subclase)

# Leer datos de capturados
datos = pd.read_table(ruta,sep=',',encoding='utf-8',error_bad_lines=True,warn_bad_lines=True,header=0)
datos=datos[datos.columns[:11]]
datos.columns= ["nombre","categoria_web","email","telefono","direccion","cp","ciudad","web","pahttp","longitud","latitud"]
datos.fillna(0,inplace=True)

# eliminar columnas no necesarias
datos.drop('email',axis=1,inplace=True)
datos.drop('telefono',axis=1,inplace=True)
datos.drop('web',axis=1,inplace=True)
datos.drop('pahttp',axis=1,inplace=True)
#datos.drop('x',axis=1,inplace=True)
              
# convertir a número              
datos.longitud=pd.to_numeric(datos.longitud,errors='coerce')
datos.latitud=pd.to_numeric(datos.latitud,errors='coerce')
datos.cp=pd.to_numeric(datos.cp,errors='coerce')        
datos.longitud.fillna(0,inplace=True)
datos.latitud.fillna(0,inplace=True)

# normalizar texto            
datos.nombre = [ normaliza_texto2(s) for s in datos.nombre ]
datos.direccion = [ normaliza_texto2(s) for s in datos.direccion ]
datos.ciudad = [ normaliza_texto2(s) for s in datos.ciudad ]
datos.categoria_web = [ normaliza_texto2(s) for s in datos.categoria_web ]

# errores
n_sin_posicion = len(datos[(datos.longitud==0) & (datos.latitud==0)])
