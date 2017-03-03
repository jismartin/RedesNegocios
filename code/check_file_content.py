# -*- coding: utf-8 -*-
"""
@author: nacho
"""

import pandas as pd
import numpy as np
import re

#ficheros='cyl'
#ficheros='barcelona'
#ficheros='barcelona_bares_7224'
#ficheros='madrid'
ficheros='prueba'

ficheros_lista = pd.read_csv(ficheros + '.txt', names=['ruta'])

errores_lineas=[]
errores_posicion=[]
nl=0
for ruta in ficheros_lista.ruta:    
    print('revisando: ' + ruta)
    f=open(ruta,'r')
    lines=f.readlines()
    for i in range(len(lines)):
        nl=nl+1;        
        ln=lines[i]
        if len(re.split('","',ln)) > 12:
            errores_lineas.append(tuple((ruta,i)))      
            #lines.remove(ln)            
    f.close()
    # Leer datos de capturados
    datos = pd.read_table(ruta,sep=',',encoding='utf-8',error_bad_lines=True,warn_bad_lines=True,header=0)#,na_values=[""," "])
    datos=datos[datos.columns[:11]]
    datos.columns= ["nombre","categoria_web","email","telefono","direccion","cp","ciudad","web","pahttp","longitud","latitud"]
    datos[['longitud','latitud']]=datos[['longitud','latitud']].fillna(0)
    if (not datos.applymap(np.isreal)['longitud'].all()) | (not datos.applymap(np.isreal)['latitud'].all()):
        errores_posicion.append(ruta)
    
(pd.DataFrame({'ruta':[e[0] for e in errores_lineas],'linea':[e[1] for e in errores_lineas]})).to_csv('errores_lineas_' + ficheros + '.csv')
print('%i líneas con error en %i líneas tratadas' % (len(errores_lineas),nl))

(pd.DataFrame(errores_posicion)).to_csv('errores_posicion_' + ficheros + '.csv')
print('%i fichero con errores en posicion' % len(errores_posicion))
