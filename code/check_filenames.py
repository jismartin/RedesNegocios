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
    return s.lower()

#ficheros_lista = pd.read_csv('cyl.txt', names=['ruta'])
#ficheros_lista = pd.read_csv('barcelona.txt', names=['ruta'])
ficheros_lista = pd.read_csv('madrid.txt', names=['ruta'])


sclase=list()
for ruta in ficheros_lista.ruta:
    print(ruta)
    # Número de tipo de negocio
    numeros = [ int(s) for s in re.findall(r'\d+',ruta)]
    clase = numeros[0]

    # Subclase de negocio
    subclase=re.split('\d+',ruta)[1]
    # Eliminar caracteres
    subclase=re.sub('_','',subclase)
    subclase=re.sub('-','',subclase)
    subclase=re.sub('.csv','',subclase)
    subclase=normaliza_texto1(subclase)
    sclase.append(tuple((ruta,clase,subclase)))

spd=pd.DataFrame({'ruta':[s[0] for s in sclase],'clase':[s[1] for s in sclase],'sclase':[s[2] for s in sclase]})
spd.to_csv('subclases.csv')