# -*- coding: utf-8 -*-
"""
@author: nacho
"""
#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3 as lite
import pandas as pd
from os.path import exists
import re
import unicodedata
from sqlalchemy import create_engine

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

def normaliza_texto3(s):
    #s=s.decode('utf8')
    s=elimina_tildes(s)
    return s
    
# Para almacenar posibles errores
errores = [] 

# Necesitamos una lista con las rutas a los ficheros de capturas de datos
# En linux shell es muy fácil: 
#   find ./ -name "*.csv" > ficheros.txt

name='barcelona_bares_7224'
ficheros_lista = pd.read_csv(name+'.txt', names=['ruta'])
n_muestras = 0
total_muestras = len(ficheros_lista.ruta)

# Conectarse a la base de datos
#con = lite.connect(input('Nombre base de datos: '))
#cur = con.cursor()    

datos_completos=pd.DataFrame({"clase":[],"subclase":[],"nombre":[],"categoria_web":[],"direccion":[],
                              "cp":[],"ciudad":[],"longitud":[],"latitud":[]})
n_registros_iniciales=0
n_registros_finales=0
for ruta in ficheros_lista.ruta:
    if exists(ruta):    
        n_muestras = n_muestras + 1
        print("Procesando muestra %d de un total de %d" % (n_muestras, total_muestras))

        # Número de tipo de negocio
        clase = 7224

        # Subclase de negocio
        subclase=''
        
        # Leer datos de capturados
        datos = pd.read_table(ruta,sep=',',encoding='utf-8',error_bad_lines=True,warn_bad_lines=True,header=0,na_values=[""," "])
        datos=datos[datos.columns[:11]]
        datos.columns= ["nombre","categoria_web","email","telefono","direccion","cp","ciudad","web","pahttp","longitud","latitud"]
        
        # eliminar columnas no necesarias
        datos.drop('email',axis=1,inplace=True)
        datos.drop('telefono',axis=1,inplace=True)
        datos.drop('web',axis=1,inplace=True)
        datos.drop('pahttp',axis=1,inplace=True)
        #datos.drop('x',axis=1,inplace=True)

        # eliminar registros sin posibilidad de tratamiento 
        n_inicial = len(datos)
        n_registros_iniciales+=n_inicial 
        datos= datos.dropna(subset=['direccion','cp','ciudad','nombre'])                    
        n_final = len(datos)
        #n_registros_finales+=n_final 

        # rellenar con 0 posibles campos de longitud, latitud vacíos        
        datos.fillna(0,inplace=True)
                      
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
        if n_sin_posicion >0:
            errores.append(tuple((normaliza_texto3(ruta), ('%i negocios sin geolocalizacion guardados' % n_sin_posicion ))))
        if n_final > n_inicial:
            errores.append(tuple((normaliza_texto3(ruta), ('%i registros sin posibilidad de tratamiento eliminados' % (n_inicial-n_final) ))))


        # añadir clase y subclase
        datos['clase']=clase
        datos['subclase']=subclase
        datos['archivo']=ruta

        # Añadir a datos completos
        datos_completos = datos_completos.append(datos)
         
#        # Copiar en base de datos
#        cur.executemany("INSERT INTO negocios (clase, subclase, nombre, categoria_web, direccion, cp, ciudad, longitud, latitud)" +
#                " VALUES(?,?,?,?,?,?,?,?,?);", [tuple([clase,subclase]) + tuple(x) for x in datos[['nombre', 'categoria_web', 'direccion', 'cp', 'ciudad', 'longitud', 'latitud']].values])
#        con.commit()

    else:
        errores.append(tuple((normaliza_texto3(ruta),'No existe fichero csv')))

        
#if len(errores) > 0:
#    cur.executemany("INSERT INTO errores (ruta, error)" +
#                " VALUES(?,?);",  [e for e in errores])
#    con.commit()
#
#    for e in errores:
#        print e
#        cur.executemany("INSERT INTO errores (ruta, error)" +
#                " VALUES(?,?);",  [e])
#        con.commit()

n_registros_finales=datos_completos.shape[0]
n_registros_sin_tratamiento=n_registros_iniciales - n_registros_finales

# Reinicializar índices
datos_completos.reset_index(drop=True,inplace=True)

# Eliminar duplicados
datos_completos.drop_duplicates(inplace=True)

n_registros_duplicados=n_registros_finales - datos_completos.shape[0]

errores.append(tuple((name,'Registros (Iniciales %i; Sin tratamiento %i; Duplicados %i; Guardados %i)' % 
                      (n_registros_iniciales,n_registros_sin_tratamiento,n_registros_duplicados,datos_completos.shape[0]))))

# Guardar como csv
datos_completos.to_csv('datos_' + name +'2.csv')
datos_completos.drop(['archivo'],axis=1,inplace=True)

# Guardar como sqlite
engine = create_engine('sqlite:///datos_' + name +'.sqlite')
conn = engine.connect()
datos_completos.to_sql('negocios',con=conn, if_exists='replace',index=False)

# Guardar como csv
datos_completos.to_csv('datos_' + name +'.csv')

# Guardar errore como csv
(pd.DataFrame(errores)).to_csv('errores_' + name + '.csv')

#con.close()

# datos sin geolocalizacion
resumen=[]
for c in datos_completos['clase'].unique():
    total =  (datos_completos['clase']==c).sum()
    sin = ((datos_completos['clase']==c) & 
            ( (datos_completos['latitud']==0) | (datos_completos['longitud']==0))).sum()
    
    resumen.append(list((int(c),sin,sin/total)))
    
resumen=pd.DataFrame(resumen,columns=['clase','sin_geolocalizacion','sin_geolocalizacion_porcentaje'])

# Guardar como csv
resumen.to_csv('resumen_' + name +'.csv')

datos_completos[(datos_completos['latitud']==0) | (datos_completos['longitud']==0)].to_csv('sin_geolocalizacion_'+name+'.csv')

