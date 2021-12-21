import pandas as pd
import numpy as np
from pandas.core.frame import DataFrame
from utils import HEADERS_DEMOGRAFICOS, HEADERS_DISPERSIONES, HEADERS_DOMICILIOS

def load_files():
    demograficos = pd.read_csv('./DM_DatosGenerales.txt', delimiter='|', encoding='latin1', header=0, dtype=object)
    dispersiones = pd.read_csv('./DM_DatosDispersion.txt', delimiter='|', encoding='latin1', header=0, dtype=object)
    domicilios = pd.read_csv('./DM_DatosDomicilio.txt', delimiter='|', encoding='latin1', header=0, dtype=object)
    marca_big = pd.read_csv('./Marca_BIG.txt', delimiter='|', encoding='latin1', header=0, dtype=object)

    return demograficos, dispersiones, domicilios, marca_big

def transform_files(N, demograficos: pd.DataFrame, dispersiones: pd.DataFrame, domicilios: pd.DataFrame, marca_big: pd.DataFrame):
    '''
        TRANSFORMACION DE DataFrame Dispersiones

        Se eliminan valorar NaN
        1) Clientes sin periodicidad calculada
        2) Clientes sin dispersiones
        3) Clientes sin salario calculado
    '''
    dispersiones['FN_SALARIO'] = dispersiones['FN_SALARIO'].astype(float)
    dispersiones['FIPERIODICIDADPAGO'] = pd.to_numeric(dispersiones['FIPERIODICIDADPAGO'], errors='coerce')
    dispersiones = dispersiones.dropna(subset=['FIPERIODICIDADPAGO'])
    dispersiones = dispersiones.dropna(subset=['FI_TOTALDISPER'])
    dispersiones = dispersiones[dispersiones['FN_SALARIO'] > 0]

    ## Ya sin valores NaN, se realiza la conversion de tipo a enteros
    dispersiones['FIPERIODICIDADPAGO'] = dispersiones['FIPERIODICIDADPAGO'].astype(int)
    dispersiones['FI_TOTALDISPER'] = dispersiones['FI_TOTALDISPER'].astype(int)

    '''
        TRANSFORMACION DE DataFrame Domicilios

        Se eliminan valorar NaN
        1) Clientes sin latitud (por ende sin longitud)
        2) Clientes sin id Estado
        3) Clientes sin id Municipio
        4) Clientes sin id Colonia
    '''
    domicilios = domicilios.dropna(subset=['FC_LATITUD'])
    domicilios = domicilios.dropna(subset=['FC_ID_ESTADO'])
    domicilios = domicilios.dropna(subset=['FI_ID_MUNICIPIO'])
    domicilios = domicilios.dropna(subset=['FI_ID_COLONIA'])

    '''
        La peticion del mail es que todos los campos de id Domicilio tengan valor y que a su vez
        los clientes sean acreditables, casos completeamente idealizados. LOL Se realiza un merge
        entre los dataFrames ya filtrados.
    '''
    set = dispersiones.merge(domicilios, on='FI_EMPLEADO').merge(demograficos, on='FI_EMPLEADO')
    # Se obtiene una muestra del set 
    subset = set.sample(n=N)
    # Se crean los archivos de muestra con el número de registros solicitado
    m_demograficos = subset[HEADERS_DEMOGRAFICOS]

    m_dispersiones = subset[HEADERS_DISPERSIONES]
    
    m_domicilios = subset[HEADERS_DOMICILIOS]

    # Se corrigen nombres de columnas debido al Merge
    m_demograficos.rename(columns={'FI_ID_PAIS_y':'FI_ID_PAIS'}, inplace=True)
    m_dispersiones.rename(columns={'#FI_ID_INSTITUCION_x':'#FI_ID_INSTITUCION'}, inplace=True)
    m_domicilios.rename(columns={'#FI_ID_INSTITUCION_y':'#FI_ID_INSTITUCION','FI_ID_PAIS_x':'FI_ID_PAIS'}, inplace=True)
    # Se homologan nombres de columnas en DataFrame MarcaBIG para poder hacer el merge
    marca_big.rename(columns={'#FI_ID_PAIS_CU':'FI_PAISCU','FI_ID_SUCURSAL_CU':'FI_SUCURSALCU',
                              'FI_ID_CANAL_CU':'FI_CANALCU','FI_FOLIO_CU':'FI_FOLIOCU'}, inplace=True)
    # Se ordenan las columnas
    marca_big = marca_big[['FI_PAISCU','FI_CANALCU','FI_SUCURSALCU','FI_FOLIOCU','FI_COD_IDENTIFICADOR','FI_MARCA']]
    # Se realiza el merge con los id de la muestra
    m_marca_big =marca_big.merge(m_demograficos, on=['FI_PAISCU','FI_CANALCU','FI_SUCURSALCU','FI_FOLIOCU'])
    # Se recuperan las columnas iniciales
    m_marca_big = m_marca_big[['FI_PAISCU','FI_CANALCU','FI_SUCURSALCU','FI_FOLIOCU','FI_COD_IDENTIFICADOR','FI_MARCA']]
    # Se cambia el nombre a las columnas al valor original
    m_marca_big.rename(columns={'FI_PAISCU': '#FI_ID_PAIS_CU', 'FI_CANALCU': 'FI_ID_CANAL_CU', 
                                'FI_SUCURSALCU': 'FI_ID_SUCURSAL_CU', 'FI_FOLIOCU': 'FI_FOLIO_CU'}, inplace=True)

    return m_demograficos, m_dispersiones, m_domicilios, m_marca_big

def write_files(m_demograficos, m_dispersiones, m_domicilios, m_marca_big):
    m_domicilios.to_csv('DM_DatosDomicilio.txt.1', sep='|', index=False)
    m_dispersiones.to_csv('DM_DatosDispersion.txt.1', sep='|', index=False)
    m_demograficos.to_csv('DM_DatosGenerales.txt.1', sep='|', index=False)
    m_marca_big.to_csv('Marca_BIG.txt.1', sep='|', index=False)

def run(N:int = 100):
    # Se leen los archivos
    demograficos, dispersiones, domicilios, marca_big = load_files()
    # Se procesan los archivos
    m_demograficos, m_dispersiones, m_domicilios, m_marca_big = transform_files(N, demograficos, dispersiones, domicilios, marca_big)
    # Se escriben los archivos
    write_files(m_demograficos, m_dispersiones, m_domicilios, m_marca_big)

if __name__ == '__main__':
    run()