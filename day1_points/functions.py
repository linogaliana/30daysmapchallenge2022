import pynsee
import requests
import io   
import numpy as np
import time
import geopandas as gpd
import contextily as ctx


def create_dataset_sirene(code_ape = '10.51C', number = 15000):
    # search data in SIRENE database
    data = pynsee.search_sirene(variable = "activitePrincipaleEtablissement",
                       pattern = code_ape, kind = 'siret', number=number)
    data = data.loc[data['dateFin'].isnull()]
    data['adresse'] = data['numeroVoieEtablissement'] + " " + data['typeVoieEtablissement'] + " " + data['libelleVoieEtablissement']
    data[['adresse','codePostalEtablissement','libelleCommuneEtablissement']] = data.loc[:, ['adresse','codePostalEtablissement','libelleCommuneEtablissement']].apply(lambda s: s.str.lower().str.replace(","," "))
    return data

def create_dataset_sirene_dep(code_ape = '10.51C', code_dep = "75*", number = 2000):
    # search data in SIRENE database
    data = pynsee.search_sirene(variable = ["activitePrincipaleEtablissement", "codePostalEtablissement"],
                       pattern = [code_ape, code_dep], kind = 'siret', number=number)
    data = data.loc[data['dateFin'].isnull()]
    data['adresse'] = data['numeroVoieEtablissement'] + " " + data['typeVoieEtablissement'] + " " + data['libelleVoieEtablissement']
    data[['adresse','codePostalEtablissement','libelleCommuneEtablissement']] = data.loc[:, ['adresse','codePostalEtablissement','libelleCommuneEtablissement']].apply(lambda s: s.str.lower().str.replace(","," "))
    return data

params = {
    'columns': ['adresse', 'libelleCommuneEtablissement'],
    'postcode': 'codePostalEtablissement',
    'result_columns': ['result_score', 'latitude', 'longitude'],
}


def geoloc_chunk(x):
    dfgeoloc = x.loc[:, ['adresse','codePostalEtablissement','libelleCommuneEtablissement']]
    dfgeoloc.to_csv("datageocodage.csv", index=False)
    response = requests.post('https://api-adresse.data.gouv.fr/search/csv/', data=params, files={'data': ('datageocodage.csv', open('datageocodage.csv', 'rb'))})
    geoloc = pd.read_csv(io.StringIO(response.text), dtype = {'CP': 'str'})
    return geoloc


def geoloc_data(data):
    start_time = time.time()
    geodata = [geoloc_chunk(dd) for dd in np.array_split(data, 10)]
    print("--- %s seconds ---" % (time.time() - start_time))
    geodata = pd.concat(geodata)
    geodata_complete = pd.concat([data.reset_index(), geodata.loc[:, ['latitude', 'longitude']].reset_index()], axis=1) 
    geodata_complete = geodata_complete.loc[geodata_complete['longitude'].astype("double")>-20]
    geodata_complete = geodata_complete.loc[geodata_complete['longitude'].astype("double")<30]
    return geodata_complete