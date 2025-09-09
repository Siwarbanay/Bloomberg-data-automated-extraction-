#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from datetime import date
import logging
import time
import uuid
import pandas as pd
import msgspec
from timeit import default_timer as timer
from urllib.parse import urljoin
from DataLicenseLibrary.Session import create_session
from DataLicenseLibrary.credential_loader import load_credentials
from datetime import datetime, timedelta
import re
from DataLicenseLibrary.requests.DataRequest import (
    build_data_request,
    build_history_request,
)

# %% Logging
logging.basicConfig(
    format="%(asctime)s [%(levelname)-8s] [%(name)s:%(lineno)s]: %(message)s",
)
# Permet de fixer le niveau de logging global
logging.root.setLevel(logging.INFO)
LOG = logging.getLogger(__name__)

# %% constantes
HOST = "https://api.bloomberg.com"
DECODER = msgspec.json.Decoder()
PAGE_REXP = re.compile("page=(\\w*)")
MAX_PAGE = 100  # pour éviter une boucle infinie.
CATALOG = 
REPLY_TIMEOUT = timedelta(minutes=5)
RETRY_DELAY = 60  # seconds
file_path = r'\\.csv'


# %% Fonctions utilitaires
def decode(response):
    """Decode la réponse en json

    Args:
        response (requests.response): une reponse

    Returns:
        _dict_: json
    """
    return DECODER.decode(response.content)


def get_session(credentials_path=None):
    if credentials_path is None:
        CREDENTIALS = load_credentials()
    else:
        CREDENTIALS = load_credentials(PATH=credentials_path)

    return create_session(CREDENTIALS)


def field_description(
    search_string, session=None, download=False, more_than_one_page=False
):
    """Fait une recherche sur les champs (Equivalent à la recherche FLDS).
    Les résultats sont triés par pertinence. La recherche est trés fussy et retourne beaucoup
    de résultats non pertinents, donc il faut forcer la récupération des pages suivantes avec l'argument optionel
    more_than_page.

    Args:
        search_string (_type_): une chaine de caractére de recherche.
        session (_type_, optional): Permet de réutiliser une session lors d'appel répetés . Defaults to None.
        download (bool, optional): est-ce qu'on conserve les json envoyés ?. Defaults to False.
        more_than_one_page (bool, optional): Récupére les resultats de recherche au dela des 20 premiers. Defaults to False.

    Returns:
        _DataFrame_: retourne un DataFrame avec la desciption des fields.
    """
    # if no session is provided we provide one. Reusing session is efficient.
    if session is None:
        session = get_session()

    page = 1  # 1 based indexing of page in view
    # https://developer.blpprofessional.com/portal/products/dl/reference#section/Getting-Started/Pagination

    universe_url = urljoin(HOST, "/eap/catalogs/bbg/fields/")

    results = []
    start = timer()
    while page < (MAX_PAGE if more_than_one_page else 2):
        LOG.info(f"Processing page {page}...")
        # https://stackoverflow.com/questions/75810236/does-the-python-requests-library-support-query-param-with-no-value
        # Add Data License	string Filter by Data License. This is only applicable when catalog is bbg.
        query = {
            "page": page,
            "q": search_string,
            "sort": "relevance",
            "Data License": "True",
        }

        response = session.get(
            universe_url,
            params=query,
        )

        if download:
            with open(
                f"DataLicenseLibrary/downloads/download_{datetime.today():%Y%m%d_%H%M%S}.json",
                "wb",
            ) as outputfile:
                outputfile.write(response.content)

        json_content = decode(response)

        if "contains" in json_content:
            results.append(pd.json_normalize(json_content["contains"]))

        if not "next" in json_content["view"]:
            break  # fin de la pagination

        page = int(
            PAGE_REXP.search(json_content["view"]["next"]).group(1)
        )  # next iteration

    df = pd.concat(results, axis=0)
    end = timer()
    # print(df)
    LOG.info(f"Loading Fields info took: {end - start} seconds.")
    return df


def field_metadata(field, session=None):
    """Retourne du json formatté en dictionnaire des attributs du champs (si il est data licence, quelle YK...)
    https://developer.blpprofessional.com/portal/products/dl/reference#tag/Fields/operation/getField

    Args:
        field (_type_): Field identifier. You can use either Mnemonic (CURVE_TENOR_RATES), Old Mnemonic, Clean Name or Field ID (DZ870).
        session (_type_, optional): Permet de réutiliser une session lors d'appel répetés . Defaults to None.

    Returns:
        _dictionnaire_: Attributs du champ.
    """
    # if no session is provided we provide one. Reusing session is efficient.
    if session is None:
        session = get_session()

    page = 1  # 1 based indexing of page in view
    # https://developer.blpprofessional.com/portal/products/dl/reference#section/Getting-Started/Pagination

    url = urljoin(HOST, f"/eap/catalogs/bbg/fields/{field}")

    start = timer()
    response = session.get(
        url,
    )
    json_content = decode(response)
    end = timer()
    LOG.info(f"Loading Field Metadata  took: {end - start} seconds.")
    return json_content


def get_scheduled_catalogs(session=None):
    # if no session is provided we provide one. Reusing session is efficient.
    if session is None:
        session = get_session()

    catalogs_url = urljoin(HOST, "/eap/catalogs/")
    response = session.get(catalogs_url)

    scheduled_catalogs = []
    # We got back a good response. Let's extract our account number.
    catalogs = response.json()["contains"]
    for catalog in catalogs:
        if catalog["subscriptionType"] == "scheduled":
            # Take the catalog having "scheduled" subscription type,
            # which corresponds to the Data License account number.
            scheduled_catalogs.append(catalog["identifier"])

    return scheduled_catalogs


def data_request(tickers, fields, session=None):
    """Récupére des données la valeur courante des champs d'une liste d'identifiants

    Args:
        tickers (pd.DataFrame ou iterable): Soit un DataFrame à 2 colonnes (IdentifierType, IdentifierValue) ou 4 (avec colonnes override_mnemonic, override_value), soit une liste de Tickers.
        Pour l'instant, un seul override distinct possible par identifiant.soit une liste de Tickers
        fields (list): La liste des mnemoonic de champ. voir fieldInfo pour utiliser le bon voir Api.field_metadata
        session (_type_, optional): _description_. Defaults to None.

    Returns:
        _type_: _description_
    """
    # if no session is provided we provide one. Reusing session is efficient.
    if session is None:
        session = get_session()

    name = "" + str(uuid.uuid1())[:6]

    # build json of the request
    data = build_data_request(name, tickers, fields)

    # On commence le dialogue avec le serveur.
    response = session.post(
        urljoin(HOST, f"/eap/catalogs/{CATALOG}/requests/"),
        data=data,
        headers={"Content-type": "application/json"},
    )

    # Extract the identifier of the created resource.
    request_id = decode(response)["request"]["identifier"]

    # Filter the required output from the available content by passing the request_name as prefix
    # and request_id as unique requestIdentifier query parameters respectively.
    params = {
        "prefix": name,
        "requestIdentifier": request_id,
    }

    response_url = urljoin(HOST, f"/eap/catalogs/{CATALOG}/content/responses/")

    # ça ne sert à rien d'envoyer la premiere requete immédiatement...
    LOG.info(f"Waiting {RETRY_DELAY} seconds before fetching results...")
    time.sleep(RETRY_DELAY)

    # We recommend adjusting the polling frequency and timeout based
    # on the amount of data or the time range requested.
    timeout = datetime.now() + REPLY_TIMEOUT

    while datetime.now() < timeout:
        content_response = session.get(response_url, params=params)
        response_key = decode(content_response)["contains"]

        if len(response_key) == 0:
            time.sleep(RETRY_DELAY)
            continue

        output_url = urljoin(response_url, response_key[0]["key"])
        break
    else:
        LOG.info("Response not received within within timeout. Exiting.")

    # récupération de la réponse sous forme de DataFrame
    with session.get(output_url, stream=True) as response:
        LOG.info(f"Encoding is {response.headers['content-encoding']}")
        json_content = decode(response)
        return pd.json_normalize(json_content)


def history_request(tickers, fields, startDate, endDate, session=None):
    """Interroge l'historique d'une liste de tickers pour une plage de date donnée.

    Args:
        tickers (pd.DataFrame ou iterable): Soit un DataFrame à 2 colonnes (IdentifierType, IdentifierValue), soit une liste de Tickers.
        Pour l'instant, un seul override distinct possible par identifiant.soit une liste de Tickers
        fields (list): _description_
        startDate (datetime.date): Démarrage de la plage de dates historiques
        endDate (datetime.date): Fin de la plage de dates historiques
        session (_type_, optional): _description_. Defaults to None.

    Returns:
        _type_: _description_
    """
    # if no session is provided we provide one. Reusing session is efficient.
    if session is None:
        session = get_session()

    name = "" + str(uuid.uuid1())[:6]

