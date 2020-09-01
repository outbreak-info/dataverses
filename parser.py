import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json

from datetime    import date
from html.parser import HTMLParser

try:
    from biothings import config
    logger = config.logger
except ImportError:
    import logging
    logger = logging.getLogger('dataverse')
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler('dataverse_log.log')
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def requests_retry_session(retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504), session=None):
    """
    request backoff + retry helper
    https://www.peterbe.com/plog/best-practice-with-retries-with-requests
    """

    session = session or requests.Session()
    retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session
 
QUERIES = ["2019-nCoV", "COVID-19", "COVID19", "SARS-2", "SARS-CoV-2", "SARS2", "coronavirus disease", "novel coronavirus"]
TIMEOUT = 300

DATAVERSE_SERVER = "https://dataverse.harvard.edu/api/"
EXPORT_URL = f"{DATAVERSE_SERVER}datasets/export?exporter=schema.org"

def compile_query(server, queries=None, response_types=None, subtrees=None):
    """
    Queries are string queries, e.g., "COVID-19"
    Response types are e.g., "dataverse", "dataset", "file"
    Subtrees are specific dataverse IDs

    All can have multiple values. Response types and subtrees are OR'd
    Queries are probably OR'd
    """

    query_string = "*"
    type_string  = ""
    subtree_string = ""

    if queries:
        # turn ["a", "b"] into '"a"+"b"'
        if type(queries) == str:
            query_string = f"\"{queries}\""
        else:
            query_string = "+".join(f"\"{q}\"" for q in queries)

    if response_types:
        type_string = "".join(f"&type={r}" for r in response_types)

    if subtrees:
        subtree_string = "".join(f"&subtree={s}" for s in subtrees)

    return f"{server}search?q={query_string}{type_string}{subtree_string}"

def compile_paginated_data(query_endpoint, per_page=1000):
    """
    pages through data, compiling all response['data']['items']
    and returning them.
    per_page max is 1000
    """

    continue_paging = True
    start   = 0
    data    = []
    retries = 0

    while continue_paging:
        url = f"{query_endpoint}&per_page={per_page}&start={start}"
        logger.info(f"getting {url}")
        try:
            req = requests_retry_session().get(url, timeout=TIMEOUT)
        except Exception as requestException:
            logger.error(f"Failed to get {url} due to {requestException}")
            if retries > 5:
                logger.error("Failed too many times")
                return data
            retries += 1
            # after 1 retry, limit per-page to 200, after 2, limit to 50
            per_page = 200 if retries == 1 else 50
            continue
        try:
            response = req.json()
        except ValueError:
            logger.error(f"Failed to get a JSON response from {url}")
        total = response.get('data').get('total_count')
        data.extend(response.get('data').get('items'))
        start += per_page
        continue_paging = total and start < total

    return data

def find_relevant_dataverses(query):
    """
    Returns a list of dataverse IDs
    """
    response_types = ["dataverse"]
    query_endpoint = compile_query(DATAVERSE_SERVER, query, response_types)
    dataverses = [data['identifier'] for data
                  in compile_paginated_data(query_endpoint)]
    return dataverses

def find_within_dataverse(dataverse_id, query):
    """
    searches for query within a specific dataverse
    or can group all dataverse IDs into a singular batch of requests
    """

    response_types = ["dataset", "file"]
    query_endpoint = compile_query(DATAVERSE_SERVER,
            query,
            response_types=response_types,
            subtrees=[dataverse_id])
    datasets_and_files = compile_paginated_data(query_endpoint, per_page=1000)
    return datasets_and_files

def get_all_datasets_from_dataverses():
    logger.info("finding all dataverses that match for queries")
    dataverses = find_relevant_dataverses(QUERIES)

    logger.info("grabbing datasets from each matched dataverse")
    datasets = []
    for dataverse in dataverses:
        datasets.extend(find_within_dataverse(dataverse, query=None))

    return datasets

def scrape_schema_representation(url):
    """
    when the schema.org export of the dataset fails
    this will grab it from the url
    by looking for <script type="application/ld+json">
    """
    logger.warning(f"scraping schema.org representation from the dataset url {url}")
    class SchemaScraper(HTMLParser):
        def __init__(self):
            super().__init__()
            self.readingSchema = False
            self.schema = None

        def handle_starttag(self, tag, attrs):
            if tag == 'script' and 'type' in attrs and attrs.get('type') == "application/ld+json":
                self.readingSchema = True

        def handle_data(self, data):
            if self.readingSchema:
                self.schema = data
                self.readingSchema = False

    try:
        req = requests_retry_session().get(url, timeout=TIMEOUT)
    except Exception as requestException:
        logger.error(f"Failed to get {url} due to {requestException}")
        return False
    if not req.ok:
        logger.error(f"failed to get {url}")
        return False
    parser = SchemaScraper()
    parser.feed(req.text)
    if parser.schema:
        return parser.schema
    return False

def fetch_datasets():
    """
    grabs all datasets and files related to QUERIES both by querying
    and by grabbing everything in related dataverses
    extracts their global_id, which in this case is a DOI
    returns a dictionary mapping global_id -> dataset
    """

    logger.info("getting all datasets that match queries")

    dataset_ids = set([None])
    datasets    = []

    for query in QUERIES:
        dataset_endpoint    = compile_query(DATAVERSE_SERVER, query, response_types=["dataset", "file"])
        new_datasets        = compile_paginated_data(dataset_endpoint)
        unique_new_datasets = [i for i in new_datasets if i.get('global_id') not in dataset_ids]
        datasets.extend(unique_new_datasets)

        # union-equals instead of += for sets
        dataset_ids        |= set([i.get('global_id') for i in unique_new_datasets])

    data_for_gid = {d.get('global_id'): d for d in datasets}
    schema_org_exports = {}

    additional_datasets = get_all_datasets_from_dataverses()
    additional_data_for_gid = {d.get('global_id'): d for d in additional_datasets}

    total_datasets = {
            **data_for_gid,
            **additional_data_for_gid
    }

    try:
        total_datasets.pop('')
    except KeyError:
        pass

    return total_datasets


def get_schema(gid, url):
    schema_export_url = f"{EXPORT_URL}&persistentId={gid}"
    logger.info(f"getting schema {url}")
    try:
        req = requests_retry_session().get(schema_export_url, timeout=TIMEOUT)
    except Exception as requestException:
        logger.error(f"Failed to get {url} due to {requestException}")
        return False
    res = req.json()
    if res.get('status') and res.get('status') == 'ERROR':
        logger.warning("schema export failed, scraping instead")
        schema = scrape_schema_representation(url)
        if schema:
            return schema
    else:
        # success, response is the schema
        return res

def transform_schema(s, gid):
    """
    Turn schema.org representation given by dataverse
    to outbreak.info format
    """

    # 'doi:10.7910/DVN/XWVOA8' -> 'DVN_XWVOA8'
    _id = 'dataverse' + '_'.join(gid.split('/')[1:])
    # 'doi:10.7910/DVN/XWVOA8' -> '10.7910/DVN/XWVOA8'
    doi = gid.replace('doi:', '')
    today = date.today().strftime("%Y-%m-%d")

    curatedBy = {
            "@type": "Organization",
            "name":  s.get("provider", "Harvard Dataverse").get("name", "Harvard Dataverse"),
            "url":   s['@id'],
            "curationDate": today,
    }
    authors = [personify(author) for author in s['author']]
    creator = [personify(creator) for creator in s['creator']]
    license = s['license'].get('url')

    pass_through_fields = ['name', 'dateModified', 'datePublished', 'keywords', 'distribution', '@id', 'funder', 'identifier', 'version', '@type']

    resource = {
        "@type": "Dataset",
        "_id": _id,
        "doi": doi,
        "curatedBy": curatedBy,
        "author": authors,
        "creator": creator,
        "description":   s['description'][0],
        "identifier":    s["@id"], # ?
    }

    if license:
        resource['license'] = license

    for field in pass_through_fields:
        resource = add_field(resource, s, field)

    return resource

def personify(person_obj):
    personified = {
         "@type": "Person",
         "name": person_obj['name']
    }
    if person_obj.get('affiliation'):
        personified['affiliation'] = [{
             "@type": "Organization",
              "name": person_obj['affiliation']
              }]
    return personified

def add_field(resource, origin, field_name):
    field = origin.get(field_name)
    if field:
        resource[field_name] = field
    return resource

def load_annotations():
    datasets = fetch_datasets()
    for gid, dataset in datasets.items():
        schema = get_schema(gid, dataset.get('url'))
        if not schema:
            continue

        transformed = transform_schema(schema, gid)
        yield transformed

if __name__ == "__main__":
    with open('transformed.json', 'w') as output:
        json.dump([i for i in load_annotations()], output)
