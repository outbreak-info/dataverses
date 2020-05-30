import requests
import json
from datetime    import date
from html.parser import HTMLParser

QUERIES = ["2019-nCoV", "COVID-19", "COVID-19 virus", "COVID19", "COVID19 virus", "HCoV-19", "HCoV19", "Human coronavirus 19", "Human coronavirus 2019", "SARS-2", "SARS-CoV-2", "SARS-CoV2", "SARS2", "SARSCoV-2", "SARSCoV2",
                 "Severe acute respiratory syndrome coronavirus 2", "Wuhan coronavirus", "Wuhan seafood market pneumonia virus", "coronavirus disease", "coronavirus disease 19", "coronavirus disease 2019", "novel coronavirus", "novel coronavirus 2019"]
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
        query_string = "+".join(f"\"{q}\"" for q in queries)

    if response_types:
        type_string = "".join(f"&type={r}" for r in response_types)

    if subtrees:
        subtree_string = "".join(f"&subtree={s}" for s in subtrees)

    return f"{server}search?q={query_string}{type_string}{subtree_string}"

def compile_paginated_data(query_endpoint, per_page=100):
    """
    pages through data, compiling all response['data']['items']
    and returning them.
    per_page max is 1000
    """

    continue_paging = True
    start = 0
    data = []

    while continue_paging:
        url = f"{query_endpoint}&per_page={per_page}&start={start}"
        #print(f"getting {url}")
        req = requests.get(url)
        response = req.json()
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
    dataverses = find_relevant_dataverses(QUERIES)
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

    req    = requests.get(url)
    parser = SchemaScraper()
    parser.feed(req.text)
    if parser.schema:
        return parser.schema
    return False

def fetch_schemas():
    dataset_endpoint = compile_query(DATAVERSE_SERVER, QUERIES, response_types=["dataset", "file"])
    datasets = compile_paginated_data(dataset_endpoint)

    #ds = len([i for i in datasets if i['type'] == 'dataset'])
    #fs = len([i for i in datasets if i['type'] == 'file'])
    #print(f"ds, fs: {ds} {fs}")

    data_for_gid = {d.get('global_id'): d for d in datasets}
    schema_org_exports = {}

    additional_datasets = get_all_datasets_from_dataverses()
    additional_data_for_gid = {d.get('global_id'): d for d in additional_datasets}
    #additional_global_ids = set()
    #ds = len([i for i in additional_datasets if i['type'] == 'dataset'])
    #fs = len([i for i in additional_datasets if i['type'] == 'file'])
    #print(f"add'l ds, fs: {ds} {fs}")


    # set-wise union, remove "" if it's in there
    total_datasets = {
            **data_for_gid,
            **additional_data_for_gid
    }
    total_datasets.pop('')

    for gid, dataset in total_datasets.items():
        schema_export_url = f"{EXPORT_URL}&persistentId={gid}"
        req = requests.get(schema_export_url)
        res = req.json()
        if res.get('status') and res.get('status') == 'ERROR':
            schema = scrape_schema_representation(dataset.get('url'))
            if schema:
                schema_org_exports[gid] = schema
                print(f"{dataset['global_id']}: got schema")
        else:
            schema_org_exports[gid] = res
    return schema_org_exports

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
    authors = []
    for author in s['author']:
        a = {"@type": "Person",
             "name": author['name']
             }
        if author.get('affiliation'):
            a['affiliation'] = {
                 "@type": "Organization",
                  "name": author.get('affiliation')
                  }
        authors.append(a)

    resource = {
        "_id": _id,
        "doi": doi,
        "curatedBy": curatedBy,
        "author": a,
        "name":          s['name'],
        "description":   s['description'][0],
        "identifier":    s["@id"], # ?
        "dateModified":  s['dateModified'],
        "datePublished": s['datePublished'],
        "keywords":      s['keywords'],
        "license":       s['license'].get('url'),
        "distribution":  s["distribution"],
    }

    return resource


if __name__ == "__main__":
    schemas = fetch_schemas()
    coerced_data = []
    for gid, schema in schemas.items():
        coerced_data.append(transform_schema(schema, gid))

    print(coerced_data)
