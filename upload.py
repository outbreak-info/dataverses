import biothings.hub.dataload.uploader

try:
    from dataverses.parser import get_parsed_data as parser_func
except ImportError:
    from .parser import get_parsed_data as parser_func

class DataverseUploader(biothings.hub.dataload.uploader.BaseSourceUploader):
    name = "dataverse"
    __metadata__ = {
            "src_meta": {
                "author": {
                    "name": "Julia Mullen",
                    "url":  "https://github.com/juliamullen"
                    },
                "code": {
                    "branch": "master",
                    "repo": "https://github.com/juliamullen/dataverses"
                    },
                "url": "https://dataverse.org/",
                "license": "https://dataverse.org/best-practices/harvard-dataverse-general-terms-use",
                }
            }
    idconverter = None
    main_source = "dataverse"
    storage_class = biothings.hub.dataload.storage.BasicStorage

    def load_data(self, data_folder):
        if data_folder:
            self.logger.info("Load data from directory: '%s'", data_folder)
        return parser_func()

    @classmethod
    def get_mapping(klass):
        r = requests.get(MAP_URL)
        if (r.status_code == 200):
            mapping = r.json
            mapping_dict = {key: mapping[key] for key in MAP_VARS}
            return mapping_dict
