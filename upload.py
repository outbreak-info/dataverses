import biothings.hub.dataload.uploader

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
                "url": "",
                "license": "",
                }
            }
    idconverter = None
    storage_class = biothings.hub.dataload.storage.BasicStorage

    def load_data(self, data_folder):
        # ?
        if data_folder:
            self.logger.info("Load data from directory: '%s'", data_folder)
        return parser_func()
