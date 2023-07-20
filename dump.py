import os
import biothings, config

biothings.config_for_app(config)

import biothings.hub.dataload.dumper

import datetime

class DataverseDumper(biothings.hub.dataload.dumper.DummyDumper):
    SRC_NAME = "dataverses"
    SRC_ROOT_FOLDER = os.path.join(config.DATA_ARCHIVE_ROOT, SRC_NAME)

    __metadata__ = {
        "src_meta": {
            "author": {
                "name": "Julia Mullen",
                "url":  "https://github.com/juliamullen"
                },
            "code": {
                "branch": "master",
                "repo": "https://github.com/outbreak-info/dataverses"
                },
            "url": "https://dataverse.org/",
            "license": "https://dataverse.org/best-practices/harvard-dataverse-general-terms-use",
        }
    }

    # SCHEDULE = "40 6 * * *"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_release()

    def set_release(self):
        self.release = datetime.datetime.now().strftime('%Y-%m-%d-%H:%M')
