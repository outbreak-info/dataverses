import os
import biothings, config

biothings.config_for_app(config)

import biothings.hub.dataload.dumper

class DataverseDumper(biothings.hub.dataload.dumper.DummyDumper):
    SRC_NAME = "covid_dataverse"
    SRC_ROOT_FOLDER = os.path.join(config.DATA_ARCHIVE_ROOT, SRC_NAME)

    SCHEDULE = None
