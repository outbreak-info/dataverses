import biothings

import biothings.hub.dataload.dumper

class DataverseDumper(biothings.hub.dataload.dumper.DummyDumper):
    SRC_NAME = "covid_dataverse"
    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, SRC_NAME)

    __metadata__ = {
            "src_meta": {}
    }

    SCHEDULE = None
