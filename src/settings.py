
# endpoints base urls
BASE_CDR_URL = "cdr.eionet.europa.eu"
BASE_BDR_URL = "bdr.eionet.europa.eu"
BASE_CDRTEST_URL = "cdrtest.eionet.europa.eu"
BASE_CDRSANDBOX_URL = "cdrsandbox.eionet.europa.eu"

# endpoints map
URL_MAP = {"CDR": BASE_CDR_URL,
           "BDR": BASE_BDR_URL,
           "CDRTEST": BASE_CDRTEST_URL,
           "CDRSANDBOX": BASE_CDRSANDBOX_URL}


ENVELOPES_DATE_FIELDS = ["modifiedDate", "reportingDate", "statusDate"]
FILES_DATE_FIELDS = ["uploadDate"]
HISTORY_DATE_FIELDS = ["modified"]

DEFAULT_FIELDS = ["url", "title", "description", "countryCode", 
                  "isReleased", "reportingDate", "modifieddate", 
                  "periodStartYear", "periodEndYear", 
                  "perioddescription", "isBlockedByQCError", 
                  "status", "statusDate", "creator", 
                  "hasUnknownQC", "files", "obligations"]

DELETE_ACTION = "delete"

# map of obligation numbers as in rod.eionet.europa.eu vs sub-paths in CDR
OBLIGATION_CODE_MAP = {'aqd:b':     (670, 'eu/aqd/b'), 
                       'aqd:c':     (671, 'eu/aqd/c'),
                       'aqd:d':     (672, 'eu/aqd/d'),
                       'aqd:e1a':   (673, 'eu/aqd/e1a'),
                       'aqd:g':     (679, 'eu/aqd/g'),
                       'aqd:h':     (680, 'eu/aqd/h'),
                       'aqd:i':     (681, 'eu/aqd/i'),
                       'aqd:j':     (682, 'eu/aqd/j'),
                       'aqd:k':     (683, 'eu/aqd/k'),
                       'aqd:b_pre': (693, 'eu/aqd/b_preliminary'),
                       'aqd:c_pre': (694, 'eu/aqd/c_preliminary'),
                       }
