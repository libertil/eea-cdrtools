
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
                       'aqd:d1b':   (742, 'eu/aqd/d1b'),
                       'aqd:e1a':   (673, 'eu/aqd/e1a'),
                       'aqd:e1b':   (674, 'eu/aqd/e1b'),
                       'aqd:g':     (679, 'eu/aqd/g'),
                       'aqd:h':     (680, 'eu/aqd/h'),
                       'aqd:i':     (681, 'eu/aqd/i'),
                       'aqd:j':     (682, 'eu/aqd/j'),
                       'aqd:k':     (683, 'eu/aqd/k'),
                       'aqd:b_pre': (693, 'eu/aqd/b_preliminary'),
                       'aqd:c_pre': (694, 'eu/aqd/c_preliminary'),
                       }

# maps the main features in each dataflow by obligation to the identifier tag
FEATURE_TYPES_MAP = {'eu/aqd/b':
                     {'AQ Zone':
                      ('//aqd:AQD_Zone/am:inspireId/base:Identifier/'
                       'base:localId/text()')},
                     'eu/aqd/c':
                     {'AQ AssessmentRegime':
                      ('//aqd:AQD_AssessmentRegime/aqd:inspireId/'
                       'base:Identifier/base:localId/text()')},
                     'eu/aqd/d':
                     {'AQ SamplingPoint':
                      ('//aqd:AQD_SamplingPoint/ef:inspireId/'
                       'base:Identifier/base:localId/text()'),
                      'AQ Station':
                      ('//aqd:AQD_Station/ef:inspireId/'
                       'base:Identifier/base:localId/text()'),
                      'AQ Network':
                      ('//aqd:AQD_Network/ef:inspireId/'
                       'base:Identifier/base:localId/text()'),
                      'AQ Sample':
                      ('//aqd:AQD_Sample/aqd:inspireId/'
                       'base:Identifier/base:localId/text()'),
                      'AQD SamplingPointProcess':
                      ('//aqd:AQD_SamplingPointProcess/ompr:inspireId/'
                       'base:Identifier/base:localId/text()')},
                     'eu/aqd/d1b':
                     {'AQ Model':
                      ('//aqd:aqd:AQD_Model/ef:inspireId/'
                       'base:Identifier/base:localIdtext()'),
                      'AQ ModelArea':
                      ('//aqd:AQD_ModelArea/aqd:inspireId/'
                       'base:Identifier/base:localId/text()'),
                      'AQD ModelProcess':
                      ('//aqd:AQD_ModelProcess/ompr:inspireId/'
                       'base:Identifier/base:localId/text()')},
                     'eu/aqd/g':
                     {'AQ Attainment':
                      ('//aqd:AQD_Attainment/aqd:inspireId/'
                       'base:Identifier/base:localId/text()')}
                     }


STANDARD_NS = {
    'http://dd.eionet.europa.eu/schemaset/id2011850eu-1.0': 'aqd',
    'http://www.w3.org/1999/xlink': 'xlink',
    'http://www.opengis.net/om/2.0': 'om',
    'http://www.opengis.net/swe/2.0': 'swe',
    'http://www.opengis.net/sampling/2.0': 'sam',
    'http://inspire.ec.europa.eu/schemas/base2/1.0': 'base2',
    'http://inspire.ec.europa.eu/schemas/base/3.3':  'base',
    'http://www.isotc211.org/2005/gsr': 'gsr',
    'http://www.isotc211.org/2005/gts': 'gts',
    'http://www.isotc211.org/2005/gss': 'gss',
    'http://inspire.ec.europa.eu/schemas/ompr/2.0': 'ompr',
    'http://inspire.ec.europa.eu/schemas/am/3.0': 'am',
    'http://www.isotc211.org/2005/gco': 'gco',
    'urn:x-inspire:specification:gmlas:Addresses:3.0': 'ad',
    'urn:x-inspire:specification:gmlas:GeographicalNames:3.0': 'gn',
    'http://inspire.ec.europa.eu/schemas/ef/3.0': 'ef',
    'http://www.isotc211.org/2005/gmd': 'gmd',
    'http://www.opengis.net/gml/3.2': 'gml',
    'http://www.opengis.net/samplingSpatial/2.0': 'sams',
    'http://www.w3.org/2001/XMLSchema-instance': 'xsi',
    'http://www.w3.org/2001/XMLSchema': 'xs',
}
