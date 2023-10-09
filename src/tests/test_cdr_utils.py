import cdr_utils
import pytest


@pytest.fixture
def cdr_envelope_a_url():
    return "https://cdr.eionet.europa.eu/es/eu/aqd/d/envxfin7q"


@pytest.fixture
def cdr_envelope_a_feedbacks_api_meta():
    return [
                {
                    "contentType": "text/html;charset=UTF-8",
                    "attachments": [],
                    "feedbackMessage": "No envelope-level errors found",
                    "documentId": "xml",
                    "feedbackStatus": "INFO",
                    "title": "AutomaticQA result for: AirQuality Dataflow D",
                    "url":
                    ("https://cdr.eionet.europa.eu/es/eu/aqd/d/envxfin7q"
                     "/AutomaticQA_652620"),
                    "postingDate": "2019-12-17T21:10:01Z",
                    "activityId": "AutomaticQA",
                    "isRestricted": 1,
                    "automatic": 1
                },
                {
                    "contentType": "text/html",
                    "attachments": [],
                    "feedbackMessage": "",
                    "documentId": None,
                    "feedbackStatus": "",
                    "title": "Confirmation of receipt",
                    "url":
                    ("https://cdr.eionet.europa.eu/es/eu/aqd/d/envxfin7q"
                     "/feedback1576577005"),
                    "postingDate": "2019-12-17T21:22:02Z",
                    "activityId": "",
                    "isRestricted": 0,
                    "automatic": 1
                }
            ]


@pytest.fixture
def cdr_envelope_a_api_meta(cdr_envelope_a_feedbacks_api_meta):
    return {"errors":   [],
            "envelopes":  [
            {"periodEndYear": 2019,
             "description": "Test description",
             "countryCode": "ES",
             "title": "2019",
             "obligations": ["672"],
             "reportingDate": "2019-12-17T21:22:02Z",
             "url": "https://cdr.eionet.europa.eu/es/eu/aqd/d/envxfin7q",
             "modifiedDate": "2019-12-17T21:22:03Z",
             "periodDescription": "Not applicable",
             "isReleased": 1,
             "periodStartYear": 2019,
             "feedbacks": cdr_envelope_a_feedbacks_api_meta
             }]
            }


@pytest.fixture
def cdrtest_envelope_a_url():
    return "http://cdrtest.eionet.europa.eu/es/eu/aqd/d/envxfin7q"


def test_build_url_insecure_no_auth():
    url = cdr_utils.build_url('CDR', None, False)
    assert url == 'http://cdr.eionet.europa.eu', 'test failed'


def test_build_url_secure_no_auth():
    url = cdr_utils.build_url('CDRTEST', None, True)
    assert url == 'https://cdrtest.eionet.europa.eu', 'test failed'


def test_build_url_auth():
    url = cdr_utils.build_url('CDRSANDBOX', ('user', 'pwd'), True)
    assert url == 'https://user:pwd@cdrsandbox.eionet.europa.eu', 'test failed'

    url = cdr_utils.build_url('CDRTEST', ('user', 'pwd'), False)
    assert url == 'https://user:pwd@cdrtest.eionet.europa.eu', 'test failed'


def test_extract_base_url_cdr(cdr_envelope_a_url):
    url = cdr_envelope_a_url
    base_url = cdr_utils.extract_base_url(url)
    assert base_url == 'https://cdr.eionet.europa.eu', 'test failed'


def test_extract_base_url_cdrtest(cdrtest_envelope_a_url):
    url = cdrtest_envelope_a_url
    base_url = cdr_utils.extract_base_url(url)
    assert base_url == 'http://cdrtest.eionet.europa.eu', 'test failed'


def test_convert_dates():
    pass


# def test_get_envelope_by_url(requests_mock,
#                              cdr_envelope_a_url,
#                              cdr_envelope_a_api_meta):
#     envelope_url = cdr_envelope_a_url

#     url = f"https://cdr.eionet.europa.eu/api/envelopes?url={envelope_url}"
#     response = cdr_envelope_a_api_meta

#     requests_mock.get(url, json=response)

#     result = cdr_utils.get_envelope_by_url(envelope_url,
#                                            eionet_login=None,
#                                            convert_dates=False)

#     envelope = response["envelopes"][0]

#     assert result["periodEndYear"] == envelope["periodEndYear"]

#     assert result["description"] == envelope["description"]

#     assert result["countryCode"] == envelope["countryCode"]

#     assert result["title"] == envelope["title"]

#     assert result["reportingDate"] == envelope["reportingDate"]

#     assert result["modifiedDate"] == envelope["modifiedDate"]

#     assert result["periodDescription"] == envelope["periodDescription"]

#     assert result["isReleased"] == envelope["isReleased"]

#     assert result["periodStartYear"] == envelope["periodStartYear"]
