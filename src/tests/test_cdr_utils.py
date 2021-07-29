import cdr_utils
import pytest

@pytest.fixture
def cdr_envelope_a_url():
    return "https://cdr.eionet.europa.eu/es/eu/aqd/d/envyog3aq"


@pytest.fixture
def cdr_envelope_a_api_meta():
    return {"errors":   [],
            "envelopes":  [
            {"periodEndYear": "", 
             "description": "", 
             "countryCode": "ES", 
             "title": "2020_v2", 
             "obligations": ["672"], 
             "reportingDate": "2021-07-23T07:54:03Z", 
             "url": "https://cdr.eionet.europa.eu/es/eu/aqd/d/envyog3aq",
             "modifiedDate": "2021-07-23T07:54:10Z", 
             "periodDescription": "Not applicable", 
             "isReleased": 1, 
             "periodStartYear": 2020
             }]
            }


@pytest.fixture
def cdrtest_envelope_a_url():
    return "http://cdrtest.eionet.europa.eu/es/eu/aqd/d/envyog3aq"


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


def test_get_envelope_by_url(requests_mock, 
                             cdr_envelope_a_url, 
                             cdr_envelope_a_api_meta):
    envelope_url = cdr_envelope_a_url
    
    url = f"https://cdr.eionet.europa.eu/api/envelopes?url={envelope_url}"
    response = cdr_envelope_a_api_meta

    requests_mock.get(url, json=response)

    result = cdr_utils.get_envelope_by_url(envelope_url, 
                                           eionet_login=None,
                                           convert_dates=False)

    envelope = response["envelopes"][0]

    assert result["periodEndYear"] == envelope["periodEndYear"]

    assert result["description"] == envelope["description"]

    assert result["countryCode"] == envelope["countryCode"]

    assert result["title"] == envelope["title"]

    assert result["reportingDate"] == envelope["reportingDate"]

    assert result["modifiedDate"] == envelope["modifiedDate"]

    assert result["periodDescription"] == envelope["periodDescription"]
    
    assert result["isReleased"] == envelope["isReleased"]

    assert result["periodStartYear"] == envelope["periodStartYear"]
    