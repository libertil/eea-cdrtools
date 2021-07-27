import pathlib
import hashlib
import json
import datetime
import requests
import io

from requests.auth import HTTPBasicAuth


BASE_CDR_URL = 'cdr.eionet.europa.eu'
BASE_BDR_URL = 'bdr.eionet.europa.eu'
BASE_CDRTEST_URL = 'cdrtest.eionet.europa.eu'
BASE_CDRSANDBOX_URL = 'cdrsandbox.eionet.europa.eu'

URL_MAP = {'CDR':BASE_CDR_URL,
           'BDR':BASE_BDR_URL,
           'CDRTEST': BASE_CDRTEST_URL,
           'CDRSANDBOX': BASE_CDRSANDBOX_URL}


ENVELOPES_DATE_FIELDS = ['modifiedDate', 'reportingDate', 'statusDate']
FILES_DATE_FIELDS = ['uploadDate']
HISTORY_DATE_FIELDS =['modified']

DELETE_ACTION = 'delete'

OBLIGATION_CODE = {680: 'eu/aqd/h',
                   681: 'eu/aqd/i',
                   682: 'eu/aqd/j',
                   683: 'eu/aqd/k',
                   }


def build_url(repo, eionet_login, secure):
    """
    Build a URL base url for a Rest or RPC call on CDR api
    repo: either CDR, CDRTEST or CDRSANDBOX
    eionet_login:   tuple of eionet login and password or None
    secure: boolean if True https url will be returned NB is a login 
    is provided then https will be used regardless of secure

    returns: a url to the right CDR api root    

    """

    # Avoid sending login info over non-secure http connection
    if secure or eionet_login: 
        scheme = 'https:'
    else:
        scheme = 'http:'

    if eionet_login:
        url = '{}//{}:{}@{}'.format(scheme,
                            eionet_login[0],
                            eionet_login[1],
                            URL_MAP[repo])
    else:
        url = '{}//{}'.format(scheme, URL_MAP[repo])
    return url


def build_rest_query(base_url,
                     obligation,
                     country_code=None,
                     is_released=None,
                     reporting_date_start=None,
                     fields=None,
                ):
    """
    Builds a query to the CDR rest API for a specific obligation code.
    Optional query parameters are added to the query id specfied. 
    """ 

    url = "{}/api/envelopes?obligations={}".format(base_url, obligation)
    
    if country_code:
      url = "{}&countryCode={}".format(url, country_code)
    
    if is_released is not None:
      url = "{}&isReleased={}".format(url, int(is_released))
          
    if reporting_date_start:
      url = "{}&reportingDateStart={}".format(url, reporting_date_start)

    if fields:
      url = "{}&fields={}".format(url, fields)
         
    return url

    
def download_file(url, dest_path, filename, eionet_login=None):
    """
    Downloads a file from a given url on CDR saving it as filename at the dest_path
    if access to the envelope is restricted specify eionet_login
    Returns the SHA256 hash signature of the file
    """

    req = requests.get(url, auth=eionet_login, stream=True)
    req.raise_for_status()
    pathlib.Path(dest_path).mkdir(parents=True, exist_ok=True)
    dest_file = pathlib.Path(dest_path).joinpath(filename)
    handle = open(dest_file, "wb")
    # Write file and calculate SHA256 hash
    sha256_hash = hashlib.sha256()
    for chunk in req.iter_content(chunk_size=512):
        if chunk:  # filter out keep-alive new chunks
            handle.write(chunk)
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


def get_envelopes_rest(obligation,
                       repo='CDR',
                       eionet_login=None, 
                       secure=True,
                       is_released=None,
                       country_code=None,
                       reporting_date_start=None,
                       convert_dates=True,
                       fields=["url","title","description","countryCode","isReleased","reportingDate",
                               "modifiedDate","periodStartYear","periodEndYear","periodDescription",
                               "isBlockedByQCError","status","statusDate","creator","hasUnknownQC",
                               "files","obligations"]
                       ):
    
    base_url =  build_url(repo, eionet_login, secure)
    
    url = build_rest_query(base_url, obligation,
                           is_released=is_released,
                           country_code= country_code,
                           reporting_date_start = reporting_date_start,
                           fields = ",".join(fields))
    #print("Issuing get request on API")
    #print(url)
    r = requests.get(url, auth=eionet_login)

    #print("Processing results")
    
    envelopes = r.json()['envelopes']
    

    # Convert date
    if convert_dates:
        convert_date_fields(envelopes, ENVELOPES_DATE_FIELDS)
       
    return envelopes

def get_envelope_by_url(envelope_url, 
                        eionet_login=None,
                        convert_dates=True,
                        repo = 'CDR',
                        fields=["url","title","description","countryCode","isReleased","reportingDate",
                               "modifiedDate","periodStartYear","periodEndYear","periodDescription",
                               "isBlockedByQCError","status","statusDate","creator","hasUnknownQC",
                               "files","obligations"]):

    base_url =  build_url(repo, eionet_login, True)
    
    url = "{}/api/envelopes?url={}".format(base_url, envelope_url)
    
    url = "{}&fields={}".format(url, ",".join(fields))
    
    #print("Issuing get request on API")
    #print(url)
    r = requests.get(url, auth=eionet_login)

    #print("Processing results")
    
    envelopes = r.json()['envelopes']
    #print(envelopes)
    # Convert date
    if convert_dates:
        convert_date_fields(envelopes, ENVELOPES_DATE_FIELDS)
       
    return envelopes


def convert_date_fields(items, date_fields):
    for df in date_fields:  
        for it in items:
            if it[df] != '' and it[df] is not None:
                it[df] = datetime.datetime.strptime(it[df],'%Y-%m-%dT%H:%M:%SZ')
            else:
                it[df] = None
    

def create_envelope(repo,
                    country_code,
                    obligation_number,
                    title = '',
                    descr = '',
                    year = '',
                    endYear = '',
                    partofyear = '',
                    locality = '',
                    eionet_login = None):

    cdr_user, cdr_pwd = eionet_login
    
    envelope_info ={}
    base_url =  build_url(repo, None, True)
    request_url = '{}/{}/{}/manage_addEnvelope'.format(base_url, country_code, OBLIGATION_CODE[obligation_number])
    data = {'title': title,
            'descr': descr,
            'year': year,
            'endyear': endYear,
            'partofyear': partofyear,
            'locality': locality} 

    session = requests.Session()
    session.auth = (cdr_user, cdr_pwd)
    headers = {'Accept': 'application/json'}
    response = session.post(request_url, data=data, headers=headers) 
    print(response)
    if response.status_code == 201:
        return response.json()
    else:
      return {'errors': ['http response {}'.format(response.status_code)]}


def delete_envelope(envelope_url, 
                     eionet_login = None):
    cdr_login, cdr_pwd = eionet_login
    
    base_url  = '/'.join(envelope_url.split('/')[0:-1])+'/'
    env_code  = envelope_url.split('/')[-1]
    print(cdr_login, cdr_pwd, envelope_url,base_url, env_code)
    data = {'ids:list':env_code, 'manage_delObjects:method':'Delete' }
    
    session = requests.Session()
    session.auth = (cdr_login, cdr_pwd)
    response = session.post(base_url, data=data)

    return response.status_code 

    
def activate_envelope(envelope_url, 
                      eionet_login = None,
                      workitem_id =0):
    """
    Activate the envelope
    """

    cdr_login, cdr_pwd = eionet_login

    request_url = '{}/activateWorkitem?workitem_id={}&DestinationURL={}'.format(envelope_url, workitem_id, envelope_url)
    #print(request_url)
    auth = HTTPBasicAuth(cdr_login, cdr_pwd)
    
    response = requests.get(request_url, auth=auth)

    return response


def start_envelope_qa(envelope_url,
                      eionet_login,
                      workitem_id):
    cdr_login, cdr_pwd = eionet_login

    #request_url = '{}/completeWorkitem?workitem_id={}&release_and_finish=0&DestinationURL={}'.format(envelope_url, envelope_url, workitem_id)
    request_url = '{}/completeWorkitem?workitem_id={}&release_and_finish=0&DestinationURL={}'.format(envelope_url, workitem_id,envelope_url)
    auth = HTTPBasicAuth(cdr_login, cdr_pwd)
    
    response = requests.get(request_url, auth=auth)  
    
    return response
    
    
def upload_file(envelope_url,
                file_path, 
                eionet_login = None):

    cdr_user, cdr_pwd = eionet_login

    request_url = '{}/{}'.format(envelope_url,'manage_addDocument')

    files = {'file': open(file_path, 'rb')}
    auth = HTTPBasicAuth(cdr_user, cdr_pwd)
    response = requests.post(request_url,files=files, auth=auth) 

#curl "https://cdrtest.eionet.europa.eu/api/envelopes?url=https://cdrtest.eionet.europa.eu/ro/colwydrga/envxde78w&fields=feedbacks"
def get_feedbacks(envelope_url,
                  eionet_login = None):
    """
    Extract the feedbacks from the speciafied envelope
    """
    
    cdr_login, cdr_pwd = eionet_login
    
    url_parts = requests.utils.urlparse(envelope_url)

    base_url = "{}://{}".format(url_parts.scheme, url_parts.netloc)
    #print(base_url)
    
    request_url = '{}/api/envelopes?url={}&fields=feedbacks,countryCode,periodStartYear,obligations'.format(base_url, envelope_url)
    #print(request_url)
    auth = HTTPBasicAuth(cdr_login, cdr_pwd)
    
    response = requests.get(request_url, auth=auth)  

    if response.status_code == 200:
        return response.json()['envelopes'][0]
    else:
      return {'errors': ['http response {}'.format(response.status_code)]}


def get_feedback_attachments(attachment_url,
                             eionet_login = None):

    cdr_login, cdr_pwd = eionet_login

    auth = HTTPBasicAuth(cdr_login, cdr_pwd)
    
    response = requests.get(attachment_url, auth=auth) 

    return response

def get_current_workitem(envelope_url,
                      eionet_login = None):
    """
    Return most recent element of the history of the envelope
    """
    
    cdr_login, cdr_pwd = eionet_login

    request_url = '{}/get_current_workitem'.format(envelope_url)
    #print(request_url)
    auth = HTTPBasicAuth(cdr_login, cdr_pwd)
    
    response = requests.get(request_url, auth=auth)

    if response.status_code == 200:
        return response.json()
    else:
      return {'errors': ['http response {}'.format(response.status_code)]}

def get_history(envelope_url,
                eionet_login = None):
    """
    Extract the history from the speciafied envelope
    """
    
    cdr_login, cdr_pwd = eionet_login
    
    url_parts = requests.utils.urlparse(envelope_url)

    base_url = "{}://{}".format(url_parts.scheme, url_parts.netloc)
    #print(base_url)
    
    request_url = '{}/api/envelopes?url={}&fields=history,countryCode,periodStartYear,obligations'.format(base_url, envelope_url)
    #print(request_url)
    auth = HTTPBasicAuth(cdr_login, cdr_pwd)
    
    response = requests.get(request_url, auth=auth)  

    if response.status_code == 200:
        res = response.json()['envelopes'][0]
        convert_date_fields(res['history'], HISTORY_DATE_FIELDS)
        return res
    else:
      return {'errors': ['http response {}'.format(response.status_code)]}
