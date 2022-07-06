import csv
import os
import datetime

import click
import requests
from tabulate import tabulate
from lxml import etree

from settings import OBLIGATION_CODE_MAP, URL_MAP, FEATURE_TYPES_MAP, \
                     STANDARD_NS

from cdr_utils import get_envelopes_rest, create_envelope, activate_envelope, \
                     delete_envelope, upload_file, get_envelope_by_url, \
                     get_history, start_envelope_qa, extract_obligation, \
                     extract_filename, download_file


def get_multiple_countries_rest(countries, obligation_number, reporting_year,
                                repo, eionet_login, released):
    """ Wrapper function to extract from CDR Rest API metadata related to
        multiple countries for a given reporting year.


    """
    envelopes = []

    for c in countries:
        res = get_envelopes_rest(obligation_number,
                                 repo=repo,
                                 eionet_login=eionet_login,
                                 secure=True,
                                 is_released=released,
                                 country_code=c,
                                 reporting_date_start=None,
                                 convert_dates=True)
        envelopes.extend(res)

    # Filter by reportingYear
    if reporting_year:
        envelopes = [t for t in envelopes
                     if t['periodStartYear'] == reporting_year]

    return envelopes


def get_envelopes_db_data(env_file, extract=None):
    """ Load envelopes info from csv  file

       Parameters:
       env_file (file): file to read
       extract: ???
    """

    envelopes = []
    reader = csv.DictReader(env_file)

    for row in reader:
        if extract is not None:
            envelopes.append(row[extract])
        else:
            envelopes.append(row)

    return envelopes


def parse_ns(source_file, STANDARD_NS):
    """
    Parse namespace declarations in source_file and
    map namespaces to standard ones contained in STANDARD_NS
    based on their uri

    returns
    - Dictionary of standard namespace values and corresponding
      namespaces declared in source_file

    """

    ns_map = {}
    context = etree.iterparse(source_file,
                              remove_blank_text=True,
                              events=('start-ns',))

    for event, elem in context:
        prefix, namespace = elem
        ns_map[prefix] = namespace
        rev_map = dict(map(reversed, ns_map.items()))
        ns_map = {STANDARD_NS[k]: k for k, v
                  in rev_map.items() if k in STANDARD_NS}

    return ns_map


def extract_identifiers(in_file, identifier_map):

    ns_map = parse_ns(in_file, STANDARD_NS)
    root = etree.parse(in_file)
    res = {}
    for feat_name, feat_xpath in identifier_map.items():
        ids = root.xpath(feat_xpath, namespaces=ns_map)
        res[feat_name] = ids
    return res


# Tools to interact with CDR infrastructure via command line

@click.group()
def main():
    pass


@main.command()
@click.option('--country_code', '-c', default=None, type=str,
              help='Countries to include.', multiple=True)
@click.option('--cdr_user', prompt=True, hide_input=False,
              help='Login user to cdr reporting_year.')
@click.option('--cdr_pwd', prompt=True, hide_input=True,
              help='Password to cdr repo.')
@click.option('--latest/--all', default=True,
              help='Process only last envelope')
@click.option('--released/--draft', default=True,
              help='Process only released envelopes')
@click.option('--repo', '-r',
              type=click.Choice(URL_MAP.keys(), case_sensitive=False),
              default='CDR', required=True)
@click.option('--obligation', '-o',
              type=click.Choice(OBLIGATION_CODE_MAP.keys(),
                                case_sensitive=False),
              required=True)
@click.option("--year", "-y", type=int)
@click.argument("out", type=click.File('w'), default=None, required=False)
def list_files(country_code, cdr_user, cdr_pwd, latest, released,
               repo, obligation, year, out):

    """List a set of files in envelopes from a repo (e.g CDR or CDRTEST) for
       a given obligation, reporting year, country code.

       All the main properties of the envelope such as url,
       reporting period etc. are copied and repeated for each file found
       in the envelope.
       If required, a comma separated text file is produced as output. 

       The file contains:
       - obligation code
       - obligation number
       - country
       - year
       - envelope url
       - file url
       - uploaded
       - envelope status (latest item)
       - status date

      Parameters:
      countryCode (list): list of 2 character iso code of countries to select
      cdr_user (string): login on cdr platform according to the selected repo
      cdr_pwd  (string): password on selected (repo) cdr platform
      latest (boolean):  if True only selects the latest envelope of the
      selected year
      obligation (string): code of the obligation
      reporting_year (int): year of the reporting - it matches envelope
      periodStartYear value
      out (string): name of a file where the output info is stored for further
      processing if no value is provided by default output will be written to
      the file: output_YYYY-mm-dd_HH_MM_SS.csv where Y,m,d,H,M,S correspond to
      the current timestamp at the time opf file generation


      CLI Example:
      copy all the files in AQ dataflow H envelopes reported for 2017 by Italy
      and Spain and generate out.txt file

      python cdr_tools.py list-files -c it -c es aqd:h 2017 out.txt

    """
    latest_str = 'latest' if latest else ''
    click.echo(f"Extracting {latest_str} files and envelopes metadata "
               f"from {repo} for obligation: {obligation}, "
               f"reporting year: {year if year is not None else 'all'}, "
               f"countries: {country_code if len(country_code)>0 else 'all'}")

    eionet_login = (cdr_user, cdr_pwd)

    obligation_number = OBLIGATION_CODE_MAP[obligation][0]
    envelopes = get_envelopes_rest(obligation_number,
                                   repo=repo,
                                   eionet_login=eionet_login,
                                   is_released=released,
                                   reporting_year=year,
                                   latest=latest)
    results = []

    click.echo(f"Found {len(envelopes)} envelopes")

    if len(envelopes) == 0:
        exit(0)

    # Process each envelope
    for idx, env in enumerate(envelopes):
        click.echo(f"Processing envelope {idx + 1}"
                   f" of {len(envelopes)} {env['url']}")
        files = env["files"]
        if len(files) == 0:
            click.echo("No files in the envelope.")
            continue

        for f in files:
            # Skip non xml files
            if f['contentType'] != 'text/xml' or ('.shp.' in f['url']):
                continue
            results.append({'ObligationCode': obligation,
                            'Obligation': obligation_number,
                            'Country': env['countryCode'],
                            'ReportingYear': env['periodStartYear'],
                            'envelope': env['url'],
                            'file': f['url'],
                            'uploaded': f['uploadDate'],
                            'status': env['status'],
                            'statusDate': env['statusDate']})

    click.echo(tabulate(results))

    # Write output file
    fieldnames = results[0].keys()
    if not out:
        out =  "envelopes_{}_{}.csv".format(obligation.replace(':', '-'),
                                            datetime.datetime.now().
                                            strftime("%Y-%m-%d_%H_%M_%S"))
        out = open(f"./{out}", "w")

    writer = csv.DictWriter(out, fieldnames=fieldnames)

    writer.writeheader()
    [writer.writerow(t) for t in results]


@main.command()
@click.option('--country_code', '-c', default=None, type=str,
              help='Countries to include.', multiple=True)
@click.option('--cdrtest_user', prompt=True, hide_input=False,
              help='Login user to cdr test.')
@click.option('--cdrtest_pwd', prompt=True, hide_input=True,
              help='Password to cdr test.')
@click.option('--latest/--all', default=True,
              help='Process only last envelope')
@click.option('--released/--draft', default=True,
              help='Process only released envelopes')
@click.option('--obligation', '-o',
              type=click.Choice(OBLIGATION_CODE_MAP.keys(),
                                case_sensitive=False),
              required=True)
@click.argument("reporting_year", type=int)
@click.argument("out", type=click.File('w'), default='-', required=False)
def clone_cdrtest(country_code, cdrtest_user, cdrtest_pwd, latest, released,
                  obligation, reporting_year, out):
    """Copies a set of envelopes from CDR to CDRTEST for a given obligation,
       reporting year, country code. The copied envelopes contain  all
       the files contained in the original ones and have the same title
       with and additional reference to the orginal envelope url.
       All the main properties of the envelope such as  description,
       reporting period etc. are copied from the original envelopes.
       A comma separated text file is produced as output. The file contains:
       - obligation code
       - obligation number
       - country
       - year
       - source envelope url
       - destination envelope url
       - number of files
       - envelope status (latest item)
       - status date

      Parameters:
      countryCode (list): list of 2 character iso code of countries to select
      cdrtest_user (string): login on cdr_test platform
      cdrtest_pwd  (string): password on cdr_test platform of cdrtest_user
      latest (boolean):  if True copies only the latest envelope of the
      selected year
      obligation (string): code of the obligation
      reporting_year (int): year of the reporting match envelope
      periodStartYear value
      out (string): name of a file where the output info is stored for further
      processing if no value is provided by default output will be written to
      the file: output_YYYY-mm-dd_HH_MM_SS.csv where Y,m,d,H,M,S correspond to
      the current timestamp at the time opf file generation

      copy all the envelopes of AQ dataflow H reported for 2017  for Italy
      and Spain and generate out.txt file
      python copy_envelopes.py -c it -c es 680 2017 out.txt

    """
    # Get the CDR envelopes per obligation
    latest_str = 'latest' if latest else ''
    click.echo(f"Extracting {latest_str} envelopes metadata "
               f"from CDR for obligation: {obligation} "
               f"reporting year: {reporting_year} "
               f"countries: {country_code}")

    eionet_login = (cdrtest_user, cdrtest_pwd)

    obligation_number = OBLIGATION_CODE_MAP[obligation][0]

    envelopes = get_envelopes_rest(obligation_number,
                                   repo='CDR',
                                   country_code=country_code,
                                   is_released=released,
                                   reporting_year=reporting_year,
                                   latest=latest
                                   )
    results = []
    click.echo(f"Found {len(envelopes)} envelopes")

    if len(envelopes) == 0:
        exit(0)

    # Process each envelope
    for idx, env in enumerate(envelopes):
        click.echo(f"Processing envelope {idx + 1}"
                   f" of {len(envelopes)} {env['url']}")

        files = env["files"]

        if len(files) == 0:
            click.echo("No files in the envelope.")
            continue

        # Copy the envelope
        envTitle = f"{env['title']} [copy of {env['url']}]"
        click.echo(f"Creating cloned envelope on CDRTEST {envTitle}")
        result = create_envelope('CDRTEST',
                                 env['countryCode'].lower(),
                                 obligation,
                                 year=env['periodStartYear'],
                                 title=envTitle,
                                 eionet_login=eionet_login)

        if len(result['errors']) > 0:
            click.echo(f"Error during envelope generation {result['errors']}")
            exit(1)

        envelope_url = result['envelopes'][0]['url']

        activate_envelope(envelope_url, eionet_login=eionet_login)

        for iidx, fl in enumerate(files):
            fileName = fl['url'].split('/')[-1]
            click.echo(f"Processing file {iidx + 1} of {len(files)} {fileName}")

            with open("./{}".format(fileName), "wb") as file:
                fr = requests.get(fl['url'])
                file.write(fr.content)

                upload_file(envelope_url,
                            './{}'.format(fileName),
                            eionet_login=eionet_login,
                            )

            os.unlink("./{}".format(fileName))

        click.echo(f"Envelope cloned to {envelope_url}")

        results.append({'Obligation': obligation_number,
                        'Country': env['countryCode'],
                        'ReportingYear': reporting_year,
                        'CDREnvelope': env['url'],
                        'CDRTESTEnvelope': envelope_url,
                        'FileCount': len(files)})

    # Write output file
    fieldnames = results[0].keys()

    if not out:
        out = 'output_{}.csv'.format(datetime.now().
                                     strftime("%Y-%m-%d_(%H_%M_%S"))

    writer = csv.DictWriter(out, fieldnames=fieldnames)

    writer.writeheader()
    [writer.writerow(t) for t in results]


@main.command()
@click.option('--cdrtest_user', prompt=True, hide_input=False,
              help='Login user to cdr test.')
@click.option('--cdrtest_pwd', prompt=True, hide_input=True,
              help='Password to cdr test.')
@click.argument("file", type=click.File('r'), default='-', required=True)
@click.argument("envelope_field", default='CDRTESTEnvelope')
def delete_envelopes(file, envelope_field, cdrtest_user, cdrtest_pwd):
    """Delete in CDRTEST the envelopes listed in the csv input file under
       envelope_field field. The file could be the same generated by the
       clone command.

      Parameters:
      cdrtest_user (string): login on cdr_test platform
      cdrtest_pwd  (string): password on cdr_test platform of cdrtest_user
      file (file or string) : csv file containing the list of envelopes to
                              delete
    """

    click.echo(f"Deleting envelopes in CDRTEST listed in file {file.name}")
    eionet_login = (cdrtest_user, cdrtest_pwd)

    envelopes = get_envelopes_db_data(file)
    click.echo(f"Found {len(envelopes)} envelopes")

    if len(envelopes) == 0:
        exit(0)

    for idx, env in enumerate(envelopes):
        click.echo("Deleting envelope {idx + 1} of {len(envelopes)} "
                   "Country {env['Country']} Year {env['ReportingYear']} "
                   " at {env[envelope_field]}")

        if click.confirm('Proceed?'):
            status = delete_envelope(env[envelope_field], eionet_login)
            if status != 200:
                print(status)


@main.command()
@click.option('--country_code', '-c', default=None, type=str,
              help='Countrie to include.', multiple=True)
@click.option('--cdrtest_user', prompt=True, hide_input=False,
              help='Login user to cdr test.')
@click.option('--cdrtest_pwd', prompt=True, hide_input=True,
              help='Password to cdr test.')
@click.option('--modified_after',  default=None,
              type=click.DateTime(formats=["%Y-%m-%d"]),
              help=('Only delete envelopes modified after'
                    ' specified date yyyy-mm-dd.'))
@click.argument("obligation_number", type=int)
@click.argument("reporting_year", type=int)
def batch_delete_envelopes(country_code, cdrtest_user, modified_after,
                           cdrtest_pwd, obligation_number, reporting_year):
    """Delete a set of envelopes from CDRTEST
       for a given obligation, reporting year, country code
       it will promp for confirmation for each envelope

     Parameters:
      countryCode (list): list of 2 character iso code of countries to select
      cdrtest_user (string): login on cdr_test platform
      cdrtest_pwd  (string): password on cdr_test platform of cdrtest_user
      modified_after (datetime): if specified only envelopes modified after the
      specified date will be deleted
      obligation_number (int): numeric code of the obligation
      reporting_year (int): year of the reporting match envelope
      periodStartYear value

       CLI Example:
       delete all the envelopes of AQ dataflow H reported for 2017 for Italy

       python delete_envelopes.py -c=it  680 2017 """

    obligation_number = OBLIGATION_CODE_MAP[obligation][0]

    # Get the CDRTEST envelopes per obligation
    click.echo(f"Deleting CDRTEST envelopes for obligation {obligation} "
               f"country: {country_code} "
               f"reporting year: {reporting_year} ")

    eionet_login = (cdrtest_user, cdrtest_pwd)

    envelopes = get_envelopes_rest(obligation_number,
                                   repo='CDRTEST',
                                   country_code=country_code,
                                   reporting_year=reporting_year
                                   )

    click.echo("Found {} envelopes".format(len(envelopes)))

    # Filter by modifiedDate
    if modified_after:
        click.echo("Filtering by modification date")
        envelopes = [t for t in envelopes if t['statusDate'] > modified_after]

        click.echo("Left {} envelopes".format(len(envelopes)))

    if len(envelopes) == 0:
        exit(0)

    # Process each envelope
    for idx, env in enumerate(envelopes):
        click.echo((f"Deleting envelope {idx + 1} of {envelopes}"
                    f" Country {country_code} Year {reporting_year}"
                    f" Modified {env['statusDate']} {env['url']}"))
        if click.confirm('Proceed?'):
            status = delete_envelope(env['url'], eionet_login)
            if status != 200:
                print(status)


@main.command()
@click.option('--cdrtest_user', prompt=True, hide_input=False,
              help='Login user to cdr test.')
@click.option('--cdrtest_pwd', prompt=True, hide_input=True,
              help='Password to cdr test.')
@click.argument("file", type=click.File('r'), required=True)
@click.argument("envelope_field", default='CDRTESTEnvelope')
@click.argument("out", type=click.File('w'), default='-')
def envelope_qa(file, envelope_field, cdrtest_user, cdrtest_pwd, out):
    """
    Extracts the QA feedbacks for the envelope urls specified in the FILE csv
    file in column ENVELOPE_FIELD and save them to OUT csv file

    cdrtest_user (string): login on cdr_test platform
    cdrtest_pwd  (string): password on cdr_test platform of cdrtest_user
    file (file): csv file containing the data urls of the envelopes to process
    envelope_field (string): field in the csv file storing the url of
                             the envelopes to process
    out (file): file where the results of the QA processing will be stored

    """
    eionet_login = (cdrtest_user, cdrtest_pwd)

    envelopes = get_envelopes_db_data(file, extrac=envelope_field)

    feedbacks = []

    for iidx, envelope_url in enumerate(envelopes):
        click.echo((f"Processing envelope {iidx + 1} of {len(envelopes)}"
                    f"{envelope_url}"))
        res = get_feedbacks(envelope_url, eionet_login)

        country = res["countryCode"]
        obligation = res["obligations"][0]
        reporting_year = res["periodStartYear"]

        if len(res['feedbacks']) == 0:
            click.echo((f"No feedback found for {obligation}"
                        f" {country} {reporting_year}"))
            continue

        for idx, v in enumerate(res['feedbacks']):
            if v['activityId'] == 'AutomaticQA' or 1 == 1:

                feeback_status = v['feedbackStatus']
                title = v['title']

            feedback_type = title.split(':')[-1].strip()
            print('Feedback {} {}'.format(idx+1, title))
            errors = []

            for iiidx, a in enumerate(v['attachments']):
                attachment = get_feedback_attachments(a['url'], eionet_login)
                parser = etree.HTMLParser()
                tree = etree.fromstring(attachment.content, parser)
                rows = tree.xpath("//td[@class='bullet']/ancestor::"
                                  "*[position()=1]")

                print(f'Attachment {iiidx + 1} - Found {len(rows)} rows')

                for r in rows:
                    err_levl = r.xpath("./td[@class='bullet']/div/@class")
                    err_code = r.xpath("./td[@class='bullet']/div/a/text()")
                    err_mesg = r.xpath("./td/span[@class='largeText']/text()")
                    if len(err_mesg) == 0:
                        err_mesg = ['']

                    if len(err_code) > 0:
                        errors.append((err_code[0], err_levl[0], err_mesg[0]))
                new_feedback = {'Country': country,
                                'ObligationNumber': obligation,
                                'Envelope': envelope_url,
                                'FeedbackMessage': v['feedbackMessage'],
                                'FeedbackStatus': feeback_status,
                                'ReportingYear': reporting_year,
                                'ManualFeedback': v['title'],
                                'PostingDate': v['postingDate'],
                                'Errors': errors}
            feedbacks.append(new_feedback)

    processed_feedbacks = []
    for v in feedbacks:

        for e in v['Errors']:
            new_rec = v.copy()
            new_rec.pop('Errors')
            new_rec['ErrorCode'] = e[0].strip('\r\n')
            new_rec['ErrorLevel'] = e[1].strip('\r\n')
            new_rec['ErrorMessage'] = e[2].strip('\r\n')

            processed_feedbacks.append(new_rec)

    if len(processed_feedbacks) == 0:
        click.echo("No output file produced")
        exit(0)

    if not out:
        out = f'output_{datetime.now().strftime("%Y-%m-%d_(%H_%M_%S")}.csv'

    fieldnames = processed_feedbacks[0].keys()

    writer = csv.DictWriter(out, fieldnames=fieldnames,  delimiter=';')

    writer.writeheader()
    [writer.writerow(t) for t in processed_feedbacks]


@main.command()
@click.option('--cdrtest_user', prompt=True, hide_input=False,
              help='Login user to cdr test.')
@click.option('--cdrtest_pwd', prompt=True, hide_input=True,
              help='Password to cdr test.')
@click.option('--max_activations', '-m', type=int, default=3,
              help='Maximum number of concurrent active qa checks allowed.')
@click.option('--qa_after', '-a', default=None,
              type=click.DateTime(formats=["%Y-%m-%d"]),
              help='Only run QA if latest was done before date yyyy-mm-dd.')
@click.argument("file", type=click.File('r'), default='-', required=True)
@click.argument("envelope_field", default=None, required=True)
def activate_qa(file, envelope_field, cdrtest_user, cdrtest_pwd,
                max_activations, qa_after):
    """
    Activates the QA  for the envelope urls specified in the FILE csv file
    in column ENVELOPE_FIELD
    Activates up to max active envelopes in QA at once
    """
    eionet_login = (cdrtest_user, cdrtest_pwd)

    envelopes = []
    reader = csv.DictReader(file)
    for row in reader:
        envelopes.append(row[envelope_field])

    cnt = 0
    for iidx, envelope_url in enumerate(envelopes):

        if cnt == max_activations:
            click.echo(f"Reached maximum number of activations "
                       f"{max_activations}")
            exit(0)

        res = get_envelope_by_url(envelope_url, eionet_login)
        hst = get_history(envelope_url, eionet_login)
        res['history'] = hst
        country = res["countryCode"]
        obligation = res["obligations"][0]
        reporting_year = res["periodStartYear"]
        current_wi = res['history'][-1]

        click.echo((f"Envelope {iidx + 1} of {len(envelopes)} {obligation}"
                    f" {country} {reporting_year} {envelope_url}"
                    f" activity: {current_wi['activity_id']}"
                    f" status: {current_wi['activity_status']}"))

        # Skip envelopes in QA
        if ((current_wi['activity_id'] == 'AutomaticQA' and
             current_wi['activity_status'] == "active") or
           (current_wi['activity_id'] == 'DeleteAutomaticQAFeedback')):
            modified = current_wi['modified']
            click.echo((f"Skipping envelope {iidx + 1} of {len(envelopes)}"
                        f" {obligation} {country} {reporting_year} "
                        f"{envelope_url}"
                        f" already running QA since {modified}"))
            cnt += 1
            continue

# TODO skip QA for envelopes already completed after a certain date time
# Find most recent QA
        if qa_after is not None:
            most_recent_qa = datetime.datetime(1900, 1, 1)
            for status in res['history']:
                if (status['activity_id'] == 'AutomaticQA' and
                   status['activity_status'] == 'complete'):

                    if most_recent_qa is None:
                        most_recent_qa = status['modified']
                    else:
                        if most_recent_qa < status['modified']:
                            most_recent_qa = status['modified']

            if qa_after <= most_recent_qa:
                click.echo(f"Skipping envelope because already run qa on "
                           f"{most_recent_qa.strftime('%d-%m-%Y')}")
                continue

        click.echo(f"Activating QA for envelope {iidx + 1} of {len(envelopes)}"
                   f" {obligation} {country} {reporting_year} {envelope_url}")

        if (current_wi['activity_id'] == 'Draft' and
             current_wi['activity_status'] == "inactive"):  # noqa: E127
            click.echo("Activating Draft")

        activate_envelope(envelope_url, eionet_login, current_wi['id'])
        start_envelope_qa(envelope_url, eionet_login, current_wi['id'])
        cnt += 1


@main.command()
@click.option('--cdr_user', prompt=True, hide_input=False,
              help='Login user to cdr.')
@click.option('--cdr_pwd', prompt=True, hide_input=True,
              help='Password to cdr.')
@click.argument("left_url", required=True)
@click.argument("right_url", required=True)
def diff_ids(left_url, right_url, cdr_user, cdr_pwd,):
    """  Reports the differences in the identifiers found in two XML files
         returns the total number of unique identifiers by feature type for
         each file, the identifiers found in the left file but not in the
         right one the identifier found in the right file but not in
         the left one
    """

    eionet_login = (cdr_user, cdr_pwd)

    obligation_left = extract_obligation(left_url)
    obligation_right = extract_obligation(left_url)

    if (obligation_left != obligation_right):
        click.echo((f'Left file obligation {obligation_left}'
                    f'does not match right file one {obligation_right}'))
        exit(0)

    obligation = obligation_left

    if obligation not in FEATURE_TYPES_MAP:
        click.echo((f'Obligation {obligation} identifiers tag'
                    f'not mapped in Settings'))
        exit(0)

    filename_l = extract_filename(left_url)
    click.echo(f'Downloading left file {filename_l} url {left_url}')
    download_file(left_url, '.', filename_l, eionet_login)

    left_identifiers = extract_identifiers("./{}".format(filename_l),
                                           FEATURE_TYPES_MAP[obligation])
    os.unlink("./{}".format(filename_l))

    filename_r = extract_filename(right_url)
    click.echo(f'Downloading right file {filename_r} url {right_url}')
    download_file(right_url, '.', filename_r, eionet_login)

    right_identifiers = extract_identifiers(filename_r,
                                            FEATURE_TYPES_MAP[obligation])
    os.unlink("./{}".format(filename_r))

    for feature_type, _ in FEATURE_TYPES_MAP[obligation].items():
        click.echo(f'**Feature: {feature_type} unique identifiers left: {len(left_identifiers[feature_type])} right: {len(right_identifiers[feature_type])} `')
        lid = set(left_identifiers[feature_type])
        rid = set(right_identifiers[feature_type])

        left_not_in_right = lid - rid
        right_not_in_left = rid - lid

        if len(left_not_in_right) > 0:
            tmp = "\n".join(sorted(left_not_in_right))
            click.echo(f'*Identifiers in left not found in right ({len(left_not_in_right)}):\n{tmp}')

        if len(right_not_in_left) > 0:
            tmp = "\n".join(sorted(right_not_in_left))
            click.echo(f'*Identifiers in right not found in left ({len(right_not_in_left)}):\n{tmp}')

        if (len(right_not_in_left) == 0) and (len(left_not_in_right) == 0):
            click.echo(f'Complete Match')


if __name__ == "__main__":
    main()
