import csv
import os
import datetime

import click
import requests
from lxml import etree

from cdr_utils import * 


def get_multiple_countries_rest(countries, obligation_number, reporting_year, repo, eionet_login, released):
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
    envelopes = [t for t in envelopes if t['periodStartYear'] == reporting_year]
    
    return envelopes



def get_envelopes_db_data(env_file, extract = None):
    """ 
       Load envelopes info from csv file

    """ 

    envelopes = []
    reader = csv.DictReader(env_file)

    for row in reader:
      if extract is not None:
        envelopes.append(row[extract])
      else:
        envelopes.append(row)

    return envelopes

# Tools to interact with CDR infrastructure via command line

@click.group()
def main():
  pass

@main.command()
@click.option('--country_code', '-c', default=None, type=str, help='Countries to include.', multiple=True)
@click.option('--cdrtest_user', prompt=True, hide_input=False, help='Login user to cdr test.')
@click.option('--cdrtest_pwd', prompt=True, hide_input=True, help='Password to cdr test.')
@click.option('--latest/--all', default=True, help='Process only last envelope')
@click.option('--released/--draft', default=True, help='Process only released envelopes')

@click.argument("obligation_number", type=int)
@click.argument("reporting_year", type=int)
@click.argument("out", type=click.File('w'), default='-', required=False)
def  clone_cdrtest(country_code, cdrtest_user, cdrtest_pwd, latest, released, obligation_number, reporting_year, out):
    """Copies a set of envelopes  from CDR to CDRTEST  for a given obligation, reporting year, country code.
       The copied envelopes will contain  all the files contained in the original ones and have the same title 
       but also with a reference to the orginal envelope url. All the main properties of the envelope such as 
       descriptiomn, reporting period etc. will be carried over to the copied envelope.
       An output comma separated text file is produced listing:
       - obligation
       - country
       - year-
       - source envelope url
       - destination envelope url
       - number of files
       - envelope status (latest item) 
       - status date  
       
       Example: copy all the envelopes of AQ dataflow H reported for 2017 for Italy and Spain
       
       python copy_envelopes.py -c it -c es 680 2017 out.txt
    """
    
    # Get the CDR envelopes per obligation
    click.echo("Extracting envelopes metadata from CDR for obligation: {} reporting year: {} ".
                format(obligation_number, reporting_year))
    click.echo("For countries: {}".format(country_code))

    eionet_login = (cdrtest_user, cdrtest_pwd)

    envelopes =  get_multiple_countries_rest(country_code, obligation_number, reporting_year, 'CDR', None, released)
    
    results =[]
    
    click.echo("Found {} envelopes".format(len(envelopes)))

    if len(envelopes) == 0:
    	exit(0)

    # Remove all non-last envelopes if required
    if latest:
      click.echo("Filtering most recently modified envelope for each country")
      latest_envelope = {}
      for env in envelopes:
        c = env['countryCode']
        if c not in latest_envelope:
          latest_envelope[c] = env
        else:
          if env['statusDate']>latest_envelope[c]['statusDate']:
            latest_envelope[c] = env

      envelopes = [v for k,v in latest_envelope.items()]
      click.echo("Left {} envelopes".format(len(envelopes)))
      

    # Process each envelope
    for idx, env in enumerate(envelopes):
        click.echo("Processing envelope {} of {} {}".format(idx + 1, len(envelopes), env['url']))
        files = env["files"]
        if len(files) == 0:
            click.echo("No files in the envelope.")
            continue

        # Copy the envelope
        envTitle = "{} [copy of {}]".format(env['title'],env['url'])
        click.echo("Creating cloned envelope on CDRTEST {}".format(envTitle))
        result = create_envelope('CDRTEST',
                           env['countryCode'].lower(), 
                           obligation_number,
                           year = env['periodStartYear'],
                           title = envTitle,
                           eionet_login = eionet_login)

        if len(result['errors'])>0:
          click.echo("Error during envelope generation {}".format(result['errors']))
          
        envelope_url = result['envelopes'][0]['url']
        
        activate_envelope(envelope_url, eionet_login = eionet_login)

        for iidx, fl in enumerate(files):
            fileName = fl['url'].split('/')[-1]
            click.echo("Processing file {} of {} {}".format(iidx + 1, len(files), fileName))
            

            with open("./{}".format(fileName), "wb") as file:
                fr = requests.get(fl['url'])
                file.write(fr.content)


                upload_file(envelope_url, 
                            eionet_login = eionet_login,
                            file_path='./{}'.format(fileName))

            os.unlink("./{}".format(fileName))
        
        results.append({'Obligation':obligation_number, 
        				'Country': env['countryCode'],
        				'ReportingYear':reporting_year, 
        				'CDREnvelope': env['url'],
        				'CDRTESTEnvelope': envelope_url, 
        				'FileCount': len(files)})
    
    # Write output file
    fieldnames = results[0].keys()
    
    if not out:
      out = 'output_{}.csv'.format(datetime.now().strftime("%Y-%m-%d_(%H_%M_%S"))
      
    writer = csv.DictWriter(out, fieldnames=fieldnames)

    writer.writeheader()
    [writer.writerow(t) for t in results]

@main.command()
@click.option('--cdrtest_user', prompt=True, hide_input=False, help='Login user to cdr test.')
@click.option('--cdrtest_pwd', prompt=True, hide_input=True, help='Password to cdr test.')
@click.argument("file", type=click.File('r'), default='-', required=True)
@click.argument("envelope_field", default='CDRTESTEnvelope')
def delete_envelopes(file, envelope_field, cdrtest_user, cdrtest_pwd):
    """Delete in cdrtest the envelopes listed in the input file under envelope_field field
    """
    
    click.echo("Deleting envelopes in CDRTEST listed in file {}".format(file.name))

    eionet_login = (cdrtest_user, cdrtest_pwd)

    envelopes = get_envelopes_db_data(file)
    click.echo("Found {} envelopes".format(len(envelopes)))

    if len(envelopes) == 0:
      exit(0)
    
    print(envelopes)

    for idx, env in enumerate(envelopes):        
        click.echo("Deleting envelope {} of {} Country {} Year {} at {}".
          format(idx + 1, len(envelopes), env['Country'], env['ReportingYear'], env[envelope_field] ))
        if click.confirm('Proceed?'):
          status = delete_envelope(env[envelope_field],eionet_login)
          if status!=200:
            print(status)




@main.command()
@click.option('--country_code', '-c', default=None, type=str, help='Countrie to include.', multiple=True)
@click.option('--cdrtest_user', prompt=True, hide_input=False, help='Login user to cdr test.')
@click.option('--cdrtest_pwd', prompt=True, hide_input=True, help='Password to cdr test.')
@click.option('--modified_after',  default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help='Only delete envelopes modified after specified date yyyy-mm-dd.')
@click.argument("obligation_number", type=int)
@click.argument("reporting_year", type=int)
def batch_delete_envelopes(country_code, cdrtest_user, modified_after, cdrtest_pwd, obligation_number, reporting_year):
    """Deletes a set of envelopes from CDRTEST 
       for a given obligation, reporting year, country code 
       
       Example: copy all the envelopes of AQ dataflow H reported for 2017 for Italy 
       
       python delete_envelopes.py -c=it  680 2017 """
    
    # Get the CDR envelopes per obligation
    click.echo("Deleting envelopes from CDRTEST")

    eionet_login = (cdrtest_user, cdrtest_pwd)

    envelopes =  get_multiple_countries_rest(country_code, obligation_number, reporting_year, 'CDRTEST', None, False)
    
    click.echo("Found {} envelopes".format(len(envelopes)))


    # Filter by modifiedDate 
    if modified_after:
      click.echo("Filtering by modification date")
      envelopes = [t for t in envelopes if t['statusDate'] > modified_after]
    
      click.echo("Left {} envelopes".format(len(envelopes)))
    
   
    results =[]
    if len(envelopes) == 0:
      exit(0)
    
    #print(envelopes)
    # Process each envelope
    for idx, env in enumerate(envelopes):
        #print(env)
        click.echo("Deleting envelope {} of {} Country {} Year {} Modified {} {}".
          format(idx + 1, len(envelopes), env['countryCode'], env['periodStartYear'], env['statusDate'],env['url']))
        if click.confirm('Proceed?'):
          status = delete_envelope(env['url'],eionet_login)
          if status!=200:
            print(status)


@main.command()
@click.option('--cdrtest_user', prompt=True, hide_input=False, help='Login user to cdr test.')
@click.option('--cdrtest_pwd', prompt=True, hide_input=True, help='Password to cdr test.')
@click.argument("file", type=click.File('r'), required=True)
@click.argument("envelope_field", default='CDRTESTEnvelope')
@click.argument("out", type=click.File('w'), default='-')
def envelope_qa(file, envelope_field, cdrtest_user, cdrtest_pwd, out):
    """
    Extracts the QA feedbacks for the envelope urls specified in the FILE csv file in column ENVELOPE_FIELD
    and save tehm to OUT csv file
    """
    eionet_login = (cdrtest_user, cdrtest_pwd)

    envelopes = get_envelopes_db_data(file, extract = envelope_field)

    feedbacks = []
    for iidx, envelope_url in enumerate(envelopes):
      click.echo("Processing envelope {} of {} {}".format(iidx + 1, len(envelopes), envelope_url))
      res = get_feedbacks(envelope_url, eionet_login)
     
      country  = res["countryCode"]
      obligation = res["obligations"][0]
      reporting_year = res["periodStartYear"]

      if len(res['feedbacks'])==0:
        click.echo("No feedback found for {} {} {}".format(obligation, country, reporting_year))
        continue
      
      for idx, v in enumerate(res['feedbacks']):
        if v['activityId'] == 'AutomaticQA' or 1 == 1:

            feeback_status = v['feedbackStatus']
            title = v['title']
            feedback_type = title.split(':')[-1].strip()
            #print(feedback_type)
            print('Feedback {} {}'.format(idx+1, title))
            errors = []
            for iiidx,a in enumerate(v['attachments']):
              
              attachment = get_feedback_attachments(a['url'], eionet_login)
              parser = etree.HTMLParser()
              tree = etree.fromstring(attachment.content, parser)
             
              #rows = tree.xpath('//body/div/div/div/div/div/table/tr')
              #rows = tree.xpath('//table[@class="maintable hover"]/table/tr')p
              #table = tree.xpath('//table[@class="maintable hover"]')[0]
              rows = tree.xpath("//td[@class='bullet']/ancestor::*[position()=1]")


              print('Attachment {} - Found {} rows'.format(iiidx + 1, len(rows))) 

              for r in rows:
                err_levl = r.xpath("./td[@class='bullet']/div/@class")
                err_code = r.xpath("./td[@class='bullet']/div/a/text()")
                err_mesg = r.xpath("./td/span[@class='largeText']/text()")
                if len(err_mesg) ==0:
                  err_mesg = ['']

                if len(err_code) > 0:
                   errors.append((err_code[0], err_levl[0], err_mesg[0]))
            
            new_feedback = {'Country':country,
                            'ObligationNumber':obligation,
                            'Envelope': envelope_url,
                            'FeedbackMessage':v['feedbackMessage'],
                            'FeedbackStatus':feeback_status,
                            'ReportingYear': reporting_year,
                            'ManualFeedback': v['title'],
                            'PostingDate':v['postingDate'],
                            'Errors': errors}
            feedbacks.append(new_feedback)
        #print(feedbacks)

    #print(feedbacks)

    processed_feedbacks = []
    for v in feedbacks:

      for e in v['Errors']:
        new_rec = v.copy()
        new_rec.pop('Errors')
        new_rec['ErrorCode'] = e[0].strip('\r\n')
        new_rec['ErrorLevel'] = e[1].strip('\r\n')
        new_rec['ErrorMessage'] = e[2].strip('\r\n')

        processed_feedbacks.append(new_rec)

    if  len(processed_feedbacks)==0:
      click.echo("No output file produced")
      exit(0)

    if not out:
      out = 'output_{}.csv'.format(datetime.now().strftime("%Y-%m-%d_(%H_%M_%S"))
      
    fieldnames = processed_feedbacks[0].keys()

    writer = csv.DictWriter(out, fieldnames=fieldnames,  delimiter=';')

    writer.writeheader()
    [writer.writerow(t) for t in processed_feedbacks]

@main.command()
@click.option('--cdrtest_user', prompt=True, hide_input=False, help='Login user to cdr test.')
@click.option('--cdrtest_pwd', prompt=True, hide_input=True, help='Password to cdr test.')
@click.option('--max_activations', '-m', type=int, default=3, help='Maximum number of concurrent active qa checks allowed.')
@click.option('--qa_after', '-a', default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help='Only run QA if latest was done before date yyyy-mm-dd.')
@click.argument("file", type=click.File('r'), default='-', required=True)
@click.argument("envelope_field", default=None, required=True)
def activate_qa(file, envelope_field, cdrtest_user, cdrtest_pwd, max_activations, qa_after):
    """
    Activates the QA  for the envelope urls specified in the FILE csv file in column ENVELOPE_FIELD
    up to maxactive envelope in QA at once
    """
    eionet_login = (cdrtest_user, cdrtest_pwd)

    envelopes = []
    reader = csv.DictReader(file)
    for row in reader:
      envelopes.append(row[envelope_field])

    cnt = 0
    for iidx, envelope_url in enumerate(envelopes):
      
      if cnt == max_activations:
        click.echo("Reached maximum number of activations {} ".format(max_activations))
        exit(0) 

      res = get_history(envelope_url, eionet_login)
      country  = res["countryCode"]
      obligation = res["obligations"][0]
      reporting_year = res["periodStartYear"]
      current_wi = res['history'][-1]
      #print(res['history'])

      click.echo("Envelope {} of {} {} {} {} {} activity: {} status: {}".
          format(iidx + 1, len(envelopes), obligation, country, reporting_year, envelope_url, 
            current_wi['activity_id'], current_wi['activity_status']))

      # Skip envelopes in QA
      if ((current_wi['activity_id'] == 'AutomaticQA' and current_wi['activity_status'] == "active") or
         (current_wi['activity_id'] == 'DeleteAutomaticQAFeedback')):
        modified = current_wi['modified']
        click.echo("Skipping envelope {} of {} {} {} {} {} already running QA since {}".
          format(iidx + 1, len(envelopes), obligation, country, reporting_year, envelope_url, modified))
        cnt +=1
        continue

      
      # TODO skip QA for envelopes already completed after a certain date time
      # Find most recent QA
      if qa_after is not None:
        most_recent_qa = datetime.datetime(1900,1,1)
        for status in res['history']:
          if status['activity_id'] =='AutomaticQA' and status['activity_status'] == 'complete':
            if  most_recent_qa is None:
              most_recent_qa =  status['modified']
            else:
              if most_recent_qa < status['modified']:
                most_recent_qa = status['modified']
        #print(qa_after, most_recent_qa )

        if qa_after <= most_recent_qa:
          click.echo("Skipping envelope because already run qa on {}".format(most_recent_qa.strftime("%d-%m-%Y")))
          continue    

      click.echo("Activating QA for envelope {} of {} {} {} {} {}".
        format(iidx + 1, len(envelopes), obligation, country, reporting_year, envelope_url))

      if current_wi['activity_id'] =='Draft' and current_wi['activity_status'] == "inactive":
        click.echo("Activating Draft")
            
      activate_envelope(envelope_url, eionet_login, current_wi['id'])
      start_envelope_qa(envelope_url, eionet_login, current_wi['id'])
      
      cnt +=1
      

if __name__ == "__main__":
	main()
