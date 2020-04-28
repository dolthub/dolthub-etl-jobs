#!/usr/bin/python3

import datetime
import json
import pandas
import glob
import re
import unidecode
import copy

from doltpy.core import Dolt
from pprint import pprint

# missing some here
case_column_map = {
    'term' : 'term',
    'caseName': 'case_name',
    'caseLink': 'link', 
    'decidedBy': 'decided_by',
    'arguedOn': 'argued_on', 
    'petitioner': 'petitioner',
    'respondent': 'respondent',
    'docket': 'docket',
    'citation': 'citation',
    'justiaLink': 'justia_link',
    'lowerCourt': 'lower_court',
    'grantedOn': 'granted_on',
    'decidedOn': 'decided_on',
    'wikiData': {
        'title': 'wiki_title',
        'Litigants': 'wiki_litigants',
        'ArgueDate': 'wiki_argue_date',
        'ArgueYear': 'wiki_argue_year',
        'DecideDate': 'wiki_decide_date',
        'DecideYear': 'wiki_decide_year',
        'FullName': 'wiki_full_name',
        'USVol': 'wiki_us_vol',
        'USPage': 'wiki_us_page',
        'ParallelCitations': 'wiki_parallel_citations',
        'Docket': 'wiki_docket',
        'OralArgument': 'wiki_oral_argument',
        'OralReargument': 'wiki_oral_reargument',
        'OpinionAnnouncement': 'wiki_opinion_announcement',
        'Prior': 'wiki_prior',
        'Subsequent': 'wiki_subsequent',
        'Holding': 'wiki_holding',
        'SCOTUS': 'wiki_scotus',
        'Majority': 'wiki_majority',
        'JoinMajority': 'wiki_join_majority',
        'Plurality': 'wiki_plurality',
        'JoinPlurality': 'wiki_join_plurality',
        'Concurrence': 'wiki_concurrence',
        'JoinConcurrence': 'wiki_join_concurrence',
        'Concurrence2': 'wiki_concurrence2',
        'JoinConcurrence2': 'wiki_join_concurrence2',
        'Concurrence3': 'wiki_concurrence3',
        'Concurrence/Dissent': 'wiki_concurrence_dissent',
        'JoinConcurrence/Dissent': 'wiki_join_concurrence_dissent',
        'Dissent': 'wiki_dissent',
        'JoinDissent': 'wiki_join_dissent',
        'Dissent2': 'wiki_dissent2',
        'JoinDissent2': 'wiki_join_dissent2',
        'NotParticipating': 'wiki_not_participating',
        'LawsApplied': 'wiki_laws_applied'
    }
}

transcript_column_map = {
    'transcriptTitle': 'title',
    'transcriptLink': 'link',
}

text_column_map = {
    'start': 'start',
    'stop': 'stop',
    'duration': 'duration',
    'text': 'text',
}

date_convert = [
    'argued_on',
    'granted_on',
    'decided_on'
]

def normalize(data, mapper, output):
    for key in dict.keys(mapper):
        if key in data:
            if isinstance(mapper[key], dict):
                normalize(data[key], mapper[key], output)
            else:
                # Strip unicode
                if isinstance(data[key], str):
                    data[key] = re.sub('<!--.*-->', '', data[key])
                    output[mapper[key]] = unidecode.unidecode(data[key])
                else:
                    output[mapper[key]] = data[key]

def import_case_file(path, case, transcripts):
    with open(path) as file:
        case_dict = json.load(file)

    normalize(case_dict, case_column_map, case)

    transcript_out = { 'case_name': case['case_name'] }
    for transcript in case_dict['caseTranscripts']:
        normalize(transcript, transcript_column_map, transcript_out)
        for speakers in transcript['transcript']:   
            transcript_out['speaker'] = speakers['speakerName']
            for text in speakers['textObjs']:
                normalize(text, text_column_map, transcript_out)
                transcripts.append(copy.copy(transcript_out))
            
def convert_dates(data, cols):
    for col in cols:
        if ( col in data and
             isinstance(data[col], str) and
             not data[col] == '' ):
            data[col] = re.sub('\s-\s\d+', '', data[col])
            data[col] = re.sub(';.*', '', data[col])
            try:
                data[col] = datetime.datetime.strptime(data[col], '%b %d, %Y')
            except ValueError:
                data[col] = ''
        else:
            # Need to put all the multi-column date logic here
            continue

#
# Start main
#
cases       = []
transcripts = []
i = 1
for file in glob.glob('supreme-court-cases/cases/*/*.js'):
    if ( file == '.js' ): continue
    
    case = {}
    transcripts = []
    import_case_file(file, case, transcripts)
    convert_dates(case, date_convert)
    
    cases.append(case)    

cases_df       = pandas.DataFrame(cases)
transcripts_df = pandas.DataFrame(transcripts) 

transcripts_pks = ['case_name', 'title', 'speaker', 'start']

repo = Dolt('./')
repo.import_df('cases', cases_df, ['case_name'], import_mode='replace')
repo.import_df('transcripts',
               transcripts_df,
               transcripts_pks,
               import_mode='replace')
