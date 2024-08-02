from datetime import datetime
from datetime import timezone
import pytz
import rss_newsoutlets
import time
from elasticsearch import Elasticsearch
from argparse import ArgumentParser
import ssl
from bs4 import BeautifulSoup
import urllib.request
import re
import feedparser
import env
import yaml
import json
from openai import OpenAI


ESNODES = env.ESNODES
esclient = env.setup_esclient(ESNODES)

### Read Polling Config
with open("rsscollect_config.yaml") as stream:
    try:
        yamlconfig = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

FEEDINDEX = yamlconfig['elasticconfig']['feedindex']

openaiclient = OpenAI(
    # This is the default and can be omitted
    api_key=env.openai_api_token,
)

def main():

    ## Parse input arguments
    parser = ArgumentParser()
    parser.add_argument("-f", "--feed", dest="feedconfig", default="ALL",
                        help="Enrich a specific collection")
    parser.add_argument("-l", "--lookback", dest="lookback", default="24",
                        help="Override lookback time (default is 24hrs)")
    args = parser.parse_args()

    daily = daily_summary(openaiclient, esclient)

    print("Done!")

def daily_summary(openaiclient, esclient):

    summaries = []
    query = {"bool": {"must": [{"range": {"eventtime": {"gte": "now-1d", "lt": "now"}}}, {"match": {"process_order": 0}}]}}

    esdata = esclient.search(index="estest-index1", query=query, size=5000)

    print("Processing total items: %s" %(esdata['hits']['total']['value']))

    for newsitem in esdata['hits']['hits']:
        timestamp = datetime.fromisoformat(newsitem['_source']['eventtime'])
        summaries.append("At "+str(timestamp)+": "+newsitem['_source']['data']['summary'])

    msg = {"role": "user", "content": "SAMPLE CONTENT"}
    msg['content'] = ("summarize top five themes including historical context across the following headlines, a high level timeline and highlight any cybersecurity implications or predictions based on input events. Output in json under keys: "
                      + "{'topics': {'topic_category': [highlights]}, 'timeline', 'cybersecurity_implications']}" + " : ") + str(summaries)

    response1 = openaiclient.chat.completions.create(messages=[msg], model="gpt-4o", max_tokens=4000, temperature=0.4)

    data = {}
    data['openaisummaryjson'] = json.loads(response1.choices[0].message.content.replace('```json', '').replace('```', '').strip())
    data['metadata'] = {}
    data['metadata']['enrich2'] = {}
    data['metadata']['enrich2']['timestamp'] = datetime.now(timezone.utc)
    data['metadata']['enrich2']['itemcount'] = len(summaries)
    data['metadata']['enrich2']['inputlength'] = len(msg['content'])
    data['metadata']['enrich2']['outputlength'] = len(json.dumps(data['openaisummaryjson']))
    data['metadata']['enrich2']['openaiusage'] = str(response1.usage)
    data['process_order'] = 2

    data ['eventtime'] = datetime.now(timezone.utc)

    storesummary = esclient.index(index="estest-index1", id="dailysummary_" + str(datetime.now(timezone.utc)), document=data)
    print(data['openaisummaryjson'])


# __name__
if __name__ == "__main__":
    main()

