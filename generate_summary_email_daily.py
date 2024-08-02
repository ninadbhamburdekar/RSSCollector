from datetime import datetime
from datetime import timezone
import pytz
import requests
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
import jinja2

ESNODES = env.ESNODES
esclient = env.setup_esclient(ESNODES)
MAILGUN_API_KEY = env.MAILGUN_API_KEY
MAILGUN_DOMAIN = env.MAILGUN_DOMAIN
MAILGUN_SEND_CONFIG = env.MAILGUN_SEND_CONFIG

### Read Polling Config
with open("rsscollect_config.yaml") as stream:
    try:
        yamlconfig = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

FEEDINDEX = yamlconfig['elasticconfig']['feedindex']

def main():

    ## Parse input arguments
    parser = ArgumentParser()
    parser.add_argument("-l", "--lookback", dest="lookback", default="24",
                        help="Override lookback time (default is 24hrs)")
    args = parser.parse_args()

    summary = email_summary(esclient)

    print("Done!")

def email_summary(esclient):

    summaries = []
    query = {"bool": {"must": [{"range": {"eventtime": {"gte": "now-1d", "lt": "now"}}}, {"match": {"process_order": 2}}]}}

    esdata = esclient.search(index="estest-index1", query=query, size=10)

    jsonblob = json.loads(json.dumps(esdata.body['hits']['hits'][-1]['_source']))['openaisummaryjson']

    topics = {}

    for topic in jsonblob['topics'].keys():
        topic_name = topic.replace('_', ' ')
        topics[topic_name.title()] = jsonblob['topics'][topic]

    geninfo = {}
    geninfo['timegenerated'] = datetime.fromisoformat(esdata.body['hits']['hits'][-1]['_source']['eventtime']).strftime("%b %d, %Y %H:%m UTC")
    geninfo['creditsusage'] = str(esdata.body['hits']['hits'][-1]['_source']['metadata']['enrich2']['openaiusage']).replace('(', ' ').replace(')', ' ').replace('CompletionUsage', '')



    environment = jinja2.Environment(loader=jinja2.FileSystemLoader("."))
    template = environment.get_template("email_template.html")

    email_content = template.render(topics=topics, geninfo=geninfo)
    with open("rendered_email.html", mode="w", encoding="utf-8") as results:
        results.write(template.render(topics=topics, geninfo=geninfo))

    response = requests.post(
        'https://api.mailgun.net/v3/' + MAILGUN_DOMAIN + '/messages',
        auth=('api', MAILGUN_API_KEY),
        files=[
            ('inline[0]', ('email.html', open('rendered_email.html', mode='rb').read()))
        ],
        data={
            'from': MAILGUN_SEND_CONFIG['from'],
            'to': MAILGUN_SEND_CONFIG['to'],
            'subject': 'News AI Summary '+datetime.now().strftime("%b ")+datetime.now().strftime("%d").lstrip('0')+", "+datetime.now().strftime("%Y"),
            'text': json.dumps(topics, indent=4),
            'html': email_content
        },
        timeout=5  # sec
    )

    print("Processing total items: %s" %(esdata['hits']['total']['value']))



# __name__
if __name__ == "__main__":
    main()

