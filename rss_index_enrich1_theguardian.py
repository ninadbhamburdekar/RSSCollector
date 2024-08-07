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


def main():

    ## Parse input arguments
    parser = ArgumentParser()
    parser.add_argument("-f", "--feed", dest="feedconfig", default="ALL",
                        help="Enrich a specific feed config")
    parser.add_argument("-l", "--lookback", dest="lookback", default="96",
                        help="Override lookback time (default is 96hrs)")
    args = parser.parse_args()

    if args.feedconfig != "ALL":
        print("Running feed config: {}".format(args.feedconfig))
        try:
            enrich_feed_docs(args.feedconfig,
                         FEEDINDEX, esclient=esclient)
        except Exception as e:
            print("Exception processing feed config: %s" %(e))

    else:
        for config in yamlconfig['outlet_rss_configs']:
            if 'theguardian' in config:
                print("Processing config: {}".format(config))
                stat = enrich_feed_docs(yamlconfig['outlet_rss_configs'][config]['sourceoutlet'],
                             FEEDINDEX, esclient=esclient)
                print(stat)

    print("Done!")

def enrich_feed_docs(feed_id, feed_index, esclient):

        #query = {"match_phrase": {"source_outlet": "TheHindu"}}
        query = {"bool":
                     {
                         "must": [
                             {"range": {"eventtime": {"gte": "now-365d", "lt": "now"}}},
                             {"match": {"source_outlet": feed_id}}
                            ],
                         "must_not": [
                             {"exists": {"field": "post_process.enrich1"}}
                            ]
                     }
                }


        feed_docs = []

        feed_docs = esclient.search(index=feed_index, query=query, size=10000)

        print("Got %s results: " % len(feed_docs.raw['hits']['hits']))

        for result in feed_docs.raw['hits']['hits']:
            print("Processing %s" %(result))
            try:

                htmlcontent = get_url_content(result['_source']['data']['links'][0]['href'])
                text  = extract_text_content(htmlcontent, 'maincontent')
                text = text.replace(u'\xa0', u' ')

                if 'post_process' not in result['_source'].keys():
                    result['_source']['post_process'] = {}

                if 'enrich1' not in result['_source']['post_process'].keys():
                    result['_source']['post_process']['enrich1'] = datetime.now()

                if 'content' not in result['_source'].keys():
                    result['_source']['content'] = {}
                    result['_source']['content']['enrich1'] = text

                result['_source']['process_order'] = 1

                esclient.update(index=feed_index, id=result['_id'], doc=result['_source'])

            except Exception as e:
                print("Exception processing ID: %s URL: %s - %s" %(result['_id'], result['_source']['data']['links'][0]['href'], e))

def get_url_content(url):
    http_headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
    }
    req = urllib.request.Request(url, headers=http_headers)
    with urllib.request.urlopen(req) as response:
        html = response.read()
    return html

def extract_text_content(htmlcontent, identifier):
    soup = BeautifulSoup(htmlcontent, 'html.parser')
    content = ""
    content = soup.find_all("div", id="maincontent")[-1].get_text()

    return content.strip()

# __name__
if __name__ == "__main__":
    main()
