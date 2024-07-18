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
            enrich_feed_docs(args.feedconfig, FEEDINDEX, esclient=esclient)
        except Exception as e:
            print("Exception processing feed config: %s" % (e))

    else:
        for config in yamlconfig['outlet_rss_configs']:
            if 'economist' in config:
                print("Processing config: {}".format(config))
                stat = enrich_feed_docs(config, FEEDINDEX, esclient=esclient)
                print(stat)

    print("Done!")


def enrich_feed_docs(feed_id, feed_index, esclient):

        #query = {"match_phrase": {"source_outlet": "TheHindu"}}
        query = {"bool":
                     {
                         "must": [
                             {"range": {"eventtime": {"gte": "now-365d", "lt": "now"}}},
                             {"match": {"source": feed_id}}
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

                print("Processing: %s" %(result['_source']['data']['links'][0]['href']))
                htmlcontent = get_url_content(result['_source']['data']['links'][0]['href'], env.ECONOMIST_COOKIE)
                text, js  = extract_text_content(htmlcontent)
                text = text.replace(u'\xa0', u' ')

                if 'post_process' not in result['_source'].keys():
                    result['_source']['post_process'] = {}

                if 'enrich1' not in result['_source']['post_process'].keys():
                    result['_source']['post_process']['enrich1'] = datetime.now()

                if 'content' not in result['_source'].keys():
                    result['_source']['content'] = {}
                    result['_source']['content']['enrich1'] = text

                if 'metadata' not in result['_source'].keys():
                    result['_source']['metadata'] = {}
                    result['_source']['metadata']['enrich1'] = js['props']['pageProps']

                esclient.update(index=feed_index, id=result['_id'], doc=result['_source'])

            except Exception as e:
                print("Exception processing ID: %s URL: %s - %s" %(result['_id'], result['_source']['data']['links'][0]['href'], e))

def get_url_content(url, cookie):
    http_headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/10.0.1916.47 Safari/537.36',
        'Cookie': cookie
    }
    req = urllib.request.Request(url, headers=http_headers)
    with urllib.request.urlopen(req) as response:
        html = response.read()
    return html

def extract_text_content(htmlcontent):
    soup = BeautifulSoup(htmlcontent, 'html.parser')
    content = ""
    try:
        js = json.loads(soup.find_all("script", id="__NEXT_DATA__")[0].text)
        paracount = 0
        for data in js['props']['pageProps']['cp2Content']['body']:

            if data['type'] == 'PARAGRAPH':
                print(data['text'])
                content = content + " " + data['text'].strip()
                paracount += 1
                print("Grabbed para #%s: %s" %(paracount, content))

    except Exception as e:
        content = soup.text.strip().replace("\n", '')
        js = {'props': {'pageProps': "NULL"}}

    return content.strip(), js

# __name__
if __name__ == "__main__":
    main()
