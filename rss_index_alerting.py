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

with open("watcher_config.yaml") as stream:
    try:
        watcherconfig = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

FEEDINDEX = yamlconfig['elasticconfig']['feedindex']

def main():

    ## Parse input arguments
    parser = ArgumentParser()
    parser.add_argument("-l", "--lookback", dest="lookback", default="24",
                        help="Override lookback time (default is 24hrs)")
    args = parser.parse_args()

    query = {"bool":
        {
            "must": [
                {"range": {"eventtime": {"gte": "now-12h", "lt": "now"}}},
            ],
            "must_not": [
                {"match": {"process_order": "2"}}
            ]
        }
    }

    new_events = esclient.search(index=FEEDINDEX, query=query, size=10000)

    summary = scan_events(new_events, esclient)

    print("Done!")


def scan_events(all_events, esclient):

    for watcher in watcherconfig['watcher_configs']:
        matches = {}
        print("Processing keywords for watcher {}".format(watcherconfig['watcher_configs'][watcher]))
        for searchterm in watcherconfig['watcher_configs'][watcher]['searchterms']:
            print("Looking for keyword %s" % (searchterm))

            for event in all_events.body['hits']['hits']:
                print("Checking article %s" % (event['_source']['data']['title']))
                try:
                    if searchterm.lower() in str(event['_source']['data']['summary']).lower():
                        print("For watcher %s found keyword %s in article %s" %(watcher, searchterm, event['_source']['data']['title_detail']))
                        if searchterm not in matches.keys():
                            matches[searchterm] = []

                        matches[searchterm].append(event)
                except KeyError:
                    continue

                try:
                    if searchterm.lower() in str(event['_source']['data']['title']).lower():
                        print("For watcher %s found keyword %s in article %s" %(watcher, searchterm, event['_source']['data']['title_detail']))
                        if searchterm not in matches.keys():
                            matches[searchterm] = []

                        matches[searchterm].append(event)
                except KeyError:
                    continue

                try:
                    if searchterm.lower() in str(event['_source']['data']['tags']).lower():
                        print("For watcher %s found keyword %s in article %s" %(watcher, searchterm, event['_source']['data']['title_detail']))
                        if searchterm not in matches.keys():
                            matches[searchterm] = []

                        matches[searchterm].append(event)
                except KeyError:
                    continue

        if len(matches) > 0:
            print("Found matches on %s keywords for watcher %s" % (len(matches), watcher))
            sendalert = alert_watcher(watcher, matches, esclient)


def alert_watcher(watcher, matches, esclient):

    watcherinfo = watcherconfig['watcher_configs'][watcher]

    items = {}
    for match in matches:
        items[match] = []
        for event in matches[match]:
            snippet = {}
            snippet['title'] = (
                event['_source']['data']['title'] if 'title' in event['_source']['data'].keys() else "No Title found")
            snippet['summary'] = (event['_source']['data']['summary'] if 'summary' in event['_source'][
                'data'].keys() else "No Summary found")
            snippet['tags'] = (event['_source']['data']['tags'] if 'tags' in event['_source'][
                'data'].keys() else "No Tags found")
            snippet['link'] = (event['_source']['data']['link'] if 'link' in event['_source'][
                'data'].keys() else "No Link found")
            snippet['source'] = event['_source']['source_outlet']
            snippet['_id'] = event['_id']


            tempevent = esclient.get(index=FEEDINDEX, id=event['_id'])

            alert_history = {}
            alert_history[match] = {"tagtime": datetime.utcnow().isoformat()}

            if "alert_history" in tempevent['_source'].keys():
                if watcher in tempevent['_source']['alert_history'].keys():
                    print("Event %s is already tagged for watcher %s at %s" % (event['_id'], watcher, tempevent['_source']['alert_history'][watcher]))
                else:
                    tempevent['_source']['alert_history'][watcher] = alert_history
                    items[match].append(snippet)
                    esclient.update(index=FEEDINDEX, id=tempevent['_id'], doc=tempevent['_source'])
            else:
                tempevent['_source']['alert_history'] = {}
                tempevent['_source']['alert_history'][watcher] = alert_history
                items[match].append(snippet)
                esclient.update(index=FEEDINDEX, id=tempevent['_id'], doc=tempevent['_source'])

        if len(items[match]) < 1:
            items.pop(match)



    if len(items) > 0:

        environment = jinja2.Environment(loader=jinja2.FileSystemLoader("."))
        template = environment.get_template("email_template_alert.html")

        email_content = template.render(items=items)
        with open("rendered_email_alert.html", mode="w", encoding="utf-8") as results:
            results.write(template.render(items=items))

        print("Alerting Watcher: %s on matches on keywords %s" % (watcherinfo, items.keys()))


        response = requests.post(
            'https://api.mailgun.net/v3/' + MAILGUN_DOMAIN + '/messages',
            auth=('api', MAILGUN_API_KEY),
            data={
                'from': 'GZR Watcher <' + 'gzrwatch@' + MAILGUN_DOMAIN + '>',
                'to': watcherinfo['email'],
                'bcc': MAILGUN_SEND_CONFIG['bcc'],
                'subject': 'Watcher Alert for ' + watcher + " - " + datetime.now().strftime("%b ") + datetime.now().strftime("%d").lstrip(
                    '0') + ", " + datetime.now().strftime("%Y ") + datetime.now().strftime("%H:%m") ,
                'text': json.dumps(items, indent=4),
                'html': email_content,
            },
            timeout=5  # sec
        )






def email_alert(esclient):

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
            'bcc': MAILGUN_SEND_CONFIG['bcc'],
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

