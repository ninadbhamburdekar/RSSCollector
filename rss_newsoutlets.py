from datetime import datetime
from datetime import timezone
import pytz
import time
from elasticsearch import Elasticsearch
from argparse import ArgumentParser
import feedparser
import env
import yaml



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
                        help="Run a specific feed config")
    parser.add_argument("-l", "--lookback", dest="lookback", default="96",
                        help="Override lookback time (default is 96hrs)")
    args = parser.parse_args()

    if args.feedconfig != "ALL":
        print("Running feed config: {}".format(args.feedconfig))
        try:
            process_feed(args.feedconfig,
                         yamlconfig['outlet_rss_configs'][args.feedconfig]['feedlink'],
                         yamlconfig['outlet_rss_configs'][args.feedconfig]['sourceoutlet'],
                         FEEDINDEX,
                         esclient)
        except Exception as e:
            print("Exception reading poll config: %s" %(e))

    else:
        for config in yamlconfig['outlet_rss_configs']:
            print("Processing config: {}".format(config))
            stat = process_feed(config,
                         yamlconfig['outlet_rss_configs'][config]['feedlink'],
                         yamlconfig['outlet_rss_configs'][config]['sourceoutlet'],
                         FEEDINDEX,
                         esclient)
            print(stat)

    print("Done!")


def process_feed(feed_id, feed_link, sourceoutlet, feed_index, esclient):

    d = feedparser.parse(feed_link)

    doc_data = {
        'project': 'outletcollect',
        'project_version': '1.0.1.a',
        'source_outlet': sourceoutlet,
        'source': feed_id,
        'source_link': feed_link,
        'runtimestamp': datetime.now(),
        'document_id': feed_id,
        'document_history' : [],
        'process_order': ['0']
    }

    for itm in d['entries']:

        print("Processing:" + itm['title'])
        doc_data['eventtime'] = datetime(*itm['published_parsed'][:6])
        doc_data['data'] = itm
        doc_data['document_id'] = feed_id+itm['id']
        doc_data['eventtime_updated'] = datetime(*itm['published_parsed'][:6])

        try:
            doc_data_old = esclient.get(index=feed_index, id=feed_id+"_"+itm['id'])
            if not doc_data_old.body['_source']['data']['published'] == doc_data['data']['published']:
                doc_data['document_history'].append(doc_data_old)
                doc_data['eventtime_updated'] = datetime.now()
                resp = esclient.index(index=feed_index, id=feed_id+"_"+itm['id'], document=doc_data)
            else:
                print("No change to item %s since published time %s" %(doc_data_old.body['_id'], doc_data_old.body['_source']['data']['published']))

        except Exception as e:
            try:
                resp = esclient.index(index=feed_index, id=feed_id+"_"+itm['id'], document=doc_data)
            except Exception as e:
                print("Exception %s while indexing item:  %s" %(e, doc_data))

    return True


# __name__
if __name__ == "__main__":
    main()
