from datetime import datetime
from datetime import timezone
import pytz
import time
from elasticsearch import Elasticsearch
from argparse import ArgumentParser
import feedparser
import env
import yaml
import rss_newsoutlets
import rss_index_enrich1_economist
import rss_index_enrich1_thehindu
import rss_index_order2_openai



def main():

    rss_newsoutlets.main()
    rss_index_enrich1_economist.main()
    rss_index_enrich1_thehindu.main()
    rss_index_order2_openai.main()

    print("Done!")


# __name__
if __name__ == "__main__":
    main()
