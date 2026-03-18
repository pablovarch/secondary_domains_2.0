
import secondary_domains_crawler
from features import site_map
from features import tld
from features import ssl_analyzer
from features import html_fields
from features import mfa_engagement
from features import high_traffic
from features import ad_density
from features import is_high_risk_geo
from features import cleaner_domains
from features import google_search_results
import for_no_redirect_domains
from features import features_to_search
import addres_bar_class
import sw_offline_class2
import jarm_rules
import rude_rules
import block_class
import sec_dom_software_classifier
import final_update_Script
import mfa_no_ads
from piracy_class import openai_media_type_sec_domain
from piracy_class import ssl_analyzer_sec_domain
from piracy_class import sec_dom_piracy_classifier_v2
import asyncio


if __name__ == '__main__':
    addres_bar_class.Address_bar_class().main()
    rude_rules.Betting_piracy().main()
    google_search_results.Google_Search_results().main()
    secondary_domains_crawler.secondary_domains_crawler().crawl()
    # jarm_rules.Jarm_processing().main()
    sw_offline_class2.Sw_offline_class().main()
    mfa_no_ads.main()
    # block_class.Block_class().main()
    # asyncio.run(openai_media_type_sec_domain.main())
    # asyncio.run(ssl_analyzer_sec_domain.run_backfill())
    # asyncio.run(sec_dom_piracy_classifier_v2.main())
    # html_fields.html_fields().main()
    # site_map.site_map().main()
    # tld.tld().main()
    # ssl_analyzer.ssl_analyzer().main()
    # mfa_engagement.mfa_engagement().main()
    # high_traffic.high_traffic().main()
    # ad_density.ad_density().main()
    # is_high_risk_geo.is_high_risk_geo().main()
    # for_no_redirect_domains.For_no_redirect_domains().main()
    asyncio.run(sec_dom_software_classifier.main())
    final_update_Script.main()





