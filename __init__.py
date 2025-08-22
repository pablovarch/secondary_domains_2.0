
import secondary_domains_crawler
import site_map
import tld
import ssl_analyzer
import html_fields
import mfa_engagement
import high_traffic
import ad_density
import is_high_risk_geo
import for_no_redirect_domains

if __name__ == '__main__':
    # secondary_domains_crawler.secondary_domains_crawler().crawl()
    site_map.site_map().main()
    tld.tld().main()
    ssl_analyzer.ssl_analyzer().main()
    html_fields.html_fields().main()
    mfa_engagement.mfa_engagement().main()
    high_traffic.high_traffic().main()
    ad_density.ad_density().main()
    is_high_risk_geo.is_high_risk_geo().main()
    # for_no_redirect_domains.For_no_redirect_domains().main()