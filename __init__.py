
import secondary_domains_crawler


if __name__ == '__main__':
    # obj = MainCrawler.Crawler()
    # obj = Exclude_domains_crawler.Exclude_Crawler()
    obj = secondary_domains_crawler.secondary_domains_crawler()
    obj.crawl()
