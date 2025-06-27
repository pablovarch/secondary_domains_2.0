from dependencies import  log
from settings import db_connect
import psycopg2
import re
from typing import Union


class tld :
    def __init__(self):
        self.__logger = log.Log().get_logger(name='ad_count.log')

    def main(self):
        self.__logger.info('getting all secondary_domains')