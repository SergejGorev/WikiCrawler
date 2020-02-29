from bs4 import BeautifulSoup, SoupStrainer
import pandas as pd
import os, re, sys
from typing import List
import logging
from logging.handlers import RotatingFileHandler

from multiprocessing import Process, Pool


import concurrent.futures
import requests
import threading
import time

thread_local = threading.local()

logger = logging.getLogger('Info')
app_log = logging.getLogger('ContentWriter')

class Settings:
    starting_link = r'https://en.wikipedia.org/wiki/Canada'
    wiki_link = r'https://en.wikipedia.org'

    # for windows
    path_dir = r'C:\Python\Projects\WikiCrawler\data'
    # for linux
    # path_dir = r'/home/admin/WikiCrawler/data'

    content_file = r'data'

    # todo ADD ADDITIONAL FUNCTIONALITY (count words for link, speed, etc..)
    used_links_file = 'used_links.csv'
    unique_links_file = 'unused_links.csv'

    filepath_unique_links = os.path.join(path_dir, unique_links_file)
    filepath_used_links = os.path.join(path_dir, used_links_file)


    # todo OPTIMISATION OF COMPILED OBJECT IS NEEDED TO SPEED UP PROCESS
    colon = re.compile(':')
    wiki = re.compile('/wiki/')
    slash = re.compile('//')
    main_page = re.compile('Main_Page')
    quotes = re.compile('"')


    # maximum_links = float('inf')
    maximum_links = 200

    # how many pages with content to store in memory until it is dumped to file
    pages_in_memory = 100

    # save unique links after limit has been reached
    unique_links_in_memory = 100_000

    # content file size
    one_mb = 1024*1024
    content_file_size = 1000 * one_mb # in mb

    backupcount = 5000


class WikiCrawler:

    links_only = SoupStrainer('a', href=True)
    text_section = SoupStrainer('p')

    def __init__(self):
        # super().__init__()
        # Response object, page from wikipedia, for each link separately
        self.page = None # type: None|requests.models.Response

        # Actual link to get Response object from
        self.link = None

        # Set of unique links
        self.unique_links = set() # type: set

        # contains already used, clicked links from unique set
        self.used_links = set() # type: set

        # contains content got from each unique link
        self.content = [] # type: List[str, str, ...]

        # Provides instance of FileHandler to save, and open files
        self.file_handler = FileHandler() # type: FileHandler

        logger.info(f'Working in {repr(WikiCrawler.__name__)}')

        self._check_if_links_saved_in_files()

        # TODO class define print function

        # initialise
        # self._get_wiki_page()
        # self._collect_unique_links()
        # self._get_content_from_page()


    def _check_if_links_saved_in_files(self):
        if not os.path.isfile(Settings.filepath_used_links) \
                or not os.path.isfile(Settings.filepath_unique_links):

            logger.info(f'{repr(Settings.used_links_file)} and {repr(Settings.unique_links_file)} '
                  f'dont exist. Starting the script from initial link: {Settings.starting_link}')

        else:
            self.used_links = self.file_handler.open_links(
                Settings.filepath_used_links)
            self.unique_links = self.file_handler.open_links(
                Settings.filepath_unique_links)
            Settings.starting_link = None
            self.link = self.unique_links.pop()

    def _get_session(self):
        if not hasattr(thread_local, 'session'):
            thread_local.session = requests.Session()
        return thread_local.session

    def _get_wiki_page(self, url):
        self.link = url
        session = self._get_session()
        with session.get(f'{Settings.wiki_link}{self.link}' if not Settings.starting_link
            else Settings.starting_link) as response:
                self.page = response
        if Settings.starting_link is not None: self.used_links.add(Settings.starting_link)
        Settings.starting_link = None
        self.unique_links.remove(self.link)


        self._collect_unique_links()
        self._get_content_from_page()

        self.used_links.add(self.link)

        if len(self.used_links) % 10 == 0:
            logger.info(f'{len(self.used_links)} links used...')
            logger.info(f'{len(self.unique_links)} unique links are in Set.')
            logger.info(f'{len(self.content)} content now.')


    def download_pages(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executors:
            executors.map(self._get_wiki_page, self.unique_links)


    def _collect_unique_links(self):
        soup = self._get_soup(parse_only=self.links_only)
        for line in soup.children:
            match1 = Settings.wiki.findall(str(line))
            match2 = Settings.colon.findall(str(line))
            match3 = Settings.slash.findall(str(line))
            match4 = Settings.main_page.findall(str(line))

            # TODO remove quotes from links, at the end and beginning
            # match5 = Settings.quotes.findall(line)

            if match1 and not match2 and not match3 and not match4:
                if line['href'] not in self.used_links:
                    self.unique_links.add(line['href'])


    def _get_content_from_page(self):
        soup = self._get_soup(parse_only=self.text_section)
        for line in soup.children:
            self.content.append(line.get_text())


    def _link_engine(self):
        while len(self.used_links) <= Settings.maximum_links:
            self.link = self.unique_links.pop()
            self._get_wiki_page()
            self._collect_unique_links()
            self._get_content_from_page()
            self.used_links.add(self.link)

            if len(self.used_links) % 10 == 0:
                logger.info(f'{len(self.used_links)} links used...')
                logger.info(f'{len(self.unique_links)} unique links are in Set.')
                logger.info(f'{len(self.content)} content now.')

            if len(self.used_links) % Settings.pages_in_memory == 0:
                self.file_handler.write_content(self.content)
                self.file_handler.save_links(self.used_links, Settings.filepath_used_links)
                self.content = []

            if len(self.unique_links) % Settings.unique_links_in_memory == 0:
                self.file_handler.save_links(self.unique_links, Settings.filepath_unique_links)

        else:
            self.file_handler.save_links(self.unique_links, Settings.filepath_unique_links)
            self.file_handler.save_links(self.used_links, Settings.filepath_used_links)

    def multi_process_pool(self):
        cores = os.cpu_count()
        with Pool(processes=cores) as pool:
            pool.apply_async(self._link_engine())


    def _get_soup(self, parse_only):
        return BeautifulSoup(self.page.content, 'html.parser',
                             parse_only=parse_only)


class FileHandler:
    def __init__(self):
        logger.info(f'Working in {repr(FileHandler.__name__)}')

    def save_links(self, links, path):
        df = pd.DataFrame(links)
        df.columns = ['Links']
        df.to_csv(path, index=False)
        logger.info(f'{len(links)} used Links saved to: {repr(path)}.')

    def open_links(self, filepath_links):
        df = pd.read_csv(filepath_links)
        links = set(df['Links'].values)
        logger.info(f'{len(links)} links opened from: {repr(filepath_links)}.')
        return links

    def write_content(self, content):
        logger.info(f'Content saved. {len(content)} Lines to {repr(Settings.content_file)}')
        for line in content:
            app_log.info(line)

class Processing(WikiCrawler):
    pass

class Timing(WikiCrawler):
    pass


def get_logger(logger, app_log, logger_level=logging.INFO, app_log_level=logging.INFO):
    app_log_path = os.path.join(Settings.path_dir, Settings.content_file)
    if not os.path.exists(app_log_path):
        os.makedirs(Settings.path_dir)

    my_handler = RotatingFileHandler(app_log_path, mode='a', maxBytes=Settings.content_file_size,
                                     encoding='utf-8', backupCount=Settings.backupcount)
    app_log.setLevel(app_log_level)
    app_log.addHandler(my_handler)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    sh = logging.StreamHandler()
    fh = logging.FileHandler('Info.log')

    sh.setFormatter(formatter)
    logger.setLevel(logger_level)
    logger.addHandler(sh)
    logger.addHandler(fh)


def run_script():
    inst = None # type: None|WikiCrawler
    try:
        logger.info('Starting Script')
        inst = WikiCrawler()
        # inst.multi_process_pool()
        # inst._get_wiki_page()
        inst.download_pages()
        logger.info('Script finisched')
    except:
        logger.error('Something went WRONG!')
        file_hanler = FileHandler()

        file_hanler.save_links(inst.unique_links, Settings.filepath_unique_links)
        file_hanler.save_links(inst.used_links, Settings.filepath_used_links)
        file_hanler.write_content(inst.content)

if __name__ == "__main__":

    start = time.time()

    get_logger(logger, app_log)
    run_script()

    end = time.time()

    print(f'{end - start} seconds needed.')