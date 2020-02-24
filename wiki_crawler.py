from bs4 import BeautifulSoup, SoupStrainer
import requests
import pandas as pd
import os, re, sys
from typing import List
import logging
from logging.handlers import RotatingFileHandler

logger = logging.getLogger('Info')
app_log = logging.getLogger('ContentWriter')

class Settings:
    starting_link = r'https://en.wikipedia.org/wiki/Canada'
    wiki_link = r'https://en.wikipedia.org'

    # for windows
    # path_dir = r'C:\Python\Projects\Testing\Data'
    # for linux
    path_dir = r'/home/admin/WikiCrawler/data'


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
    maximum_links = 20

    # how many pages with content to store in memory until it is dumped to file
    pages_in_memory = 20

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
        # self.app_log = None # type: # None|logging

        # Response object, page from wikipedia, for each link separately
        self.page = None # type: None|requests.models.Response

        # Actual link to get Response object from
        self.link = '' # type: str

        # Set of unique links
        self.unique_links = set([]) # type: set

        # contains already used, clicked links from unique set
        self.used_links = [] # type: List[str, str, ...]

        # contains content got from each unique link
        self.content = [] # type: List[str, str, ...]

        # Provides instance of FileHandler to save, and open files
        self.file_handler = FileHandler() # type: FileHandler

        logger.info(f'Working in {repr(WikiCrawler.__name__)}')

        self.check_if_links_saved_in_files()
        self.get_wiki_page()
        self.collect_unique_links()
        self.get_content_from_page()

        self.link_engine()


    def check_if_links_saved_in_files(self):
        if not os.path.isfile(Settings.filepath_used_links) \
                or not os.path.isfile(Settings.filepath_unique_links):

            logger.info(f'{repr(Settings.used_links_file)} and {repr(Settings.unique_links_file)} '
                  f'dont exist. Starting the script from initial link: {Settings.starting_link}')

        else:
            self.used_links = self.file_handler.open_links(
                Settings.filepath_used_links, "used")
            self.unique_links = self.file_handler.open_links(
                Settings.filepath_unique_links, "unique")
            Settings.starting_link = None
            self.link = self.unique_links.pop()

    def get_wiki_page(self):
        self.page = requests.get(
            f'{Settings.wiki_link}{self.link}' if not Settings.starting_link
            else Settings.starting_link)
        if Settings.starting_link is not None: self.used_links.append(Settings.starting_link)
        Settings.starting_link = None

    def collect_unique_links(self):
        soup = self.get_soup(parse_only=self.links_only)
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

    def get_content_from_page(self):
        soup = self.get_soup(parse_only=self.text_section)
        for line in soup.children:
            self.content.append(line.get_text())

    def link_engine(self):
        while len(self.used_links) <= Settings.maximum_links:
            self.link = self.unique_links.pop()
            self.get_wiki_page()
            self.collect_unique_links()
            self.get_content_from_page()
            self.used_links.append(self.link)

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

    def get_soup(self, parse_only):
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

    def open_links(self, filepath_links, cond="unique"):
        df = pd.read_csv(filepath_links)
        if cond == "unique":
            links = set(df['Links'].values)
            logger.info(f'File with {repr(cond)} links opened. '
                        f'{len(links)} links from: {repr(filepath_links)}.')
            return links
        elif cond == "used":
            links = df['Links'].values.tolist()
            logger.info(f'File with {repr(cond)} links opened. '
                        f'{len(links)} links from: {repr(filepath_links)}.')
            return links

    def write_content(self, content):
        logger.info(f'Content saved. {len(content)} Lines to {Settings.content_file}')
        for line in content:
            app_log.info(line)

class Processing(WikiCrawler):
    pass

class Timing(WikiCrawler):
    pass


def get_logger():
    app_log_path = os.path.join(Settings.path_dir, Settings.content_file)

    my_handler = RotatingFileHandler(app_log_path, mode='a', maxBytes=Settings.content_file_size,
                                     encoding='utf-8', backupCount=Settings.backupcount)
    my_handler.setLevel(logging.INFO)
    app_log.setLevel(logging.INFO)
    app_log.addHandler(my_handler)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    sh = logging.StreamHandler()
    fh = logging.FileHandler('Info.log')

    sh.setFormatter(formatter)

    logger.setLevel(logging.INFO)

    logger.addHandler(sh)
    logger.addHandler(fh)
    return logger, app_log


def run_script():
    inst = None # type: None|WikiCrawler
    try:
        logger.info('Starting Script')
        inst = WikiCrawler()
        logger.info('Script finisched')
    except:
        logger.error('Something went WRONG!')
        file_hanler = FileHandler()

        file_hanler.save_links(inst.unique_links, Settings.filepath_unique_links)
        file_hanler.save_links(inst.used_links, Settings.filepath_used_links)
        file_hanler.write_content(inst.content)

if __name__ == "__main__":
    logger, app_log = get_logger()
    run_script()