import os
import sys
import time
import cutil
import signal
import logging
from pprint import pprint
from scraper_monitor import scraper_monitor
from models import db_session, Setting, Comic, NoResultFound, DBSession
from scraper_lib import Scraper
from web_wrapper import DriverRequests


# Create logger for this script
logger = logging.getLogger(__name__)


class Worker:

    def __init__(self, scraper, web, comic_id):
        """
        Worker Profile

        Run for each item that needs parsing
        Each thread has a web instance that is used for parsing
        """
        # `web` is what utilizes the profiles and proxying
        self.web = web
        self.comic_id = comic_id
        self.scraper = scraper

        # Get the sites content as a beautifulsoup object
        logger.info("Getting comic {id}".format(id=self.comic_id))
        url = "http://www.smbc-comics.com/comic/{id}".format(id=self.comic_id)
        response = self.web.get_site(url, page_format='html')
        if response is None:
            logger.warning("Response was None for url {url}".format(url=url))

        else:
            parsed_data = self.parse(response)
            if len(parsed_data) > 0:
                # Add raw data to db
                self.scraper.insert_data(parsed_data)

                # Remove id from list of comics to get
                self.scraper.comic_ids.remove(self.comic_id)

                # Add success count to stats. Keeps track of how much ref data has been parsed
                self.scraper.track_stat('ref_data_success_count', 1)

        # Take it easy on the site
        time.sleep(1)

    def parse(self, soup):
        """
        :return: List of items with their details
        """
        # Adds title
        rdata = self.scraper.archive_list.get(self.comic_id)

        # Parse the items here and return the content to be added to the db
        comic_raw = soup.find(id='cc-comic')
        img_src = comic_raw['src']

        comic_filename = '{base}/{year}/{month}/{name}{ext}'\
                         .format(base=self.scraper.BASE_SAVE_DIR,
                                 year=rdata['posted_at'].year,
                                 month=rdata['posted_at'].month,
                                 name=str(rdata['posted_at']),
                                 ext=cutil.get_file_ext(img_src))
        rdata.update({'time_collected': cutil.get_datetime(),
                      'file_path': self.web.download(img_src, comic_filename)
                                           .replace(self.scraper.BASE_DATA_DIR + os.path.sep, ''),
                      'alt': comic_raw['title']
                      })

        return rdata


class SMBCComics(Scraper):

    def __init__(self, config_file=None):
        super().__init__('smbc')

        self.newest_comic_id = None
        self.scraped_ids = self.get_scraped()
        self.archive_list = self.load_archive_list()
        self.comic_ids = list(set(list(self.archive_list.keys())) - set(self.scraped_ids))

    def start(self):
        """
        Send the ref data to the worker threads
        """
        if len(self.comic_ids) == 0:
            # No need to continue
            logger.info("Already have the newest comic")
            return

        # Log how many items in total we will be parsing
        self.stats['ref_data_count'] = len(self.comic_ids)

        # Only ever use 1 thread here
        self.thread_profile(1, DriverRequests, self.comic_ids, Worker)

    def load_archive_list(self):
        """
        Load all the comics and store in a dict with the id's as keys
        Need to do this since this is the only place where the date posted is listed
        """
        rdata = {}
        tmp_web = DriverRequests()

        url = "http://www.smbc-comics.com/comic/archive"
        try:
            soup = tmp_web.get_site(url, page_format='html')

        except Exception:
            logger.critical("Problem getting comic archive", exc_info=True)
            sys.exit(1)

        archive = soup.find('select', {'name': 'comic'}).find_all('option')

        # Check the newest first (last in list so reverse)
        for entry in archive[::-1]:
            try:
                if entry.text == 'Select a comic...':
                    continue

                comic_id = entry['value']
                posted_at, comic_title = entry.text.split('-', 1)

                # Get last comic posted
                if self.newest_comic_id is None:
                    self.newest_comic_id = comic_id

                rdata[comic_id] = {'id': comic_id,
                                   'title': comic_title.strip(),
                                   'posted_at': cutil.str_to_date(posted_at.strip(), formats=['%B %d, %Y']).date(),
                                   }

            except Exception:
                logger.exception("Could not get id or title from comic {title}".format(title=entry.text))

        return rdata

    def get_scraped(self):
        """
        Get last comic scraped
        """
        results = db_session.query(Setting).with_entities(Comic.comic_id)

        scraped_ids = [r[0] for r in results]

        return scraped_ids

    def log_last_scraped(self):
        try:
            setting = db_session.query(Setting).filter(Setting.bit == 0).one()
            setting.comic_last_ran = cutil.get_datetime()

            db_session.add(setting)
            db_session.commit()

        except:
            logger.exception("Problem logging last comic scraped")

    def insert_data(self, data):
        """
        Will handle inserting data into the database
        """
        try:
            db_session = DBSession()
            # Check if comic is in database, if so update else create
            try:
                comic = db_session.query(Comic).filter(Comic.comic_id == data.get('comic_id')).one()
            except NoResultFound:
                comic = Comic()

            comic.title = data.get('title')
            comic.comic_id = data.get('id')
            comic.alt = data.get('alt')
            comic.file_path = data.get('file_path')
            comic.posted_at = data.get('posted_at')
            comic.time_collected = data.get('time_collected')

            db_session.add(comic)
            db_session.commit()

        except Exception:
            db_session.rollback()
            logger.exception("Error adding to db {data}".format(data=data))


def sigint_handler(signal, frame):
    logger.critical("Keyboard Interrupt")
    pprint(scraper.stats)
    sys.exit(0)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint_handler)

    try:
        # Setup the scraper
        scraper = SMBCComics()
        try:
            # Start scraping
            scraper.start()
            scraper.cleanup()
            pprint(scraper.stats)

        except Exception:
            logger.critical("Main Error", exc_info=True)

    except Exception:
        logger.critical("Setup Error", exc_info=True)

    finally:
        scraper.log_last_scraped()
        try:
            # Log stats
            scraper_monitor.stop(total_urls=scraper.stats['total_urls'],
                                 ref_data_count=scraper.stats['ref_data_count'],
                                 ref_data_success_count=scraper.stats['ref_data_success_count'],
                                 rows_added_to_db=scraper.stats['rows_added_to_db'])

        except NameError:
            # If there is an issue with scraper.stats
            scraper_monitor.stop()

        except Exception:
            logger.critical("Scraper Monitor Stop Error", exc_info=True)
            scraper_monitor.stop()
