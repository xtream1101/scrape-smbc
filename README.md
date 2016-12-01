# scrape-smbc

Developed using Python 3.4

Must pass in a config file like so: `python3 smbc-comics.py -c ~/scrapers.conf`

See what the conf file need to be here: https://github.com/xtream1101/scraper-lib

## Setup

Run `pip3 install -r requirements.txt`


Scrape the site http://www.smbc-comics.com/ and save all the comics on the site and get new ones on each run.

This scraper also requires the section in the config:
```
[smbc-comics]
# `scraper_key` is only needed if `scraper-monitor` is enabled
scraper_key =
```

