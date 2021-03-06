#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import shutil
import logging
import time
import re
import requests

from distutils.version import StrictVersion
from random import shuffle

import s2sphere, pickle
from threading import Thread, Event
from queue import Queue
from flask_cache_bust import init_cache_busting

from pogom import config
from pogom.app import Pogom
from pogom.utils import get_args, insert_mock_data, get_encryption_lib_path

from pogom.search import search_overseer_thread, fake_search_loop
from pogom.models import init_database, create_tables, drop_tables

# Currently supported pgoapi
pgoapi_version = "1.1.7"

# Moved here so logger is configured at load time
logging.basicConfig(format='%(asctime)s [%(threadName)16s][%(module)14s][%(levelname)8s] %(message)s')
log = logging.getLogger()

# Make sure pogom/pgoapi is actually removed if it is an empty directory
# This is a leftover directory from the time pgoapi was embedded in PokemonGo-Map
# The empty directory will cause problems with `import pgoapi` so it needs to go
oldpgoapiPath = os.path.join(os.path.dirname(__file__), "pogom/pgoapi")
if os.path.isdir(oldpgoapiPath):
    log.info("I found %s, but its no longer used. Going to remove it...", oldpgoapiPath)
    shutil.rmtree(oldpgoapiPath)
    log.info("Done!")

# Assert pgoapi is installed
try:
    import pgoapi
    from pgoapi import utilities as util
except ImportError:
    log.critical("It seems `pgoapi` is not installed. You must run pip install -r requirements.txt again")
    sys.exit(1)

# Assert pgoapi >= pgoapi_version
if not hasattr(pgoapi, "__version__") or StrictVersion(pgoapi.__version__) < StrictVersion(pgoapi_version):
    log.critical("It seems `pgoapi` is not up-to-date. You must run pip install -r requirements.txt again")
    sys.exit(1)

def get_covering_cells_id(minx, miny, maxx, maxy):
    r = s2sphere.RegionCoverer()
    r.min_level = 15
    r.max_level = 15
    p1 = s2sphere.LatLng.from_degrees(miny,minx)
    p2 = s2sphere.LatLng.from_degrees(maxy, maxx)
    cell_ids = r.get_covering(s2sphere.LatLngRect.from_point_pair(p1, p2))
    cell_ids_long = [cell._CellId__id for cell in cell_ids]

    return cell_ids_long

def setup_params():
    config['parse_pokemon'] = not args.no_pokemon
    config['parse_pokestops'] = not args.no_pokestops
    config['parse_gyms'] = not args.no_gyms

def from_cellid_to_center_location(cell_id):
    current_cell_id = s2sphere.CellId(id_=cell_id)
    lat = current_cell_id.to_lat_lng().lat().degrees
    lng = current_cell_id.to_lat_lng().lng().degrees
    alt = 0
    step_location = (lat, lng, alt)

    return step_location

if __name__ == '__main__':


    # Check if we have the proper encryption library file and get its path
    encryption_lib_path = get_encryption_lib_path()
    if encryption_lib_path is "":
        sys.exit(1)

    args = get_args()

    if args.debug:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    setup_params()

    # Let's not forget to run Grunt / Only needed when running with webserver
    if not args.no_server:
        if not os.path.exists(os.path.join(os.path.dirname(__file__), 'static/dist')):
            log.critical('Missing front-end assets (static/dist) -- please run "npm install && npm run build" before starting the server')
            sys.exit()

    # These are very noisey, let's shush them up a bit
    logging.getLogger('peewee').setLevel(logging.INFO)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('pgoapi.pgoapi').setLevel(logging.WARNING)
    logging.getLogger('pgoapi.rpc_api').setLevel(logging.INFO)
    logging.getLogger('werkzeug').setLevel(logging.ERROR)

    # use lat/lng directly if matches such a pattern
    prog = re.compile("^(\-?\d+\.\d+),?\s?(\-?\d+\.\d+)$")
    res = prog.match(args.location)
    if res:
        log.debug('Using coordinates from CLI directly')
        position = (float(res.group(1)), float(res.group(2)), 0)
    else:
        log.debug('Looking up coordinates in API')
        position = util.get_pos_by_name(args.location)

    # Use the latitude and longitude to get the local altitude from Google
    try:
        url = 'https://maps.googleapis.com/maps/api/elevation/json?locations={},{}'.format(
            str(position[0]), str(position[1]))
        altitude = requests.get(url).json()[u'results'][0][u'elevation']
        log.debug('Local altitude is: %sm', altitude)
        position = (position[0], position[1], altitude)
    except (requests.exceptions.RequestException, IndexError, KeyError):
        log.error('Unable to retrieve altitude from Google APIs; setting to 0')

    if not any(position):
        log.error('Could not get a position by name, aborting')
        sys.exit()

    log.info('Parsed location is: %.4f/%.4f/%.4f (lat/lng/alt)',
             position[0], position[1], position[2])

    config['LOCALE'] = args.locale
    config['CHINA'] = args.china

    app = Pogom(__name__)
    db = init_database(app)
    create_tables(db)

    #login credentials
    login_credentials = []
    login_credentials.append({"username": "cheetah90.apple", "password": "Star2Night", "auth_service": "google" })
    login_credentials.append({"username": "cheetah90.chicago", "password": "Star2Night", "auth_service": "google"})
    login_credentials.append({"username": "cheetah90.evanston", "password": "Star2Night", "auth_service": "google"})
    args.accounts = login_credentials

    # Control the search status (running or not) across threads
    pause_bit = Event()
    pause_bit.clear()

    new_location_queue = Queue()
    # if the file exist, this query has been interrupted. So we just resume the query
    if os.path.isfile("remaining_cells_id_{}.pickle".format(args.db)):
        all_cells_id = pickle.load(open("remaining_cells_id_{}.pickle".format(args.db), "rb"))
    else: #if the file does not exist, we create it first
        log.info("Compute the cell ids, could take a while...")
        all_cells_id = get_covering_cells_id(args.minx, args.miny, args.maxx, args.maxy)
        pickle.dump(all_cells_id, open("remaining_cells_id_{}.pickle".format(args.db), "wb"))

    #shuffle the order of the cell ids so that it's pseudo random access to maximize the coverage
    shuffle(all_cells_id)
    for cellid in all_cells_id:
        new_location_queue.put(cellid)
    args.remaining_cells = all_cells_id

    if len(args.remaining_cells):
        app.set_current_location(from_cellid_to_center_location(all_cells_id[0]))
    else:
        app.set_current_location(position)

    if not args.only_server:
        # Gather the pokemons!
        if not args.mock:
            log.debug('Starting a real search thread')
            search_thread = Thread(target=search_overseer_thread, args=(args, new_location_queue, pause_bit, encryption_lib_path))
        else:
            log.debug('Starting a fake search thread')
            insert_mock_data(position)
            search_thread = Thread(target=fake_search_loop)

        search_thread.daemon = True
        search_thread.name = 'search_thread'
        search_thread.start()


    # No more stale JS
    init_cache_busting(app)

    app.set_search_control(pause_bit)
    app.set_location_queue(new_location_queue)

    config['ROOT_PATH'] = app.root_path
    config['GMAPS_KEY'] = args.gmaps_key

    while search_thread.is_alive():
        time.sleep(60)
