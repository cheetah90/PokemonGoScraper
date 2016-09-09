#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Search Architecture:
 - Have a list of accounts
 - Create an "overseer" thread
 - Search Overseer:
   - Tracks incoming new location values
   - Tracks "paused state"
   - During pause or new location will clears current search queue
   - Starts search_worker threads
 - Search Worker Threads each:
   - Have a unique API login
   - Listens to the same Queue for areas to scan
   - Can re-login as needed
   - Shares a global lock for map parsing
'''

import logging
import time
import s2sphere

from threading import Thread, Lock
import gc
import pickle

from pgoapi import PGoApi
from pgoapi.utilities import f2i
from pgoapi import utilities as util
from pgoapi.exceptions import AuthException

from .models import parse_map

log = logging.getLogger(__name__)

TIMESTAMP = '\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000'


#
# A fake search loop which does....nothing!
#
def fake_search_loop():
    while True:
        log.info('Fake search loop running')
        time.sleep(10)


# The main search loop that keeps an eye on the over all process
def search_overseer_thread(args, new_location_queue, pause_bit, encryption_lib_path):

    log.info('Search overseer starting')

    search_items_queue = new_location_queue
    parse_lock = Lock()
    cell_id_lock = Lock()

    # Create a search_worker_thread per account
    log.info('Starting search worker threads')
    for i, account in enumerate(args.accounts):
        log.debug('Starting search worker thread %d for user %s', i, account['username'])
        t = Thread(target=search_worker_thread,
                   name='search_worker_{}'.format(i),
                   args=(args, account, search_items_queue, parse_lock,
                         encryption_lib_path, cell_id_lock))
        t.daemon = True
        t.start()

    # A place to track the current location
    current_location = False

    # The real work starts here but will halt on pause_bit.set()
    while True:
        # # paused a little bit to dump the current remaining cells to pickle
        time.sleep(60)
        pickle.dump(args.remaining_cells, open("remaining_cells_id_{}.pickle".format(args.db), "wb"))
        log.info("Dump the remaining_cells object")


        if search_items_queue.qsize() == 0:
            with parse_lock:
                log.info("Finished!")
                return


def search_worker_thread(args, account, search_items_queue, parse_lock, encryption_lib_path, cellid_lock):

    # If we have more than one account, stagger the logins such that they occur evenly over scan_delay
    if len(args.accounts) > 1:
        delay = (args.scan_delay / len(args.accounts)) * args.accounts.index(account)
        log.debug('Delaying thread startup for %.2f seconds', delay)
        time.sleep(delay)

    log.debug('Search worker thread starting')

    # The forever loop for the thread
    while True:
        try:
            #wait a bit before we start
            time.sleep(5)

            search_counter = 0
            log.debug('Entering search loop')

            # Create the API instance this will use
            api = PGoApi()
            if args.proxy:
                api.set_proxy({'http': args.proxy, 'https': args.proxy})

            # Get current time
            loop_start_time = int(round(time.time() * 1000))

            # The forever loop for the searches
            while True:
                # Grab the next thing to search (when available)
                if search_items_queue.qsize() == 0:
                    break

                while True:
                    cell_id_long = search_items_queue.get()
                    with cellid_lock:
                        if cell_id_long in args.remaining_cells:
                            break

                current_cell_id = s2sphere.CellId(id_=cell_id_long)
                lat = current_cell_id.to_lat_lng().lat().degrees
                lng = current_cell_id.to_lat_lng().lng().degrees
                alt = 0
                step_location = (lat, lng, alt)

                log.info('Search beginning (queue size is %d)', search_items_queue.qsize())

                # Let the api know where we intend to be for this loop
                api.set_position(*step_location)

                # The loop to try very hard to scan this step
                failed_total = 0
                while True:

                    # After so many attempts, let's get out of here
                    if failed_total >= args.scan_retries:
                        # I am choosing to NOT place this item back in the queue
                        # otherwise we could get a "bad scan" area and be stuck
                        # on this overall loop forever. Better to lose one cell
                        # than have the scanner, essentially, halt.
                        log.error('Search step %d went over max scan_retires; abandoning')
                        break

                    # Increase sleep delay between each failed scan
                    # By default scan_dela=5, scan_retries=5 so
                    # We'd see timeouts of 5, 10, 15, 20, 25
                    sleep_time = args.scan_delay * (1 + failed_total)

                    # Ok, let's get started -- check our login status
                    check_login(args, account, api, step_location)

                    api.activate_signature(encryption_lib_path)

                    # Make the actual request (finally!)
                    response_dict = map_request(api, step_location)

                    # G'damnit, nothing back. Mark it up, sleep, carry on
                    if not response_dict:
                        log.error('Search area download failed, retrying request in %g seconds', sleep_time)
                        failed_total += 1
                        time.sleep(sleep_time)
                        continue

                    # Got the response, lock for parsing and do so (or fail, whatever)
                    with parse_lock:
                        try:
                            parse_map(response_dict, cellid_lock, args.remaining_cells)
                            with cellid_lock:
                                if cell_id_long in args.remaining_cells:
                                    args.remaining_cells.remove(cell_id_long)
                                    break
                            log.debug('Search step completed')
                            # if search queue is empty that means we've finished scrapingm, exist
                            if search_items_queue.qsize() == 0:
                                log.info("Exist thread!")
                                return
                            search_items_queue.task_done()
                            break  # All done, get out of the request-retry loop
                        except KeyError:
                            log.exception('Search step map parsing failed, retrying request in %g seconds', sleep_time)
                            failed_total += 1
                            time.sleep(sleep_time)

                # If there's any time left between the start time and the time when we should be kicking off the next
                # loop, hang out until its up.
                sleep_delay_remaining = loop_start_time + (args.scan_delay * 1000) - int(round(time.time() * 1000))
                if sleep_delay_remaining > 0:
                    time.sleep(sleep_delay_remaining / 1000)

                loop_start_time += args.scan_delay * 1000

                # need to break out this while loop after 25 searches and re-login to avoid banned
                if search_counter >= 15:
                    # break out o the loop
                    api = ""
                    gc.collect()
                    time.sleep(200)
                    break
                else:
                    search_counter += 1

        # catch any process exceptions, log them, and continue the thread
        except Exception as e:
            log.exception('Exception in search_worker: %s. Username: %s', e, account['username'])


def check_login(args, account, api, position):

    # Logged in? Enough time left? Cool!
    if api._auth_provider and api._auth_provider._ticket_expire:
        remaining_time = api._auth_provider._ticket_expire / 1000 - time.time()
        if remaining_time > 60:
            log.debug('Credentials remain valid for another %f seconds', remaining_time)
            return

    # Try to login (a few times, but don't get stuck here)
    i = 0
    api.set_position(position[0], position[1], position[2])
    while i < args.login_retries:
        try:
            api.set_authentication(provider=account['auth_service'], username=account['username'], password=account['password'])
            break
        except AuthException:
            if i >= args.login_retries:
                raise TooManyLoginAttempts('Exceeded login attempts')
            else:
                i += 1
                log.error('Failed to login to Pokemon Go with account %s. Trying again in %g seconds', account['username'], args.login_delay)
                time.sleep(args.login_delay)

    log.debug('Login for account %s successful', account['username'])


def map_request(api, position):
    try:
        cell_ids = util.get_cell_ids(position[0], position[1])
        timestamps = [0, ] * len(cell_ids)
        return api.get_map_objects(latitude=f2i(position[0]),
                                   longitude=f2i(position[1]),
                                   since_timestamp_ms=timestamps,
                                   cell_id=cell_ids)
    except Exception as e:
        log.warning('Exception while downloading map: %s', e)
        return False


class TooManyLoginAttempts(Exception):
    pass
