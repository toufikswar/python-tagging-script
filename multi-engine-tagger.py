#!/usr/bin/python

# Standard libraries
import argparse
import datetime
import glob
import logging
from logging.handlers import RotatingFileHandler
import os
import os.path
import sys
import time
import xml.etree.ElementTree as Xml

# 3rd Party Libraries
import requests

# Custom classes
from classes.nxql import Nxql
from classes.appliance import Appliance
from classes.websession import WebSession
import classes.functions as functions

# Script execution path
path = os.path.dirname(os.path.abspath(__file__))

def clear_tags(nxql, all_engines, object_type, category, logger):
    """Clear the category from all Engines"""
    success = True

    # First clear the tag from all engines
    nxql.engine = all_engines

    # Get the Object Type and Category Name from the first row
    logger.info('Removing any existing tags from {} objects for Category "{}"...'.format(object_type, category))

    # We add the update query to the NXQL object
    nxql.clean_category_query(category, object_type)

    # Create the URL with the correct Format
    nxql.prepare_url_2()

    # Run the requests and get the response as a list
    results_clean = nxql.run_request_2()

    # Sleep 10 seconds to give the Engines time to quiesce
    time.sleep( 10 )

    # Iterate over the responses return by the get requests
    for hostname, response, response_code in results_clean:
        logger.debug('Hostname: "{}" returned http response code: {}.'.format(hostname, response_code))
        if response_code:
            template = 'Tags successfully cleaned for Category "{0}" on Engine: {1}'
            message = template.format(category, hostname)
            logger.info(message)
        else:
            template = 'Unable to clean tags for Category "{0}" on Engine: {1}'
            message = template.format(category, hostname)
            logger.error(message)
            # Return unsuccessful if any engines throw an error
            success = False

    return success

def tag_device(config_file_name, tags_file, nxql, all_engines, logger):

    # Read tags from CSV files
    # Assumption that all rows of the file are for the same object type and category
    tags = functions.read_csv_file(tags_file)
    object_type = tags[0]["Object Type"]
    category = tags[0]["Category"]

    # First clear the tag from all engines (end if not successful)
    result = clear_tags(nxql, all_engines, object_type, category, logger)
    if not result: return

    # Get the object identity query from the config file
    id_column, id_query = functions.get_object_query(config_file_name, object_type, category, logger)

    # Prepare to run the per-engine update process
    nxql.engine = all_engines
    nxql.prepare_for_engine_object_updates(id_query, id_column, tags)

    # Run the multi-engine process on the current set of tags
    tag_results = nxql.process_engine_objects()

    # Dump the results to the log
    all_updates = 0
    all_failures = 0
    updated_ids = []
    logger.info('Results for updating Category "{}":'.format(category))
    for tag_result in tag_results:
        if tag_result["num_failures"] > 0:
            logger.error('\tEngine: "{url}", {num_updates} Sucessful Updates, {num_failures} Failed Updates.'.format(**tag_result))
        else:
            logger.info('\tEngine: "{url}", {num_updates} Sucessful Updates, {num_failures} Failed Updates.'.format(**tag_result))
        all_updates += tag_result["num_updates"]
        all_failures += tag_result["num_failures"]
        updated_ids.extend(tag_result["updated_ids"])

    # Put the total processed into the log
    if all_failures > 0:
        logger.error('Tagged Category "{}" on {} {}s with {} failures across all {} Engines.'.format(
            category, all_updates, object_type, all_failures, len(all_engines)))
    else:
        logger.info('Tagged Category "{}" on {} {}s with {} failures across all {} Engines.'.format(
            category, all_updates, object_type, all_failures, len(all_engines)))

    # Now, determine which items were not tagged
    object_ids_to_be_tagged = [tag["Object ID"] for tag in tags]
    missed_object_ids = sorted(set(object_ids_to_be_tagged) - set(updated_ids))
    logger.debug('missed_object_ids: {}'.format(missed_object_ids))

    return (all_failures == 0), missed_object_ids

def main():

    # Define argument parser
    parser = argparse.ArgumentParser()
    # Define the arguments
    parser.add_argument("config_file", help="xml file containing the configuration parameters")
    parser.add_argument("query_file", help="xml file in which the queries are located")
    parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
    args = parser.parse_args()

    # Get tag file and log file path from the configuration file
    tags_path, log_path = functions.get_paths(args.config_file)
    if tags_path is None or log_path is None: exit(-1)

    # Create the logger
    logger = logging.getLogger(__name__)
    # Configure logger
    app_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    rundate = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
    log_name = '{}{}.{}.log'.format(log_path, app_name, rundate)
    handler = RotatingFileHandler(log_name, maxBytes=100000000, backupCount=5)
    formatter = logging.Formatter('%(asctime)s - %(levelname)-8s %(message)s', '%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Set log level to debug if argument passed
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    os.chdir(path)

    logger.info("====== Starting Multi Engine Tagging ======")

    # Get Portal credentials from credentials.xml
    portal_credentials = functions.get_credentials(args.config_file, logger)
    # Get the Portal's fqdn
    portal_fqdn, portal_port = functions.get_portal(args.config_file, logger)

    # create a session object
    websession = WebSession(portal_credentials)
    session = websession.create_session()

    # Create a Portal Object for making API Call (Portal)
    portal = Appliance(portal_fqdn,"Portal", portal_port, portal_credentials, session, logger)

    # Create NXQL object (passing the logger)
    nxql = Nxql(websession, logger)
    
    # Get list of connected engines (via API call)
    all_engines = portal.get_engines_list()
    if all_engines:

        # We check if the tag directory contains tags csv files
        csv_files = glob.glob(os.path.join(tags_path, '*.csv')) 
        if not csv_files:
            logger.error('Tags directory ({}) contains no .csv files to process'.format(tags_path))
        else:
            for fullpath in csv_files:
                # Open and process each file
                print('Processing: {}...'.format(fullpath))
                logger.info("###### Starts tagging file => " + fullpath + " ######")
                success, missed_object_ids = tag_device(args.query_file, fullpath, nxql, all_engines, logger)
                logger.info("###### Ends tagging file => " + fullpath + " ######")
                # Write the missed_object_ids to a similarly named file
                if missed_object_ids and len(missed_object_ids) > 0:
                    missed_path = '{}.{}.missing'.format(fullpath, rundate)
                    with open(missed_path, "w") as missed:
                        for id in missed_object_ids:
                            missed.write(id + '\r\n')
                    logger.info('Wrote {} object idntiefiers that were not found to: {}'.format(len(missed_object_ids), missed_path))
                # Rename the file once completed
                if success:
                    new_name = '{}.{}.success'.format(fullpath, rundate)
                    os.rename(fullpath, new_name)
                    logger.info("###### Renaming successfully complete tagging file => " + new_name + " ######")
                    print('Processing completed successfuly: {}'.format(new_name))
                else:
                    new_name = '{}.{}.failed'.format(fullpath, rundate)
                    os.rename(fullpath, new_name)
                    logger.error("###### Renaming unsuccessful (errors occurred) tagging file => " + new_name + " ######")
                    print('Processing completed with errors: {}'.format(new_name))

    else:
        logger.error("No Engines found - Exiting Program")
        raise SystemExit()

    logger.info("====== Script execution completed ======")

if __name__ == "__main__":
    # execute only if run as a script
    main()
