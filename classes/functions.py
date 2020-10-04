#!/usr/bin/python
# Copyright (C) 2017 Nexthink SA, Switzerland

# Library import
import csv
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
import os
from os.path import basename
import re
import smtplib
import subprocess
import sys
import xml.etree.ElementTree as Xml

logger = logging.getLogger('nxql')

def send_mail(subject, text, attachment, recipients):
    """Send email function (FROM PREVOUS CODE)

    Function to send a email using the SMTP configuration from the Appliance.
    Works only if no authentication is needed on the SMTP server

    Args:
        subject: the email subject
        text: text of the email
        attachment: file to attach to the email
        recipients: list of recipients

    """

    logger.info("Preparing email to be sent ...")

    # Retrieve SMTP parameters from Appliance config file
    try:
        server = Xml.parse('/var/nexthink/common/conf/smtp-config.xml').find('Smtp/Server').text
        port = Xml.parse('/var/nexthink/common/conf/smtp-config.xml').find('Smtp/Port').text
        send_from = Xml.parse('/var/nexthink/common/conf/smtp-config.xml').find('Smtp/From').text

        if not server:
            raise ValueError("empty SMTP server")

    except IOError as ex:
        template = "smtp-config.xml file not found. The file needs to be in /var/nexthink/common/conf/.\n " \
                   "An exception of type {0} occurred. Arguments: {1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logger.error(message)
    except TypeError as ex:
        template = "one of the tag is missing or incorrect.\n An exception of type {0} occurred. Arguments: {1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logger.error(message)
    except ValueError as ex:
        template = "No SMTP server configured.\n An exception of type {0} occurred. Arguments: {1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logger.error(message)
    else:

        template = "SMTP server: {0} Port: {1} Send from: {2} To: {3}"
        message = template.format(server, port, send_from, recipients)
        logger.debug(message)

        msg = MIMEMultipart()
        msg['From'] = send_from
        msg['Subject'] = subject
        if len(recipients) == 1:
            msg['To'] = str(recipients[0])
        else:
            msg['To'] = ", ".join(recipients)

        msg.attach(MIMEText(text))

        if attachment is not None:
            with open(attachment, "rb") as f:
                part = MIMEApplication(f.read(), Name=basename(attachment))
                part['Content-Disposition'] = 'attachment; filename='+basename(attachment)+''
            msg.attach(part)

        try:
            smtp = smtplib.SMTP(server, port)
            smtp.sendmail(send_from, recipients, msg.as_string())
            smtp.close()
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            logger.exception(message)
        else:
            logger.info("Email sent successfully")

def get_object_query(file_name, object_type, category, logger):
    """Function to get the first Query in the configuration file based on object_type

    Function that will parse the xml file provided to get all the query in the <Queries> tag.
    Note: the file need to be in the same directory as the script
    !! IMPORTANT: A CSV file should only contain ONE type of object

    Args:
        file_name: name of the file in which to look for the queries
        object_type: the type of object we are looking for the query for
        category: The name of the Category to find the query for
        logger: the logging object

    Return:
        str: object id column name
        str: NXQL Query
       
    """
    id_column = None
    nxql_query = None
    logger.info('Getting the NXQL query for object type "{}" from "{}"...'.format(object_type, file_name))

    try:
        for query in Xml.parse(file_name).getroot().find('Queries'):
            # we take the query that corresponds to the object in the tag files
            if query.get("objecttype") == object_type and query.get("category") == category:
                id_column = query.get("id_column")
                # We remove \n and \r characters from the query
                splited_query = query.text.split()
                nxql_query = " ".join(splited_query)
                break
            
        if not id_column:
            raise ValueError("ID Column not found")
        if not nxql_query:
            raise ValueError("Query not found")
        logger.debug('id_column: "{}", nxql_query: "{}"'.format(id_column, nxql_query))
        
    except IOError as ex:
        template = "{0} file not found.\n An exception of type {1} occurred. Arguments: {2!r}"
        message = template.format(file_name, type(ex).__name__, ex.args)
        logger.error(message)
        sys.exit(1)
    except AttributeError as ex:
        template = "tag <Queries> not found.\n An exception of type {0} occurred. Arguments: {1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logger.error(message)
        sys.exit(1)
    except ValueError as ex:
        template = "No query provided.\n An exception of type {0} occurred. Arguments: {1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logger.error(message)
        sys.exit(1)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logger.exception(message)
        sys.exit(1)
    else:
        logger.info("ID Column and Query retrieved successfully")
        return id_column, nxql_query

def get_query_2(file_name, tags, logger):
    """Function to get list of Queries in the configuration file

    Function that will parse the xml file provided to get all the query in the <Queries> tag.
    Note: the file need to be in the same directory as the script
    !! IMPORTANT: A CSV file should only contain ONE type of object

    Args:
        file_name: name of the file in which to look for the queries
        tags: a list of dictionaries with the different tags
        logger: the logging object

    Return:
        list_queries: list of tuple of (query, category, tag)

    """

    logger.info('Getting the NXQL queries from "{}"...'.format(file_name))
    list_queries = []

    # We get the type of object in the csv file - it get it from first column of first raw"
    objecttype = tags[0]["Object Type"]

    try:
        for query in Xml.parse(file_name).getroot().find('Queries'):
            # we take the query that corresponds to the object in the tag files
            if query.get("objecttype") == objecttype:
                # We remove \n and \r characters from the query
                splited_query = query.text.split()
                formated_query = " ".join(splited_query)
                # Replace object_id in the query with corresponding object ID from tag file
                list_queries = [(re.sub(r"\$(.*?)\$",tag["Object ID"],formated_query) , tag["Category"], tag["Keyword"], tag["Object Type"]) for tag in tags]
            
        if not list_queries:
            raise ValueError("Empty Query list")
        for i in len(list_queries):
            logger.debug('Tupple {}: {}'.format(i, list_queries[i]))

    except IOError as ex:
        template = "{0} file not found.\n An exception of type {1} occurred. Arguments: {2!r}"
        message = template.format(file_name, type(ex).__name__, ex.args)
        logger.error(message)
        sys.exit(1)
    except AttributeError as ex:
        template = "tag <Queries> not found.\n An exception of type {0} occurred. Arguments: {1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logger.error(message)
        sys.exit(1)
    except ValueError as ex:
        template = "No query provided.\n An exception of type {0} occurred. Arguments: {1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logger.error(message)
        sys.exit(1)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logger.exception(message)
        sys.exit(1)
    else:
        logger.info("Queries retrieved successfully")
        logger.debug("The generated queries are : " + str(list_queries))
        return list_queries

def read_csv_file(file_name):
    """ Function to read a CSV file
    
    Function that will read a CSV file and return a list of dictionaries

    Args:
        file_name: the name of the CSV file

    Return:
        a list of dictionaries with the different tags
  
    """
    try:
        with open(file_name, 'r') as file:

            tags_list = []
            # Read tag file and store tags as a dictionary
            csv_file = csv.DictReader(file)
            # Add all dictionaries in a list
            for row in csv_file:
                tags_list.append(dict(row))
        
        # If the file is empty or has only headers 
        if not tags_list:
            raise ValueError('No tags founds - program will close')
        else:
            return tags_list
    
    except ValueError as ex:
        template = "No tags in found.\n An exception of type {0} occurred. Arguments: {1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logger.error(message)
        sys.exit(1)

    except FileNotFoundError as ex:
        template = "File not found.\n An exception of type {0} occurred. Arguments: {1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logger.error(message)
        sys.exit(1)

def get_credentials(file_name, logger):
    """Function to get the username and password from a file

    Function that will parse the xml file provided to get the credentials.
    Note: the tags used are <Credentials>

    Args:
        file_name: name of the file in which to look for the queries

    Return:
        credentials_value: encoded base64 username:password

    """

    logger.info("Getting credentials from " + file_name + " ...")

    try:
        credentials_value = Xml.parse(file_name).find('Credentials').text

        if not credentials_value:
            raise ValueError("Empty credentials")

    except IOError as ex:
        template = "{0} file not found.\n An exception of type {1} occurred. Arguments: {2!r}"
        message = template.format(file_name, type(ex).__name__, ex.args)
        logger.error(message)
    except AttributeError as ex:
        template = "tag <Username> or <Password> not found.\n An exception of type {0} occurred. Arguments: {1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logger.error(message)
    except ValueError as ex:
        template = "No username or password provided.\n An exception of type {0} occurred. Arguments: {1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logger.error(message)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logger.exception(message)
    else:
        logger.info("Credentials retrieved successfully")
        return credentials_value

def get_portal(file_name, logger):
    """Function to get the portal fqdn from a file

    Function that will parse the xml file provided to get the Portal fqdn.
    Note: the tag used is <Portal>

    Args:
        file_name: name of the file in which to look for the element

    Return:
        portal_value: Portal fqdn

    """

    logger.info("Getting Portal FQDN from " + file_name + " ...")

    try:
        portal_value = Xml.parse(file_name).find('Portal').text
        port_value = Xml.parse(file_name).find('Port').text

        logger.debug("Portal: " + portal_value)
        logger.debug("Port: " + portal_value)


        if not portal_value:
            raise ValueError("Empty portal")
        if not port_value:
            raise ValueError("Empty port")


    except IOError as ex:
        template = "{0} file not found.\n An exception of type {1} occurred. Arguments: {2!r}"
        message = template.format(file_name, type(ex).__name__, ex.args)
        logger.error(message)
    except AttributeError as ex:
        template = "tag <Portal> not found.\n An exception of type {0} occurred. Arguments: {1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logger.error(message)
    except ValueError as ex:
        template = "No portal provided.\n An exception of type {0} occurred. Arguments: {1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logger.error(message)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logger.exception(message)
    else:
        logger.info("Portal FQDN retrieved successfully")
        return portal_value, port_value

def get_paths(file_name):
    """Function to get the Tag and log paths
    
    Function that will parse the xml file provided to get the log and tag configuration 
    Note: the tag used is <Tags> and <LogPath> respectively

    Args:
        file_name: name of the file in which to look for the element

    Return:
        tag_path_value: Path of where tags csv file will be stored
        log_path_value: Path of where the log file will be stored

    """
    tag_path_value = None
    log_path_value = None

    logger.info("Getting Tag and Log File paths from " + file_name + " ...")

    try:
        tag_path_value = Xml.parse(file_name).find('Tags').text
        if not tag_path_value:
            raise ValueError("Tags file path not provided")
        print('Tag files will be read from: {}'.format(tag_path_value))
        
        log_path_value = Xml.parse(file_name).find('LogPath').text
        if not log_path_value:
            raise ValueError("Log file path not provided")
        print('Log file will be written to: {}'.format(log_path_value))
    except IOError as ex:
        template = "{0} file not found.\n An exception of type {1} occurred. Arguments: {2!r}"
        message = template.format(file_name, type(ex).__name__, ex.args)
        print(message)
    except AttributeError as ex:
        template = "Missing XML tag.\n An exception of type {0} occurred. Arguments: {1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
    except ValueError as ex:
        template = "Configuration info missing.\n An exception of type {0} occurred. Arguments: {1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
    else:
        print("Tags and Log Path configuration retrieved successfully")

    return tag_path_value, log_path_value

def umount(folder):
    """Function to unmount a folder (FROM PREVOUS CODE)

    Function used to unmount a mounted folder

    Args:
        folder: mounted folder

    """

    logger.info("Umounting folder " + folder + " ...")

    # UnMount the specific folder
    if os.path.ismount(folder):
        try:
            os.system("umount " + folder)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            logger.exception(message)
        else:
            logger.info("Unmounted successfully")
    else:
        logger.info("Folder was not mounted ...")

def mount(user, pwd, share_path, folder):
    """Function mount a folder to a share (FROM PREVOUS CODE)

    Function used to mount a folder on the Appliance to a share

    Args:
        user: user to mount the share
        pwd: password of the user
        share_path: path of the share folder on the remote system
        folder: folder which will be mounted to the share

    """

    logger.info("Mounting folder " + folder + " to " + share_path + " ...")

    # In case backslash were used, replace them with slash
    share_path.replace('\\', '/')

    # Create temporary folder if not existing
    if not os.path.exists(folder):
        os.makedirs(folder)

    if not user:
        command = "mount -t cifs -o " + share_path + " " + folder
    else:
        command = "mount -t cifs -o username=" + user + ",password=" + pwd + " " + share_path + " " + folder

    logger.debug(command)

    if not os.path.ismount(folder):

        # Mount the specific share_path to the folder
        try:
            os.system(command)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            logger.exception(message)
    else:
        logger.info("Folder already mounted, possibly to another share.")
        umount(folder)
        try:
            os.system(command)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            logger.exception(message)

    logger.info("Mounted successfully to " + share_path)
