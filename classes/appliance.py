import requests
import base64
import logging
import json
import urllib3
import sys


from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

urllib3.disable_warnings()


class Appliance:

    
    def __init__(self, hostname_fqdn, name, port, credentials,session, logger):
        self.logger = logger
        self._hostname_fqdn = hostname_fqdn
        self._name = name
        self._port = port
        self._credentials = credentials
        self._session = session
        
        
        


    def __repr__(self):
        return f"Appliance : {self._name} Hostname : {self._hostname_fqdn} Port : {self._port}"



    def _get_engines_json(self):
        """ Private method that queries the portal API and gets a list of all available engines

        Return:
            JSON object with engines hostnames
        """
        
        
        # We get the session attribute
        session = self._session


        try:
            self.logger.debug("Querying the API to get a list of all engines...")
            response = session.get(f"https://"+self._hostname_fqdn+"/api/configuration/v1/engines", verify=True)
            response.raise_for_status()

        except requests.exceptions.HTTPError as err:
            self.logger.error(err)
            raise SystemExit(err)

        except requests.exceptions.HTTPError as HTTPerr:
            self.logger.error("HTTP Error : " + HTTPerr)
            self.logger.error("Program will close")
            raise SystemExit(HTTPerr)

        except requests.exceptions.SSLError as SSLerr:
            self.logger.error("SLL Error : " + SSLerr)
            self.logger.error("Program will close")
            raise SystemExit(SSLerr)

        else:
            self.logger.debug("Succesfully executed API Call - JSON retrieved")
            self._json = response.json()
            return self._json




    def get_engines_list(self):
        # if we get a list of engines from the API query
        if self._get_engines_json():
            list_jsons = self._get_engines_json()
            # We keep only "connected" engines
            engines_list = [item["address"] for item in list_jsons if item["status"] == "CONNECTED"]
            # if there are engines connected
            if engines_list:
                self.logger.info("Connected Engines are : " + str(engines_list))
                return engines_list
            else:
                self.logger.info("There are no engines connected - Program will close")
                sys.exit(1)
                 
        else:
            self.logger.info("No engines returned from the API call - Program wil close")
            sys.exit(1) 



    

