# python-tagging-script

## Overview

The multi-engine-tagger is a Python 3 based utility that may be used to automate the process of tagging a Nexthink object with a given Category’s 
Keyword based on the contents of a user-supplied .csv file.

The utility is being used in both manual mode on an ad-hoc basis, as well as scheduled as a Linux cron-job.

The utility looks for properly formatted .csv files in the input tags folder, reads them, and applies the Keywords to the appropriate Objects on each Engine in the environment.  Note that the Category is set to Nil on all matching objects in the Engine before setting the Keywords.  This allows the file to be updated and re-applied as needed without worrying about having to manually reset the Category on Objects that no longer need to have a Keyword set.  The Engine list is requested dynamically via a request to the Portal.  Once complete the input file is renamed so that the utility can be run on a scheduled basis and not re-apply the file more than once.
