#!/usr/bin/env python

from ncimonitor.UsageDataset import *
from ncimonitor.DBcommon import *

import pandas as pd
import argparse
import os
from warnings import warn
import datetime


def filter_new_only(project,dp):

    # Dusql info
    dp1 = pd.read_csv(f'du_{project}.out',names=['size','files','path'])

    for row in range(dp1.shape[0]):
        # Get only the number of file
        dp1.files.values[row] = dp1.files.values[row].split()[0]
        # Get only the user id
        dp1.path.values[row] = dp1.path.values[row].split('/')[-1]

    # Filter out the users with 0 files
    dp1 = dp1.where(dp1.files != '0').dropna()
    for row in range(dp1.shape[0]):
        # Find user in dp with the same user id
        for s in dp.index:
            if dp1.path.values[row] in s:
                dp1.path.values[row] = s

    # Create new dataframe: Name / Yes
    dp3 = pd.DataFrame(data=['yes']*dp1.shape[0],index=dp1.path)
    dp3.columns=[f'{project} New data']

    # Concatenate with dp dataframe
    return pd.concat([dp,dp3],axis=1, sort=False)

def storage_frame(db,project,storagept,year,quarter,datafield):

    if datafield == 'size':
        # Scale sizes to GB
        # scale = 1.e12       # 1 GB 1000^4
        scale = 1099511627776. # 1 GB 1024^4
        ylabel = "Storage Used (TB)"
    else:
        scale = 1
        ylabel = "Inodes"

    dp = db.getstorage(year, quarter, storagept=storagept, datafield=datafield)

    if dp is not None:
        dp = dp / scale
    else:
        return

    if dp is None:
        warn("No data to display for this selection")
        return

    # Tranpose the dataframe to have columns: User, Size. And take only the last column
    dp = dp.transpose()
    dp = dp.iloc[:,-1]
    # Set as a dataframe
    dp = pd.DataFrame(dp)
    # Change column name from date to 'project size'
    dp.columns=[f'{project} Size (in TB)']
    return dp

def main():

    parser = argparse.ArgumentParser(description="Show NCI account usage information with more context")

    parser.add_argument("-p","--period", help="Time period in year.quarter (e.g. 2015.q4)")
    parser.add_argument("-P","--project", help="Specify project id(s)", default=[os.environ["PROJECT"]], nargs='*')

    args = parser.parse_args()

    if args.period is not None:
        year, quarter = args.period.split(".")
    else:
        raise ValueError('you must give a period') 

    datafield = 'size'
        
    dbfileprefix = '/short/public/aph502/.data/'

    dd=[]
    for project in args.project:

        dbfile = 'sqlite:///'+os.path.join(dbfileprefix,"usage_{}_{}.db".format(project,year))
        try:
            db = ProjectDataset(project,dbfile)
        except:
            print("ERROR! You are not a member of this group: ",project)
            continue
        else:
            dp=storage_frame(db,project,'short',year,quarter,datafield)
            # Get info on who has new data
            dd.append(filter_new_only(project,dp))

    # Concatenate the Frames together
    res = pd.concat(dd,axis=1,sort='False')
    res.to_csv('test.csv')


if __name__ == "__main__":
    main()
