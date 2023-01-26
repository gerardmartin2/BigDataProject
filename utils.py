import os
import sys
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from datetime import date,timedelta
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
from tempfile import NamedTemporaryFile
from glob import glob
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
import re
from tempfile import NamedTemporaryFile
import shutil
from datetime import datetime

def aircraft_files(aircraft, date):
    """
    Returns a list with the file names of data for the aircraft and day given
    """
    aircraft_files = []
    target = date + "-...-...-....-" + aircraft + ".csv"
    #for every file check if it's name matches with the aircraft and the day given
    for file in list(os.walk("./resources/trainingData/"))[0][2]:
        match = re.match(target, file)
        if match:
            aircraft_files.append(file)
    return aircraft_files

def Maintenances(Operation_Interruption):
    """
    Returns maintenances per aircraft and day
    """

    return (Operation_Interruption
            .select("aircraftregistration","starttime","kind","subsystem")
            # Filter events of kind Delay,AOG or Safety (Unscheduled)
            .filter(Operation_Interruption.kind.isin(['Delay', 'AircraftOnGround', 'Safety']))
            # Filter data for the desired sensor
            .filter(Operation_Interruption.subsystem == '3453')
            .rdd
            # Format [(time,aircraft_reg),kind]
            .map(lambda t: ((t[1].date(),t[0]),t[2]))
            .sortByKey()
    )

def KPIs(Aircraft_utilization, aircraft):
    """
    Returns FH, FC and DM for aircraft and day.
    """
    #get KPIs for the given aircraft
    KPIs = (Aircraft_utilization.select("aircraftid","timeid","flighthours","flightcycles","delayedminutes"))

    # Format [(time, aircraftid), (FH, FC, DM)]
    return (KPIs.filter(KPIs.aircraftid== aircraft).rdd
            .map(lambda t: ((t[1],t[0]),(float(t[2]),int(t[3]),int(t[4]))))
            .sortByKey()
    )


def avg_sensor(sc, csv_path):
    """
    Returns average sensor measurements for aircraft and day.
    """

    return (sc.wholeTextFiles(csv_path)
                # Pair (aircraft_id, csv_info)
                .map(lambda t: (t[0].split("/")[-1][20:26], t[1].split("\n")))
                # Retrieve the date and remove the column names
                .map(lambda t: ((datetime.strptime(t[1][1][0:10],'%Y-%m-%d').date(), t[0]), t[1][1:]))
                # Create a row for each value of the sensor
                .flatMap(lambda t: [(t[0], record) for record in t[1]])
                # Ensure that sensor measurements exist
                .filter(lambda t: t[1]!= '')
                # Store the value pairs (sensor_value,1)
                .mapValues(lambda t: (float(t.split(";")[-1]),1))
                # Compute the avg of the sensor
                .reduceByKey(lambda t1,t2: (t1[0]+t2[0], t1[1]+t2[1]))
                .mapValues(lambda t: t[0]/t[1])
                .sortByKey()
                )
def delete_folders():
    """
    Deletes the files stored in previous executions
    """
    #Files stored from data management
    if os.path.exists('./LibSVM-files/'):
        shutil.rmtree('./LibSVM-files/')
    #Model stored from analysis
    if os.path.exists('./Model/'):
        shutil.rmtree('./Model/')
    return
