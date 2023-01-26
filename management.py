import utils
from utils import *

"""
MANAGEMENT PIPELINE:

Steps:
        1. Read sensor measurements from CSV files and average it per day.
        2. Enrich it with KPIs from the Data Warehouse.
        3. Label each row.
        4. Generate a matrix with the gathered data and store it.

"""


def data_management(sc,Aircraft_utilization, Operation_Interruption, aircraft, csv_path):
    """
    Returns transformed data prepared for model training for a given aircraft
    """

    # Step1: Get average per day of the sensor measurements
    avg_sensor=utils.avg_sensor(sc, csv_path)
    # Step2: Read KPIs for the aircraft given
    KPIs=utils.KPIs(Aircraft_utilization, aircraft)
    # Get maintenances
    Maintenances=utils.Maintenances(Operation_Interruption)
    # Add rows for the 7 previous days as if there was a maintenance needed
    Maintenances_mod=Maintenances.flatMap(lambda t: [((t[0][0] - timedelta(days=days),t[0][1]),t[1]) for days in range(8)])
    # Step3: Join avg_sensor with KPIs and maintenances and label the data (1 for maintenance, 0 for no maintenance)
    final_trans=avg_sensor.join(KPIs).leftOuterJoin(Maintenances_mod).mapValues(lambda t: (t[0], t[1] is not None)).map(lambda t: utils.LabeledPoint(t[1][1],[t[1][0][0],t[1][0][1][0],t[1][0][1][1],t[1][0][1][2]]))
    # Step4: Store the final output
    utils.MLUtils.saveAsLibSVMFile(final_trans, "./LibSVM-files/")
    return final_trans
