import utils
from utils import *

"""
RUNTIME PIPELINE:

Steps:
        1. Replicate the data management pipeline.
        2. Prepare the tuple to be inputted into the model.
        3. Classifies the record.
        4. Outputs maintenance/no maintenance.
"""


def data_runtime(sc,Aircraft_utilization, Operation_Interruption, aircraft, csv_path,date):
    """
    Gets files needed for an aircraft and a date to return a prediction for the maintenance
    """
    # Load the model stored in the analysis procedure
    model = utils.DecisionTreeModel.load(sc, "Model")
    # Step1: Replicate management pipeline
    # Get a list of the files registered for the aircraft given
    target_files = utils.aircraft_files(aircraft, date)
    target_path="./resources/trainingData/" + target_files[0]
    # Get the average sensor for the aricraft given
    avg_sensor = utils.avg_sensor(sc,target_path)
    # Read the KPIs for the aircraft given
    KPIs=utils.KPIs(Aircraft_utilization, aircraft)
    # KPI and avg_sensor for the date given
    rdd = avg_sensor.join(KPIs).collect()[0]
    # Step2: Generate the sample
    record = [rdd[1][0],rdd[1][1][0],rdd[1][1][1],rdd[1][1][2]] #[avg sensor, FH, FC, DM]
    # Step3: Classify the record
    prediction = model.predict(record)

    # Step4: Print results
    print(f"Record: Avg_sensor: {record[0]}, FH: {record[1]}, FC: {record[2]}, DM: {record[3]}")
    if (prediction == 0):
        print("Prediction: No maintenance will be required in the next 7 days")
    else:
        print("Prediction: Maintenance will be required in the next 7 days")
    return
