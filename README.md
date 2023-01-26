## BIG DATA PROJECT

## Description
The project consists in predicting whether an aircraft is going to need unexpected maintenance or not in the following 7 days. The information used about the aircrafts and their flights has been extracted from three different sources:
- AMOS: Data base containing information about aircarfts maintenances. The table used, OperationInterruption, reports the reason of the interruption in the aircraft operations, as well as information about sensors and the duration of the interruption.
- DW: Data warehouse whose table AircraftUtilization contains information about aircrfats' KPIs.
- CSV files: Files that contain measures from different sensors in an aircraft, measured every five minutes of a flight.

The project is divided in three secuential pipelines:
- ```management.py```: Pipeline that generates a matrix by grouping KPIs and the average of the subsystem sensor 3453. 
- ```analysis.py```: Pipeline that creates, trains, validates and stores a decision tree model.
- ```runtime.py```: Pipeline that, given an input (date,aircraft), replicates the management pipeline and uses the model created in the analysis pipeline topredict wheter the plane is going to need an unscheduled maintenance or not.

## Usage

If the requeriments detailed in the file ```assumptions.pdf``` of this repository are satisfied, after running the ```main.py``` programm, the user will be requested a specific date and aircraft ID. Once provided, the three pipelines are executed secuentially, outputing if this aircarft is going to need an unscheduled maintenance or not in the following week. Notice that all internal functions are provided in the file ```utils.py```.

## Contributing
We are open to any contribution that aims to improve the project. Please contact:
eduard.ramon.aliaga@estudiantat.upc.edu
gerard.martin.pey@estudiantat.upc.edu

## Support
If you have any doubt related with the project do not hesitate to contact the already mentioned mails. 

## Authors
Eduard Ramón Aliaga
Gerard Martin Pey
