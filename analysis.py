import management
from management import *

"""
ANALYSIS PIPELINE:

Steps:
        1. Create two datasets (training and validation).
        2. Create the validate model.
        3. Compute accuracy and recall.
        4. Store the validate model.
"""


def data_analysis(sc,data):
    """
    Trains a model for the output data of the data_manegement part and returns the accuracy and the recall.
    """

    # Step1: Split the data for training and test
    (tr, te) = data.randomSplit([0.7, 0.3])
    # Step2: Design a model with a DecisionTree with 2 classes and default parameters
    model = management.DecisionTree.trainClassifier(tr, numClasses=2, categoricalFeaturesInfo={}, impurity='gini', maxDepth=5, maxBins=32)
    # Extract test data features
    data_features=te.map(lambda t: t.features)
    # Predict for those features and get the results
    preds = model.predict(data_features)
    results = te.map(lambda t:t.label).zip(preds)
    # Step3: Compute accuracy and recall
    false_negatives=results.filter(lambda r: r[0] != r[1]).filter(lambda r: r[1] == 0).count()
    true_positives = results.filter(lambda r: (r[0] == 1) & (r[0] == r[1])).count()
    if (true_positives + false_negatives)==0:
        recall="NaN"
    else:
        recall = true_positives / float(true_positives + false_negatives)
    Acc = 1 - (results.filter(lambda r: r[0] != r[1]).count() / float(te.count()))
    # Step4: Save the model trained for the runtime procedure
    model.save(sc, "Model")
    print(f"Evaluation metrics: \nAccuraccy: {Acc} \nRecall: {recall}")
    return recall,Acc
