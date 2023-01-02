from sklearn.metrics import roc_auc_score, accuracy_score, precision_recall_curve, confusion_matrix, classification_report, brier_score_loss
from datetime import datetime
import os
import pandas as pd
import pathlib
import matplotlib.pyplot as plt
from sklearn.calibration import calibration_curve

def print_report(clf, X_test, y_true, feature_names=[], report_path=''):
    # timestamp = datetime.now().strftime('%Y%m%d%H%M')
    # report_path = pathlib.Path(__file__).resolve().parent.parent.joinpath('ModelReports')
    # report_path = os.path.join(os.path.pardir(os.path.dirname(os.path.realpath(__file__))), 'ModelReports')
    y_pred = clf.predict(X_test)
    y_prob = clf.predict_proba(X_test)[:,1]
    feat_df = pd.DataFrame()
    feat_df['feature'] = feature_names
    feat_df['importance'] = clf['classifier'].feature_importances_
    feat_df = feat_df.sort_values(['importance'], ascending=False)

    thresh = [0, 0.4, 0.6, 0.8, 1.01]
    conf_df = X_test.copy(deep=True)
    conf_df['y_true'] = y_true
    conf_df['y_prob'] = y_prob
    conf_df['conf'] = pd.cut(conf_df['y_prob'], thresh, labels=[1, 2, 3, 4])
    agg_true = conf_df[conf_df['y_true'] == True].groupby(['conf']).agg({'y_prob': 'count'}).reset_index()
    agg_true = agg_true.rename(columns={'y_prob': 'true_count'})
    agg = conf_df.groupby(['conf']).agg({'y_prob': 'count'}).reset_index()
    agg = agg.rename(columns={'y_prob': 'count'})
    merged_agg = agg_true.merge(agg)
    merged_agg['pct'] = merged_agg['true_count'] / merged_agg['count']
    merged_agg

    calibration_loss = brier_score_loss(y_true, y_prob)

    fop, mpv = calibration_curve(y_true, conf_df['y_prob'], n_bins=10)
    plt.plot([0, 1], [0, 1], linestyle='--')
    # plot model reliability
    plt.plot(mpv, fop, marker='.')
    plt.ylabel("Fraction of positives")
    plt.xlabel("Mean predicted value")
    plt.show()

    report = f'''{clf.__str__()}
    {confusion_matrix(y_true, y_pred, labels=[True, False]).__str__()}
    {classification_report(y_true, y_pred, labels=[True, False]).__str__()}
    AUC: {roc_auc_score(y_true, y_prob)}
    Calibration Loss: {calibration_loss}
    {feat_df.head(25).__str__()}
    Rank order:
    {merged_agg.__str__()}
    '''
    print(report)
    if report_path:
        with open(report_path, 'w') as f:
            f.write(report)
            figure_report_path = report_path[:-4] + '_figure.pdf'
            plt.savefig(figure_report_path)
    print("Report Complete")
