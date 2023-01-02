import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os,time,sys,json
import psycopg2
import xgboost as xgb
from sklearn.model_selection import KFold
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score,precision_score, accuracy_score, recall_score

def train_xgboost_model(initialData, y, nonNPICols, param, num_round=50, early_stopping_rounds=6, splits=5):
    # generate folds and data sets
    kf = KFold(n_splits=splits, shuffle=True)
    X = np.nan_to_num(initialData[nonNPICols].values)

    # performing validation over the folds
    for train_index, test_index in kf.split(initialData.npi.value_counts()):
    
        train_npi = initialData.npi.value_counts().index[train_index]
        test_npi = initialData.npi.value_counts().index[test_index]
    
        train = initialData.npi.isin(train_npi.tolist())
        test = initialData.npi.isin(test_npi.tolist())
    
        # generating test and training sets
        X_train, X_test = X[train], X[test]
        y_train, y_test = y[train].reshape(-1,), y[test].reshape(-1,)
    
        # Training and testing the XGBoost model
        dTrain = xgb.DMatrix(X_train, label=y_train)
        dTest = xgb.DMatrix(X_test, label=y_test)
        evallist = [(dTrain, 'train'), (dTest, 'test'),]
        results = {}
        bst = xgb.train(param, dTrain, num_round, evallist, verbose_eval=False, 
                        evals_result=results, early_stopping_rounds=early_stopping_rounds)
        print('AUC:',results['test']['auc'][-1],'Accuracy:',accuracy_score(y_test, (bst.predict(dTest)>0.5).astype(int)))
    
    return pd.DataFrame({'address_binary':y_test,'model_score':bst.predict(dTest)})

def test_xgb_performance(initialData, y, nonNPICols, param, num_round=50, k=4, early_stopping_rounds=6):
    # generate folds and data sets
    kf = KFold(n_splits=k, shuffle=True)
    X = np.nan_to_num(initialData[nonNPICols].values)

    # variables for recording performance
    auc_xgb, precision_xgb, accuracy_xgb = [], [], []


    # performing validation over the folds
    for train_index, test_index in kf.split(initialData.npi.value_counts()):
    
        train_npi = initialData.npi.value_counts().index[train_index]
        test_npi = initialData.npi.value_counts().index[test_index]
    
        train = initialData.npi.isin(train_npi.tolist())
        test = initialData.npi.isin(test_npi.tolist())
    
        # generating test and training sets
        X_train, X_test = X[train], X[test]
        y_train, y_test = y[train].reshape(-1,), y[test].reshape(-1,)
    
        # Training and testing the XGBoost model
        dTrain = xgb.DMatrix(X_train, label=y_train)
        dTest = xgb.DMatrix(X_test, label=y_test)
        evallist = [(dTrain, 'train'), (dTest, 'test'),]
        results = {}
        bst = xgb.train(param, dTrain, num_round, evallist, verbose_eval=False, 
                        evals_result=results, early_stopping_rounds=early_stopping_rounds)
        auc_xgb.append(results['test']['auc'][-1])
        precision_xgb.append(precision_score(y_test, (bst.predict(dTest)>0.5).astype(int)))
        accuracy_xgb.append(accuracy_score(y_test, (bst.predict(dTest)>0.5).astype(int)))
    
    print("Test Set Size:",y_test.shape[0])
    outputCols = ['Model','Mean AUC','Std AUC','Mean Accuracy','Std Accuracy','Mean Precision','Std Precision']

    # Printing out metrics around CV
    return pd.DataFrame({'Model':['XGBoost'],
              'Mean AUC':[np.mean(auc_xgb)],
              'Std AUC':[np.std(auc_xgb)],
              'Mean Accuracy':[np.mean(accuracy_xgb)],
              'Std Accuracy':[np.std(accuracy_xgb)],
              'Mean Precision':[np.mean(precision_xgb)],
              'Std Precision':[np.std(precision_xgb)]})[outputCols]


# Method for performing model NPI based cross validation
def test_classifier_performance(initialData, y, nonNPICols, param, num_round=50, k=4, early_stopping_rounds=6):

    # generate folds and data sets
    kf = KFold(n_splits=k, shuffle=True)
    X = np.nan_to_num(initialData[nonNPICols].values)

    # variables for recording performance
    auc_l1, precision_l1, accuracy_l1 = [], [], []
    auc_xgb, precision_xgb, accuracy_xgb = [], [], []


    # performing validation over the folds
    for train_index, test_index in kf.split(initialData.npi.value_counts()):
    
        train_npi = initialData.npi.value_counts().index[train_index]
        test_npi = initialData.npi.value_counts().index[test_index]
    
        train = initialData.npi.isin(train_npi.tolist())
        test = initialData.npi.isin(test_npi.tolist())
    
        # generating test and training sets
        X_train, X_test = X[train], X[test]
        y_train, y_test = y[train].reshape(-1,), y[test].reshape(-1,)
    
        # Training and testing the L1 Logistic Regression model
        LR_L1 = LogisticRegression(penalty='l1', solver='liblinear')
        LR_L1.fit(X_train, y_train)
        auc_l1.append(roc_auc_score(y_test, LR_L1.predict(X_test)))
        precision_l1.append(precision_score(y_test, LR_L1.predict(X_test)))
        accuracy_l1.append(accuracy_score(y_test, LR_L1.predict(X_test)))
    
        # Training and testing the XGBoost model
        dTrain = xgb.DMatrix(X_train, label=y_train)
        dTest = xgb.DMatrix(X_test, label=y_test)
        evallist = [(dTrain, 'train'), (dTest, 'test'),]
        results = {}
        bst = xgb.train(param, dTrain, num_round, evallist, verbose_eval=False, 
                        evals_result=results, early_stopping_rounds=early_stopping_rounds)
        auc_xgb.append(results['test']['auc'][-1])
        precision_xgb.append(precision_score(y_test, (bst.predict(dTest)>0.5).astype(int)))
        accuracy_xgb.append(accuracy_score(y_test, (bst.predict(dTest)>0.5).astype(int)))
    
    print("Test Set Size:",y_test.shape[0])
    outputCols = ['Model','Mean AUC','Std AUC','Mean Accuracy','Std Accuracy','Mean Precision','Std Precision']

    # Printing out metrics around CV
    return pd.DataFrame({'Model':['LR-L1', 'XGBoost'],
              'Mean AUC':[np.mean(auc_l1), np.mean(auc_xgb)],
              'Std AUC':[np.std(auc_l1), np.std(auc_xgb)],
              'Mean Accuracy':[np.mean(accuracy_l1), np.mean(accuracy_xgb)],
              'Std Accuracy':[np.std(accuracy_l1), np.std(accuracy_xgb)],
              'Mean Precision':[np.mean(precision_l1), np.mean(precision_xgb)],
              'Std Precision':[np.std(precision_l1), np.std(precision_xgb)]})[outputCols]

def test_ensemble_model(initialData, addr_idx, link_idx, y, y_phone, y_addr, y_link, 
                        nonNPICols, param, num_round=50, k=4, early_stopping_rounds=6):
    # generate folds and data sets
    kf = KFold(n_splits=k, shuffle=True)
    auc_xgb, precision_xgb, accuracy_xgb = [], [], []
    X = np.nan_to_num(initialData[nonNPICols].values)
    X_addr = np.nan_to_num(initialData[addr_idx.reset_index(drop=True)][nonNPICols].values)
    X_link = np.nan_to_num(initialData.loc[link_idx][nonNPICols].values)
    
    
    for train_index, test_index in kf.split(initialData.npi.value_counts()):
    
        train_npi = initialData.npi.value_counts().index[train_index]
        test_npi = initialData.npi.value_counts().index[test_index]
    
        train = initialData.npi.isin(train_npi.tolist())
        test = initialData.npi.isin(test_npi.tolist())
        train_addr = initialData[addr_idx.reset_index(drop=True)].npi.isin(train_npi.tolist())
        test_addr = initialData[addr_idx.reset_index(drop=True)].npi.isin(test_npi.tolist())
        train_link = initialData.loc[link_idx].npi.isin(train_npi.tolist())
        test_link = initialData.loc[link_idx].npi.isin(test_npi.tolist())
    
        # generating test and training sets
        X_train, X_test = X[train], X[test]
        X_train_addr, X_test_addr = X_addr[train_addr], X_addr[test_addr]
        X_train_link, X_test_link = X_link[train_link], X_link[test_link]
        y_train, y_test = y[train].reshape(-1,), y[test].reshape(-1,)
        y_phone_train, y_phone_test = y_phone[train].reshape(-1,), y_phone[test].reshape(-1,)
        y_addr_train, y_addr_test = y_addr[train_addr].reshape(-1,), y_addr[test_addr].reshape(-1,)
        y_link_train, y_link_test = y_link[train_link].reshape(-1,), y_link[test_link].reshape(-1,)
        ensemble_scores_train, ensemble_scores_test = pd.DataFrame(),pd.DataFrame()
        
        # training phone model
        dTrainPhone = xgb.DMatrix(X_train, label=y_phone_train)
        dTestPhone = xgb.DMatrix(X_test, label=y_phone_test)
        evallist_phone = [(dTrainPhone, 'train'), (dTestPhone, 'test'),]
        results_phone = {}
        bstPhone = xgb.train(param, dTrainPhone, num_round, evallist_phone, verbose_eval=False, 
                        evals_result=results_phone, early_stopping_rounds=early_stopping_rounds)
        ensemble_scores_train['phone_score'] = bstPhone.predict(dTrainPhone)
        ensemble_scores_test['phone_score'] = bstPhone.predict(dTestPhone)
        phone_predict_label = (bstPhone.predict(dTestPhone)>0.5).astype(int)
        auc_xgb.append({'Model':'XGBPhoneNPI','AUC':results_phone['test']['auc'][-1],
                        'Precision':precision_score(y_phone_test,phone_predict_label),
                        'Accuracy':accuracy_score(y_phone_test,phone_predict_label)})
                        
        
        # training address model
        dTrainAddr = xgb.DMatrix(X_train_addr,label=y_addr_train)
        dTestAddr = xgb.DMatrix(X_test_addr,label=y_addr_test)
        evallist_addr = [(dTrainAddr,'train'),(dTestAddr,'test')]
        results_addr = {}
        bstAddr = xgb.train(param, dTrainAddr, num_round, evallist_addr, verbose_eval=False, 
                        evals_result=results_addr, early_stopping_rounds=early_stopping_rounds)
        ensemble_scores_train['addr_score'] = bstAddr.predict(dTrainPhone)
        ensemble_scores_test['addr_score'] = bstAddr.predict(dTestPhone)
        addr_predict_label = (bstAddr.predict(dTestAddr)>0.5).astype(int)
        auc_xgb.append({'Model':'XGBAddressNPI','AUC':results_addr['test']['auc'][-1],
                        'Precision':precision_score(y_addr_test,addr_predict_label),
                        'Accuracy':accuracy_score(y_addr_test,addr_predict_label)})
        
        
        # training linking model
        dTrainLink = xgb.DMatrix(X_train_link, label=y_link_train)
        dTestLink = xgb.DMatrix(X_test_link, label = y_link_test)
        evallist_link = [(dTrainLink,'train'),(dTestLink,'test')]
        results_link = {}
        bstLink = xgb.train(param, dTrainLink, num_round, evallist_link, verbose_eval=False, 
                        evals_result=results_link, early_stopping_rounds=early_stopping_rounds)
        ensemble_scores_train['link_score'] = bstLink.predict(dTrainPhone)
        ensemble_scores_test['link_score'] = bstLink.predict(dTestPhone)
        link_predict_label = (bstLink.predict(dTestLink)>0.5).astype(int)
        auc_xgb.append({'Model':'XGBLink','AUC':results_link['test']['auc'][-1],
                        'Precision':precision_score(y_link_test,link_predict_label),
                        'Accuracy':accuracy_score(y_link_test,link_predict_label)})
        
        #trainin ensemble model
        scores_cols = ['phone_score','addr_score','link_score']
        dEnsembleTrain = xgb.DMatrix(np.nan_to_num(ensemble_scores_train[scores_cols].values),label=y_train)
        dEnsembleTest = xgb.DMatrix(np.nan_to_num(ensemble_scores_test[scores_cols].values),label=y_test)
        evallist_ensemble = [(dEnsembleTrain,'train'),(dEnsembleTest,'test')]
        results_ensemble = {}
        bstEnsemble = xgb.train(param, dEnsembleTrain, num_round, evallist_ensemble, verbose_eval=False, 
                        evals_result=results_ensemble, early_stopping_rounds=early_stopping_rounds)
        ensemble_predict_label = (bstEnsemble.predict(dEnsembleTest)>0.5).astype(int)
        auc_xgb.append({'Model':'XGBEnsemble','AUC':results_ensemble['test']['auc'][-1],
                        'Precision':precision_score(y_test,ensemble_predict_label),
                        'Accuracy':accuracy_score(y_test,ensemble_predict_label)})
        
    return pd.DataFrame(auc_xgb).groupby('Model').describe()
    