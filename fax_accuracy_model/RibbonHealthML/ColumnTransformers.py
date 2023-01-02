from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import MultiLabelBinarizer
import numpy as np
import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer


class MultiLabelTransformer(BaseEstimator, TransformerMixin):
    """
    Wraps `MultiLabelBinarizer` in a form that can work with `ColumnTransformer`
    """
    def __init__(
            self,
            features=[]
        ):
        self.feature_name = ["mlb"]
        self.mlb_arr = []
        self.cols = []
        self.cats = []
        self.features = features

    def fit(self, X, y=None):
        for col in X.columns.tolist():
            temp_mlb = MultiLabelBinarizer(sparse_output=False)
            temp_mlb.fit(X[col])
            self.mlb_arr.append(temp_mlb)
            if self.features:
                temp_cats = [f'{col}_{x}' for x in temp_mlb.classes_ if f'{col}_{x}' in self.features]
            else:
                temp_cats = [f'{col}_{x}' for x in temp_mlb.classes_]
            self.cats.extend(temp_cats)
        return self

    def transform(self, X):
        X_ret = pd.DataFrame()
        for idx, col in enumerate(X.columns.tolist()):
            X_temp = pd.DataFrame(self.mlb_arr[idx].transform(X[col]))
            X_temp.columns = [f'{col}_{x}' for x in self.mlb_arr[idx].classes_]
            if self.features:
                temp_feat = [c for c in X_temp.columns if c in self.features]
                X_temp = X_temp[temp_feat]
            if idx == 0:
                X_ret = X_temp.copy(deep=True)
            else:
                X_ret = X_ret.merge(X_temp, left_index=True, right_index=True)
        return X_ret

    def get_feature_names(self, input_features=None):
        cats = self.cats
        return np.array(cats, dtype=object)

class FeatureSelector(BaseEstimator, TransformerMixin):
    def __init__(self, columns):
        self.columns = columns

    def fit(self, X, y=None):
        return self

    def transform(self, X, y=None):
        return X[self.columns]