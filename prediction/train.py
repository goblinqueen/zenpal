"""
Model training, extracted from run.ipynb cell 7.
Call train_model(zen, train_start, train_end) to retrain and save model.cbm + model_features.json.
"""
import json
import pandas as pd
from catboost import CatBoostClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report

from prediction.pipeline import MANUAL_TAGS, OP_FAMILY_ID, apply_pre_rules
from rules import _tag_name_map

MODEL_PATH      = 'prediction/model.cbm'
FEATURES_PATH   = 'prediction/model_features.json'
MIN_CLASS_SAMPLES = 10
ITERATIONS = 200


class _ProgressCallback:
    def __init__(self, total, fn):
        self._total = total
        self._fn = fn

    def after_iteration(self, info):
        if self._fn:
            self._fn(info.iteration + 1, self._total)
        return True


def train_model(zen, train_start: str, train_end: str, progress_fn=None) -> str:
    """Train CatBoost on [train_start, train_end]. Returns classification report string."""
    df = pd.DataFrame(zen.transaction)
    df['date'] = pd.to_datetime(df['date'])

    df_train = df[
        (df['date'] >= pd.Timestamp(train_start))
        & (df['date'] <= pd.Timestamp(train_end))
        & (df['deleted'] == False)
        & ~((df['income'] > 0) & (df['outcome'] > 0))
    ].copy()

    df_train['tag_first'] = df_train['tag'].apply(
        lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None
    )
    df_train = df_train.dropna(subset=['tag_first'])

    tag_name_map = _tag_name_map(zen)
    df_train = df_train[~df_train['tag_first'].map(tag_name_map).isin(MANUAL_TAGS)]

    df_train = apply_pre_rules(df_train, zen)

    class_counts = df_train['tag_first'].value_counts()
    df_train = df_train[df_train['tag_first'].isin(
        class_counts[class_counts >= MIN_CLASS_SAMPLES].index
    )]

    df_train['day_of_week'] = df_train['date'].dt.dayofweek
    df_train['month']       = df_train['date'].dt.month
    df_train['day']         = df_train['date'].dt.day
    df_train['is_family']   = (
        (df_train['incomeAccount'] == OP_FAMILY_ID) |
        (df_train['outcomeAccount'] == OP_FAMILY_ID)
    ).astype(int)

    num_features   = ['day_of_week', 'month', 'day', 'income', 'outcome', 'is_family']
    cat_col_names  = ['incomeAccount', 'outcomeAccount', 'merchant']
    text_col_names = ['originalPayee']

    all_features  = [f for f in num_features + cat_col_names + text_col_names if f in df_train.columns]
    cat_features  = [f for f in cat_col_names  if f in all_features]
    text_features = [f for f in text_col_names if f in all_features]

    X = df_train[all_features].copy()
    X[cat_features + text_features] = X[cat_features + text_features].astype(str).fillna('NA')
    y = df_train['tag_first']

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, stratify=y
    )

    model = CatBoostClassifier(iterations=ITERATIONS, verbose=0,
                               cat_features=cat_features,
                               text_features=text_features,
                               train_dir='prediction/catboost_info')
    model.fit(X_train, y_train,
              callbacks=[_ProgressCallback(ITERATIONS, progress_fn)])

    tag_map = {t['id']: t['title'] for t in zen.tag}
    y_test_names = y_test.map(tag_map)
    y_pred_names = pd.Series(model.predict(X_test).ravel(), index=y_test.index).map(tag_map)
    report = classification_report(y_test_names, y_pred_names)

    model.save_model(MODEL_PATH)
    with open(FEATURES_PATH, 'w') as f:
        json.dump({
            'all_features':  all_features,
            'cat_features':  cat_features,
            'text_features': text_features,
            'op_family_id':  OP_FAMILY_ID,
        }, f)

    return report
