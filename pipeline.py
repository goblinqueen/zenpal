"""
Two-stage tag prediction pipeline.

  Stage 1 — ML:           Predictor.predict(df, zen)
  Stage 2 — Manual rules: apply_rules(df, zen)
  Combined:               Predictor.tag(df, zen)
"""
import json
from typing import Callable, NamedTuple

import pandas as pd
from catboost import CatBoostClassifier

MODEL_PATH    = 'model.cbm'
FEATURES_PATH = 'model_features.json'

# ---------------------------------------------------------------------------
# Account constants
# ---------------------------------------------------------------------------

OP_FAMILY_ID = '50ca9746-f13a-4b67-adbb-1fe8f7f28439'   # OP Family
CREDO_0686   = '9b7a11ad-d9b4-42fb-af07-50f2d12953b7'   # Credo Bank *0686

# ---------------------------------------------------------------------------
# Display-filter constants (used by callers to hide rows, not by the pipeline)
# ---------------------------------------------------------------------------

MANUAL_TAGS      = {'корректировка'}
FAMILY_ONLY_TAGS = {'B Карманные П', 'B Здоровье П'}
NOT_FAMILY_TAGS  = {'B Карманные Х'}


# ---------------------------------------------------------------------------
# Stage 1 — ML predictor
# ---------------------------------------------------------------------------

class Predictor:
    def __init__(self, model_path=MODEL_PATH, features_path=FEATURES_PATH):
        self._model = CatBoostClassifier()
        self._model.load_model(model_path)
        with open(features_path) as f:
            fc = json.load(f)
        self._all_features  = fc['all_features']
        self._cat_features  = fc['cat_features']
        self._text_features = fc['text_features']
        self._op_family_id  = fc['op_family_id']

    def _add_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df['day_of_week'] = df['date'].dt.dayofweek
        df['month']       = df['date'].dt.month
        df['day']         = df['date'].dt.day
        df['is_family']   = (
            (df['incomeAccount'] == self._op_family_id) |
            (df['outcomeAccount'] == self._op_family_id)
        ).astype(int)
        return df

    def predict(self, df: pd.DataFrame, zen) -> pd.DataFrame:
        """Add 'predicted_tag' (tag id or None) and 'confidence' columns."""
        if df.empty:
            df = df.copy()
            df['predicted_tag'] = pd.Series(dtype=object)
            df['confidence']    = pd.Series(dtype=float)
            return df

        df = self._add_features(df)
        X  = df[self._all_features].copy()
        X[self._cat_features + self._text_features] = (
            X[self._cat_features + self._text_features].astype(str).fillna('NA')
        )

        preds      = self._model.predict(X).ravel()
        confidence = self._model.predict_proba(X).max(axis=1)

        df['predicted_tag'] = list(preds)
        df['confidence']    = confidence
        return df

    def tag(self, df: pd.DataFrame, zen) -> pd.DataFrame:
        """Run both stages. Adds 'predicted_tag', 'confidence', 'final_tag'."""
        df = self.predict(df, zen)
        df = apply_post_rules(df, zen)
        return df


# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------

class Rule(NamedTuple):
    name:  str
    match: Callable[[dict, object], bool]
    tag:   str | None   # tag title to force (post-rules only); None = suppress / drop


def _is_outcome(txn):
    return txn.get('outcome', 0) > 0 and txn.get('income', 0) == 0


def _is_income(txn):
    return txn.get('income', 0) > 0 and txn.get('outcome', 0) == 0


def _account(txn):
    return txn.get('outcomeAccount') or txn.get('incomeAccount')


def _tag_name(tag_id, zen) -> str | None:
    if not tag_id:
        return None
    return next((t['title'] for t in zen.tag if t['id'] == tag_id), None)


def _tag_id(tag_title, zen) -> str | None:
    if not tag_title:
        return None
    return next((t['id'] for t in zen.tag if t['title'] == tag_title), None)


# Pre-rules: applied to training data — matching rows are dropped before training.
# match() receives the training row dict which has a 'tag_first' key (the label).
PRE_RULES: list[Rule] = [
    # Tags that may only appear on OP Family — drop training rows where label is on wrong account
    Rule('family-only-not-family',
         match=lambda txn, zen: (
             _tag_name(txn.get('tag_first'), zen) in FAMILY_ONLY_TAGS
             and _account(txn) != OP_FAMILY_ID
         ),
         tag=None),

    # Tags that must not appear on OP Family — drop training rows where label is on family account
    Rule('not-family-on-family',
         match=lambda txn, zen: (
             _tag_name(txn.get('tag_first'), zen) in NOT_FAMILY_TAGS
             and _account(txn) == OP_FAMILY_ID
         ),
         tag=None),
]

# Post-rules: applied after ML prediction — override or suppress the predicted tag.
# match() receives the tagged row dict which has a 'predicted_tag' key.
POST_RULES: list[Rule] = [
    # All spending on the Credo card account goes to Onetime/others regardless of prediction
    Rule('credo-0686-outcome',
         match=lambda txn, _zen: _account(txn) == CREDO_0686 and _is_outcome(txn),
         tag='Onetime/others'),

    # Never surface MANUAL_TAGS as a prediction — suppress
    Rule('manual-tag-predicted',
         match=lambda txn, zen: _tag_name(txn.get('predicted_tag'), zen) in MANUAL_TAGS,
         tag=None),
]


def apply_pre_rules(df: pd.DataFrame, zen) -> pd.DataFrame:
    """Drop training rows that violate a PRE_RULE. df must have a 'tag_first' column."""
    records = df.to_dict('records')
    keep = [not any(r.match(txn, zen) for r in PRE_RULES) for txn in records]
    dropped = len(keep) - sum(keep)
    if dropped:
        print(f'Dropped {dropped} rule-violating row(s), {sum(keep)} remain')
    return df[keep].copy()


RULE_CONFIDENCE = 1.46  # sentinel value displayed when a post-rule overrides the model

def apply_post_rules(df: pd.DataFrame, zen) -> pd.DataFrame:
    """Apply POST_RULES over 'predicted_tag'. Adds 'final_tag' and 'final_confidence' columns."""
    df = df.copy()
    final_tags, final_confs = [], []
    for txn in df.to_dict('records'):
        _SENTINEL = object()
        forced = _SENTINEL
        for rule in POST_RULES:
            if rule.match(txn, zen):
                forced = _tag_id(rule.tag, zen) if rule.tag else None
                break
        if forced is _SENTINEL:
            final_tags.append(txn.get('predicted_tag'))
            final_confs.append(txn.get('confidence'))
        else:
            final_tags.append(forced)
            final_confs.append(RULE_CONFIDENCE)
    df['final_tag']        = final_tags
    df['final_confidence'] = final_confs
    return df
