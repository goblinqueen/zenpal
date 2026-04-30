"""
Business rules that override or suppress ML tag predictions.
`filter_prediction(tag_id, txn, zen)` returns the tag_id to use,
or None if the prediction should be suppressed.
"""

OP_FAMILY_ID = '50ca9746-f13a-4b67-adbb-1fe8f7f28439'

# Tags that may only appear on OP Family transactions
FAMILY_ONLY_TAGS = {'B Карманные П', 'B Здоровье П'}

# Tags that must not appear on OP Family transactions
NOT_FAMILY_TAGS = {'B Карманные Х'}

# Subscriptions are managed separately — predictions for this tag are suppressed
MANUAL_TAGS = {'корректировка'}


def _tag_id_map(zen):
    return {t['title']: t['id'] for t in zen.tag}


def _tag_name_map(zen):
    return {t['id']: t['title'] for t in zen.tag}


def filter_prediction(tag_id: str, txn: dict, zen) -> str | None:
    """Return tag_id if the prediction is allowed, None to suppress it."""
    name_map = _tag_name_map(zen)
    tag_name = name_map.get(tag_id)

    if tag_name in MANUAL_TAGS:
        return None

    is_family = (txn.get('incomeAccount') == OP_FAMILY_ID
                 or txn.get('outcomeAccount') == OP_FAMILY_ID)

    if tag_name in FAMILY_ONLY_TAGS and not is_family:
        return None

    if tag_name in NOT_FAMILY_TAGS and is_family:
        return None

    return tag_id
