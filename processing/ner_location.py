import spacy

_nlp = None


def _get_nlp():

    global _nlp

    if _nlp is None:
        _nlp = spacy.load("en_core_web_sm")

    return _nlp


def extract_locations(text):

    if not text:
        return []

    doc = _get_nlp()(text)

    locs = []

    for ent in doc.ents:
        if ent.label_ in {"GPE", "LOC", "FAC"}:
            locs.append(ent.text)

    return list(dict.fromkeys(locs))


def extract_locations_batch(texts, batch_size=128):

    if not texts:
        return []

    location_groups = []

    for doc in _get_nlp().pipe(texts, batch_size=batch_size):
        locs = []

        for ent in doc.ents:
            if ent.label_ in {"GPE", "LOC", "FAC"}:
                locs.append(ent.text)

        location_groups.append(list(dict.fromkeys(locs)))

    return location_groups