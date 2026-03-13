import re


def clean_text(text):

    if text is None:
        return ""

    text = re.sub(r"http\S+", "", text)
    text = re.sub(r"\s+", " ", text)

    return text.strip()