from __future__ import annotations


ISSUE_KEYWORDS = {
    "health": {
        "kidney", "dialysis", "hospital", "dengue", "malaria", "doctor", "clinic", "disease", "fever",
        "ambulance", "medicine", "medical", "oxygen", "outbreak", "health centre", "primary health centre",
        "aspatal", "chikitsa", "bimari", "bukhar", "dawai", "ilaaj", "swasthya", "aarogya",
        "अस्पताल", "चिकित्सा", "बीमारी", "बुखार", "दवाई", "इलाज", "स्वास्थ्य", "डायलिसिस", "किडनी",
        "hosptial", "medical camp", "screening camp",
    },
    "water": {
        "water", "contamination", "sewage", "pipeline", "drinking water", "tanker", "filtration", "fluoride",
        "arsenic", "waterlogging", "flood", "drain", "sanitation", "water supply", "drainage",
        "pani", "jal sankat", "jalapurti", "nal jal", "ganda pani", "baarish ka pani", "naala", "nala",
        "पानी", "जल संकट", "जलापूर्ति", "नल जल", "गंदा पानी", "सीवेज", "नाला", "बाढ़", "जलभराव",
        "neer", "thanneer", "manchi neellu", "neeru",
    },
    "road_safety": {
        "accident", "road", "highway", "pothole", "traffic", "collision", "crash", "speeding", "helmet",
        "street light", "black spot", "junction", "road safety", "road accident",
        "sadak", "gadda", "jam", "sadak hadsa", "tez raftaar", "signal kharab", "pulia",
        "सड़क", "गड्ढा", "जाम", "सड़क हादसा", "तेज रफ्तार", "स्ट्रीट लाइट", "हाईवे", "चौराहा",
        "raste", "rasta", "maarg", "veedhi",
    },
    "crime": {
        "murder", "assault", "robbery", "rape", "kidnap", "gang", "crime", "violence", "clash",
        "firing", "weapon", "law and order", "police", "riot", "loot", "attack",
        "hatya", "hamla", "loot", "balatkar", "apradh", "hinsa", "golibari", "tanav", "danga",
        "हत्या", "हमला", "लूट", "बलात्कार", "अपराध", "हिंसा", "गोलीबारी", "तनाव", "दंगा", "पुलिस",
        "kolai", "dacoity", "ghatana",
    },
    "infrastructure": {
        "bridge", "power", "electricity", "transformer", "internet", "drainage", "school building",
        "hospital building", "repair", "collapse", "construction", "sewer", "road repair", "power cut",
        "bijli", "pul", "marammat", "nirman", "vikas karya", "transformer kharab", "building girna",
        "बिजली", "पुल", "मरम्मत", "निर्माण", "विकास कार्य", "ट्रांसफार्मर", "इंटरनेट", "भवन", "ढांचा",
        "vidyut", "sethu", "current cut", "karant",
    },
}

SENSITIVE_EVENT_KEYWORDS = {
    "violent_crime": {
        "murder", "mob", "lynching", "firing", "violent", "attack", "rape",
        "hatya", "golibari", "mob attack", "balatkar", "हत्या", "गोलीबारी", "भीड़ हिंसा", "बलात्कार",
    },
    "communal_tension": {
        "communal", "religious tension", "sectarian", "riot", "hate",
        "sampradayik", "dharmik tanav", "samudaayik tanav", "साम्प्रदायिक", "धार्मिक तनाव", "दंगा",
    },
    "religious_protest": {
        "festival protest", "religious protest", "procession clash", "temple protest",
        "dharmik virodh", "juloos vivad", "festival clash", "धार्मिक विरोध", "जुलूस विवाद",
    },
    "mob_violence": {
        "mob", "mob violence", "lynching", "crowd attack",
        "bheed", "bheed hinsa", "भीड़", "भीड़ हिंसा", "lynch",
    },
    "police_clash": {
        "police clash", "lathi charge", "tear gas", "police firing",
        "police jhadap", "lathicharge", "पुलिस झड़प", "लाठीचार्ज", "आंसू गैस", "पुलिस फायरिंग",
    },
}

PROTEST_KEYWORDS = {
    "protest", "anger", "outrage", "demand", "blocked road", "government failure", "sit-in",
    "demonstration", "agitation", "march", "strike", "road blockade", "shutdown",
    "andolan", "dharna", "pradarshan", "narazgi", "gherao", "bandh", "raasta roko", "sadak jam",
    "विरोध", "आक्रोश", "मांग", "धरना", "प्रदर्शन", "नाराजगी", "घेराव", "बंद", "रास्ता जाम",
    "hartal", "porattam", "horata", "samaram",
}
