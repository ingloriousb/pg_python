common_words = {
    'privatelimited': 'Private Limited',
    'privateltd': 'Private Limited',
    'pvtlimited': 'Private Limited',
    "pvtltd": "Private Limited",
    'pltd': 'Private Limited',
    'p/ltd': 'Private Limited',
    "ltd": "Limited",
    "co$": "Company",
    "corp": "Corporation",
    "(i)": "(India)",
    "[i]": "(India)",
    "pvt": "Private",
    "prvt": "Private",
    "pte": "Private",
    "mfg": "Manufacturing",
    "mfgco": "Manufacturing Co",
    "(p)": "Private",
    "[p]": "Private",
    "oil": "Oil",
    "das": "Das",
    'copvt': 'Co Private',
    '&amp;': '&',
    "and": "&",
    'pl$': 'Private Limited',
    'limite$': 'Limited',
    'pv$': 'Private',
    '(prop)': 'Proprietor',
    'prop': 'Proprietor',
    'constt': 'Construction',
    'const': 'Construction',
    'cons': 'Construction',
    'corpn': 'Corporation',
    'engrs': 'Engineers',
    'engg': 'Engineering',
    'eng': 'Engineering',
    'govt': 'Government',
    'inc': 'Incorporated',
    'llc': 'Limited Liability Company',
    'llp': 'Limited Liability Partnership',
    'mktg': 'Marketing',

}

after_words_map = {
    ' P Limited$': ' Private Limited',
    ' Private L$': ' Private Limited',
    ' P L$': ' Private Limited',
    ' PLtd$': ' Private Limited',
    ' (Private) ': ' Private ',
    ' (Private)$': ' Private',
    ' Cipvt': ' Co Private',
    ' I Private': ' (India) Private',
    ' I Limited': ' (India) Limited',
    ' L$': ' Limited',
    ' P Limited-': ' Private Limited-',
    ' Private LT$': ' Private Limited',
    ' P$': ' Private',
    ' Government Cont$': ' Government Contractor',
    '(Private Limited)': 'Private Limited',
}

NONE_LIST = ["None", None, "none", "null", "<null>", "", [], {}, set(), tuple(), '[]', '{}', [None], ['None'], 'nan', 'Nan', "NA", "Na"]


def process_raw_word(word, last):
    if word is None or len(word) == 0:
        return ""
    word = word.strip()
    word = word.replace(".", "").replace(",", "")
    w = word.lower()
    if w in common_words.keys():
        return common_words[w]
    for symbol in ["(", "[", ")", "]", '-', '/', '\\', ';', '&', '"', "'"]:
        for common_word in common_words:
            if common_word + symbol == w and symbol in ['(', '[', ')', ']', '&', '-']:
                return common_words[common_word] + symbol
            elif symbol + common_word == w and symbol in ['(', '[', ')', ']', '&', '-']:
                return symbol + common_words[common_word]
            elif common_word + symbol == w or symbol + common_word == w:
                return common_words[common_word]
            elif common_word + symbol in w:
                processed_word = w.replace(common_word + symbol, common_words[common_word] + symbol)
            elif symbol + common_word in w:
                processed_word = w.replace(symbol + common_word, symbol + common_words[common_word])
            else:
                processed_word = None

            if processed_word is not None:
                return symbol.join([i.capitalize() if len(i) > 0 and i[0].isalpha() and i[0].islower() else i
                                    for i in processed_word.split(symbol)])
    if last and w + "$" in common_words.keys():
        return common_words[w + "$"]
    return word


def find_proper_name(name):
    word_tuples = []
    name_words = []
    good_case = 0
    bad_case = 0
    for f_special_word in ['(', '[']:
        name = name.replace(f_special_word, " " + f_special_word)
        name = name.replace(f_special_word + " ", f_special_word)
    for b_special_word in [')', ']']:
        name = name.replace(b_special_word, b_special_word + " ")
        name = name.replace(" " + b_special_word, b_special_word)
    split_names = name.split()
    count = 1
    for raw_word in split_names:
        last = False
        if count == len(split_names):
            last = True
        count += 1
        word = process_raw_word(raw_word, last)
        if not word.isupper():
            good_case += 1
            word_tuples.append(("good", word))
            continue
        if len(word) <= 2:
            word_tuples.append(("good", word.upper()))
            continue
        if len(word) <= 3:
            word_tuples.append(("good", word))
            continue
        bad_case += 1
        word_tuples.append(("bad", word))
    for t, w in word_tuples:
        if t in ("bad", "neutral"):
            name_words.append(w.capitalize())
        else:
            if w.islower():
                name_words.append(w.capitalize())
            else:
                name_words.append(w)
    norm_name = " ".join(name_words)
    for strip_word in ["'", '"', '-']:
        norm_name = norm_name.strip(strip_word)
    for k, v in after_words_map.items():
        if k.endswith('$') and norm_name.endswith(k[:-1]):
            norm_name = norm_name[:-len(k) + 1] + v
        else:
            norm_name = norm_name.replace(k, v)
    norm_name = " ".join(norm_name.split())
    return norm_name.strip()
