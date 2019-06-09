import os
import nltk
from lib.flexiterm import get_acronyms, get_phrases, get_ids_of_phrases
from nltk.tokenize import sent_tokenize
from acronym import acronym
from sklearn.feature_extraction.text import TfidfVectorizer
from lib.tfpdf import tokenize, cosine_similarity
from lib.compare_db import create_compare_table, insert_compare


CONTEXT_SIZE = 10


def file_get_contents(filename):
    with open(filename) as f:
        return f.read()


def is_abbrev(abbrev, text):
    abbrev=abbrev.lower()
    text=text.lower()
    words=text.split()
    if not abbrev:
        return True
    if abbrev and not text:
        return False
    if abbrev[0]!=text[0]:
        return False
    else:
        return (is_abbrev(abbrev[1:], ' '.join(words[1:])) or
                any(is_abbrev(abbrev[1:], text[i+1:])
                    for i in range(len(words[0]))))


def full_acronym_context(acronym, acronym_context):
    for _acronym in acronym_context:
        if acronym == _acronym['acronym']:
            return " ".join(_acronym['context'])


def full_definition_context(definition, definitions_context):
    for _definition in definitions_context:
        if definition == _definition['definition']:
            return " ".join(_definition['context'])


def calculate_similarity(acronym_context, definitions_context):
    sklearn_tfidf = TfidfVectorizer(norm='l2', min_df=0, use_idf=True, smooth_idf=False, sublinear_tf=True,
                                    tokenizer=tokenize)
    sklearn_representation = sklearn_tfidf.fit_transform([acronym_context, definitions_context]).toarray()
    cs = cosine_similarity(sklearn_representation[0], sklearn_representation[1])
    return cs

abstracts = {}

acronyms = []
_acronyms = get_acronyms()
for a in _acronyms:
    acronyms.append(a['acronym'])


phrases = []
_phrases = get_phrases()
for a in _phrases:
    phrases.append(a['phrase'])


documents = []
for file in os.listdir("./text"):
    content = file_get_contents("./text/" + file)
    sentences = []
    try:
        sentences = sent_tokenize(content)
    except TypeError:
        sentences = []
    documents.append({'document_id': file, 'sentences': sentences})


definitions = []
for acronym in acronyms:
    x = {'acronym': acronym, 'definitions': []}
    for phrase in phrases:
        if is_abbrev(acronym, phrase):
            x['definitions'].append(phrase)
    definitions.append(x)


acronym_context = []
for acronym in acronyms:
    x = {'acronym': acronym, 'context': []}
    for document in documents:
        document_id = document['document_id']
        sentences = document['sentences']
        abstracts[document_id] = sentences
        for sentence in sentences:
            if acronym in sentence:
                x['context'].append(sentence)
    acronym_context.append(x)


definitions_context = []
for _definition in definitions:
    for definition in _definition['definitions']:
        x = {'definition': definition, 'context': []}
        document_ids = get_ids_of_phrases(definition)
        for document_id in document_ids:
            sentences = abstracts[document_id]
            found = False
            for sentence in sentences:
                if definition in sentence:
                    found = True
                    x['context'].append(sentence)
            if not found:
                x['context'].append(" ".join(sentences))
        definitions_context.append(x)


similarity = []
for _definition in definitions:
    acronym = _definition['acronym']
    _acronym_context = full_acronym_context(acronym, acronym_context)
    for definition in _definition['definitions']:
        if definition == acronym:
            continue
        _definitions_context = full_definition_context(definition, definitions_context)
        _similarity = calculate_similarity(_acronym_context, _definitions_context)
        similarity.append({
            'acronym': acronym,
            'acronym_context': _acronym_context,
            'definition': definition,
            'definition_context': _definitions_context,
            'similarity': _similarity
        })


create_compare_table()
for row in similarity:
    insert_compare(row['acronym'], row['acronym_context'], row['definition'], row['definition_context'], row['similarity'])
