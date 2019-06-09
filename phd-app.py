import xml.etree.ElementTree
import shutil
import os
import re
import random
import nltk
from abbreviations import schwartz_hearst
from nltk.tokenize import sent_tokenize
from sklearn.feature_extraction.text import TfidfVectorizer
from lib.tfpdf import tokenize, cosine_similarity
from lib.db import create_tables, insert_abstract, insert_acronym, insert_filtered_abstract, insert_found_acronym, \
    insert_found_full_form, select_similarity_candidates, insert_similarity


nltk.download('punkt')

ABSTRACTS_PER_FILE = 1000
CONTEXT_SIZE = 10

db_abstracts = []
db_acronyms = []
db_acronym_index = []
db_filtered_abstracts = []
db_found_acronyms = []
db_unique_acronyms = []
db_found_full_forms = []
db_similarity = []


def parse_files():
    for file in os.listdir("./xml-files"):
        if file.endswith(".xml"):
            parse_file("./xml-files/" + file)


def parse_abstracts(el, results=None):
    if results is None:
        results = []
    for child in el:
        if child.tag == 'AbstractText':
            if child.text:
                results.append(''.join(child.itertext()))
        parse_abstracts(child, results)
    return results


def parse_file(file):
    global db_abstracts
    count_abstracts = 0
    root = xml.etree.ElementTree.parse(file).getroot()
    for article in root:
        cit = article.find("MedlineCitation")
        inserted_abstracts = []
        if cit:
            document_id = cit.find("PMID").text
            abstracts = parse_abstracts(article)
            if abstracts:
                for a in abstracts:
                    inserted_abstracts.append(a)
                    count_abstracts = count_abstracts + 1
                    if count_abstracts == ABSTRACTS_PER_FILE:
                        db_abstracts.append({'document_id': document_id, 'text': [" ".join(inserted_abstracts)]})
                        insert_abstract(document_id, " ".join(inserted_abstracts))
                        return
                db_abstracts.append({'document_id': document_id, 'text': [" ".join(inserted_abstracts)]})
                insert_abstract(document_id, " ".join(inserted_abstracts))


def create_folder(folder):
    shutil.rmtree(folder, True)
    try:
        os.mkdir(folder)
    except OSError:
        pass


def create_file(abstracts, file_name):
    f = open(file_name, "w")
    f.write("\n".join(abstracts))
    f.close()


def put_abstracts_in_files():
    create_folder('original_text')
    for abstract in db_abstracts:
        create_file(abstract['text'], "original_text/" + abstract['document_id'])


def run_schwartz_algorithm():
    global db_acronyms
    for file in os.listdir("./original_text"):
        pairs = schwartz_hearst.extract_abbreviation_definition_pairs(file_path="./original_text/" + file)
        result = {'document_id': file, 'acronyms': []}
        for key, value in pairs.items():
            result['acronyms'].append({'acronym': key, 'full_form': value})
            insert_acronym(file, key, value)
        db_acronyms.append(result)


def sort_acronyms():
    global db_acronym_index
    acronym_index = {}
    create_folder('acronyms')
    for acronym_lines in db_acronyms:
        for acronyms in acronym_lines['acronyms']:
            if not acronyms['acronym'] in acronym_index.keys():
                acronym_index[acronyms['acronym']] = []
            if acronyms['full_form'] not in acronym_index[acronyms['acronym']]:
                acronym_index[acronyms['acronym']].append(acronyms['full_form'])
    for acronym in acronym_index.keys():
        _acronym = acronym.replace("/", " ")
        db_acronym_index.append({'acronym': acronym, 'full_forms': acronym_index[acronym]})
        create_file(acronym_index[acronym], "acronyms/" + _acronym)


def filter_acronyms():
    global db_acronyms, db_filtered_abstracts
    create_folder('text')
    abstracts_index = {}
    for db_abstract in db_abstracts:
        abstracts_index[db_abstract['document_id']] = db_abstract['text']
    for db_acronym in db_acronyms:
        document_id = db_acronym['document_id']
        acronyms = db_acronym['acronyms']
        text = abstracts_index[document_id]
        sentences = []
        for text_item in text:
            try:
                _text = sent_tokenize(text_item)
            except TypeError:
                _text = []
            for _line in _text:
                sentences.append(_line)
        new_sentences = []
        for sentence in sentences:
            for acronym_pair in acronyms:
                acronym = acronym_pair['acronym']
                full_form = acronym_pair['full_form']
                choice = random.choice([0, 1])
                if choice == 1:
                    sentence = sentence.replace("("+acronym+")", "")
                elif choice == 0:
                    sentence = sentence.replace(full_form, "")
                    sentence = sentence.replace("(" + acronym + ")", acronym)
            new_sentences.append(sentence)
        insert_filtered_abstract(document_id, " ".join(new_sentences))
        db_filtered_abstracts.append({'document_id': document_id, 'sentences': [" ".join(new_sentences)]})
        create_file([" ".join(new_sentences)], "text/" + document_id)


def strip_acronym(text):
    text = text.replace("(", "")
    text = text.replace(")", "")
    text = text.replace(".", "")
    text = text.replace("!", "")
    text = text.replace(",", "")
    return text.strip()


def find_acronyms_in_string(text):
    result = []
    pattern = r'(\s\([A-Z]{2,}\)(?:\s|\,|\.|\!|\?))'
    for match in re.finditer(pattern, text):
        group = match.group()
        span = match.span()
        l_words = text[0:span[0]].strip().split(" ")
        r_words = text[span[1]:].strip().split(" ")
        left = " ".join(l_words[-CONTEXT_SIZE:])
        if left:
            left = left + " "
        right = " ".join(r_words[0:CONTEXT_SIZE])
        if right:
            right = " " + right
        context = left + group.strip() + right
        result.append({'original': group, 'striped': strip_acronym(group), 'span': span, 'context': context})
    return result


def find_acronyms():
    global db_found_acronyms
    for abstracts in db_filtered_abstracts:
        document_id = abstracts['document_id']
        for abstract in abstracts['sentences']:
            acronyms = find_acronyms_in_string(abstract)
            for acronym in acronyms:
                db_found_acronyms.append({'document_id': document_id, 'acronym': acronym})
                insert_found_acronym(document_id,
                                     acronym['striped'],
                                     ",".join(str(x) for x in acronym['span']),
                                     acronym['context'])


def find_unique_acronyms():
    global db_unique_acronyms
    for acronyms in db_found_acronyms:
        striped_acronym = acronyms['acronym']['striped']
        if striped_acronym not in db_unique_acronyms:
            db_unique_acronyms.append(striped_acronym)


def find_full_forms_in_string(text, acronym):
    result = []
    words = text.split(" ")
    chars = list(acronym)
    c = 0
    for word in words:
        passed = False
        d = 0
        for char in chars:
            position = c + d
            if position < len(words):
                tested_word = words[position]
                if tested_word and char == tested_word[0]:
                    if d == len(chars) - 1:
                        passed = True
                        break
                    d = d + 1
                    continue
                else:
                    break
        if passed:
            span = [c, c + len(chars)]
            full_form = " ".join(words[span[0]:span[1]])
            l_words = words[0:span[0]]
            r_words = words[span[1]:]
            left = " ".join(l_words[-CONTEXT_SIZE:])
            if left:
                left = left + " "
            right = " ".join(r_words[0:CONTEXT_SIZE])
            if right:
                right = " " + right
            context = left + full_form + right
            result.append({'full_form': strip_full_form(full_form), 'span': span, 'context': context})
        c = c + 1
    return result


def strip_full_form(full_form):
    if full_form.startswith(".") or full_form.startswith(","):
        full_form = full_form[1::]
    if full_form.endswith(",") or full_form.endswith(","):
        full_form = full_form[:-1]
    return full_form


def find_full_forms():
    global db_found_full_forms
    for abstracts in db_filtered_abstracts:
        document_id = abstracts['document_id']
        for abstract in abstracts['sentences']:
            for acronym in db_unique_acronyms:
                full_forms = find_full_forms_in_string(abstract, acronym)
                for full_form in full_forms:
                    db_found_full_forms.append({
                        'document_id': document_id,
                        'acronym': acronym,
                        'full_form': full_form})
                    insert_found_full_form(document_id,
                                           acronym,
                                           strip_full_form(full_form['full_form']),
                                           ",".join(str(x) for x in full_form['span']),
                                           full_form['context'])


def calculate_similarity():
    rows = select_similarity_candidates()
    for row in rows:
        sklearn_tfidf = TfidfVectorizer(norm='l2', min_df=0, use_idf=True, smooth_idf=False, sublinear_tf=True,
                                        tokenizer=tokenize)
        sklearn_representation = sklearn_tfidf.fit_transform([row['acronym_context'], row['full_form_context']]).toarray()
        cs = cosine_similarity(sklearn_representation[0], sklearn_representation[1])
        row['cosine_similarity'] = cs
        insert_similarity(row)


def main():
    create_tables()
    parse_files()
    put_abstracts_in_files()
    run_schwartz_algorithm()
    sort_acronyms()
    filter_acronyms()
    find_acronyms()
    find_unique_acronyms()
    find_full_forms()
    calculate_similarity()


if __name__ == "__main__":
    main()
