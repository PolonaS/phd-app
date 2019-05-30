import xml.etree.ElementTree
import shutil
import os
import random
import nltk
from abbreviations import schwartz_hearst
from nltk.tokenize import sent_tokenize
from lib.db import create_tables, insert_abstract, insert_acronym, insert_filtered_abstract


nltk.download('punkt')

ABSTRACTS_PER_FILE = 1000


db_abstracts = []
db_acronyms = []
db_acronym_index = []
db_filtered_abstracts = []


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
                results.append(child.text)
        parse_abstracts(child, results)
    return results


def parse_file(file):
    global db_abstracts
    count_abstracts = 0
    root = xml.etree.ElementTree.parse(file).getroot()
    for article in root:
        cit = article.find("MedlineCitation")
        if cit:
            document_id = cit.find("PMID").text
            abstracts = parse_abstracts(article)
            if abstracts:
                inserted_abstracts = []
                for a in abstracts:
                    inserted_abstracts.append(a)
                    insert_abstract(document_id, a)
                    count_abstracts = count_abstracts + 1
                    if count_abstracts == ABSTRACTS_PER_FILE:
                        db_abstracts.append({'document_id': document_id, 'text': inserted_abstracts})
                        return
                db_abstracts.append({'document_id': document_id, 'text': inserted_abstracts})


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
    create_folder('text')
    for abstract in db_abstracts:
        create_file(abstract['text'], "text/" + abstract['document_id'])


def run_schwartz_algorithm():
    global db_acronyms
    for file in os.listdir("./text"):
        pairs = schwartz_hearst.extract_abbreviation_definition_pairs(file_path="./text/" + file)
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
    create_folder('filtered_text')
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
            new_sentences.append(sentence)
            insert_filtered_abstract(document_id, sentence)
        db_filtered_abstracts.append({'document_id': document_id, 'sentences': new_sentences})
        create_file(new_sentences, "filtered_text/" + document_id)


def main():
    create_tables()
    parse_files()
    put_abstracts_in_files()
    run_schwartz_algorithm()
    sort_acronyms()
    filter_acronyms()
    for x in db_acronyms:
        print(x)


if __name__ == "__main__":
    main()
