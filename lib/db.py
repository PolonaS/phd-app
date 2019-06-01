import sqlite3

conn = None


def create_connection():
    return sqlite3.connect('phd-app.sqlite')


def create_abstracts_table():
    table_name = "abstracts"
    drop_table(table_name)
    sql = """
            CREATE TABLE IF NOT EXISTS 
            """ + table_name + """(
                id integer PRIMARY KEY AUTOINCREMENT,
                document_id text,
                document text
            )
            """
    create_table(table_name, sql)


def create_acronyms_table():
    table_name = "acronyms"
    drop_table(table_name)
    sql = """
            CREATE TABLE IF NOT EXISTS 
            """ + table_name + """(
                id integer PRIMARY KEY AUTOINCREMENT,
                document_id text,
                acronym text,
                full_form text
            )
            """
    create_table(table_name, sql)


def create_filtered_abstracts_table():
    table_name = "filtered_abstracts"
    drop_table(table_name)
    sql = """
        CREATE TABLE IF NOT EXISTS 
        """ + table_name + """(
            id integer PRIMARY KEY AUTOINCREMENT,
            document_id text,
            sentence text
        )
        """
    create_table(table_name, sql)


def create_found_acronyms_table():
    table_name = "found_acronyms"
    drop_table(table_name)
    sql = """
            CREATE TABLE IF NOT EXISTS 
            """ + table_name + """(
                id integer PRIMARY KEY AUTOINCREMENT,
                document_id text,
                acronym_original text,
                acronym_striped text,
                acronym_span text,
                acronym_context text
            )
            """
    create_table(table_name, sql)


def create_found_full_forms_table():
    table_name = "found_full_forms"
    drop_table(table_name)
    sql = """
            CREATE TABLE IF NOT EXISTS 
            """ + table_name + """(
                id integer PRIMARY KEY AUTOINCREMENT,
                document_id text,
                acronym text,
                full_form text,
                full_form_span text,
                full_form_context text
            )
            """
    create_table(table_name, sql)


def create_similarity_table():
    table_name = "similarity"
    drop_table(table_name)
    sql = """
            CREATE TABLE IF NOT EXISTS 
            """ + table_name + """(
                id integer PRIMARY KEY AUTOINCREMENT,
                ff_document_id text,
                fa_document_id text,
                acronym text,
                full_form text,
                acronym_context text,
                full_form_context text,
                cosine_similarity text
            )
            """
    create_table(table_name, sql)


def create_table(table_name, sql):
    global conn
    try:
        if conn is None:
            conn = create_connection()
        c = conn.cursor()
        c.execute(sql)
        c.execute("DELETE FROM " + table_name)
        conn.commit()
        c.close()
    except sqlite3.Error as e:
        print(e)
    except Exception as e:
        print(e)


def drop_table(table_name):
    global conn
    try:
        if conn is None:
            conn = create_connection()
        c = conn.cursor()
        c.execute("DROP TABLE IF EXISTS " + table_name)
        conn.commit()
        c.close()
    except sqlite3.Error as e:
        print(e)
    except Exception as e:
        print(e)


def create_tables():
    create_abstracts_table()
    create_acronyms_table()
    create_filtered_abstracts_table()
    create_found_acronyms_table()
    create_found_full_forms_table()
    create_similarity_table()


def insert(table_name, fields):
    global conn
    values = []
    columns = fields.keys()
    for column in columns:
        values.append(fields[column])
    sql = """
        INSERT INTO """ + table_name + """
        (""" + ", ".join(columns) + """)
        VALUES
        ('""" + "', '".join(str(x) for x in values) + """')
    """
    try:
        if conn is None:
            conn = create_connection()
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
    except sqlite3.Error as e:
        print(e)
    except Exception as e:
        print(e)


def insert_abstract(document_id, abstract):
    insert("abstracts", {'document_id': document_id, 'document': abstract})


def insert_acronym(document_id, acronym, full_form):
    insert("acronyms", {'document_id': document_id, 'acronym': acronym, 'full_form': full_form})


def insert_filtered_abstract(document_id, sentence):
    insert("filtered_abstracts", {'document_id': document_id, 'sentence': sentence})


def insert_found_acronym(document_id, acronym_original, acronym_striped, acronym_span, acronym_context):
    insert("found_acronyms", {
        'document_id': document_id,
        'acronym_original': acronym_original,
        'acronym_striped': acronym_striped,
        'acronym_span': acronym_span,
        'acronym_context': acronym_context
    })


def insert_found_full_form(document_id, acronym, full_form, full_form_span, full_form_context):
    insert("found_full_forms", {
        'document_id': document_id,
        'acronym': acronym,
        'full_form': full_form,
        'full_form_span': full_form_span,
        'full_form_context': full_form_context
    })


def insert_similarity(similarity_row):
    insert("similarity", similarity_row)


def select_similarity_candidates():
    global conn
    rows = []
    try:
        if conn is None:
            conn = create_connection()
        c = conn.cursor()

        sql = """
            select
              fff.document_id as ff_document_id,
              fa.document_id as fa_document_id,
              fff.acronym,
              fff.full_form,
              group_concat(fa.acronym_context) as acronym_context,
              group_concat(fff.full_form_context) as full_form_context
            from found_full_forms fff
            left join found_acronyms fa
              on fa.acronym_striped = fff.acronym
            where fa.document_id is not null
            group by fff.acronym, fff.full_form
            order by fff.acronym, fff.full_form
        """

        c.execute(sql)
        columns = [col[0] for col in c.description]
        rows = [dict(zip(columns, row)) for row in c.fetchall()]
        conn.commit()
        c.close()
    except sqlite3.Error as e:
        print(e)
    except Exception as e:
        print(e)
    return rows
