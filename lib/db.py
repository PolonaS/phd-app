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
                abstract text
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
        ('""" + "', '".join(values) + """')
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
    insert("abstracts", {'document_id': document_id, 'abstract': abstract})


def insert_acronym(document_id, acronym, full_form):
    insert("acronyms", {'document_id': document_id, 'acronym': acronym, 'full_form': full_form})


def insert_filtered_abstract(document_id, sentence):
    insert("abstracts", {'document_id': document_id, 'sentence': sentence})
