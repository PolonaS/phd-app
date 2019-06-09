import sqlite3

conn = None


def create_connection():
    return sqlite3.connect('compare.sqlite')


def create_compare_table():
    table_name = "compare"
    drop_table(table_name)
    sql = """
            CREATE TABLE IF NOT EXISTS 
            """ + table_name + """(
                id integer PRIMARY KEY AUTOINCREMENT,
                acronym text,
                acronym_context text,
                definition text,
                definition_context text,
                similarity decimal(18,12)
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
    create_compare_table()


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


def insert_compare(acronym, acronym_context, definition, definition_context, similarity):
    insert("compare", {
        'acronym': acronym,
        'acronym_context': acronym_context,
        'definition': definition,
        'definition_context': definition_context,
        'similarity': similarity
    })
