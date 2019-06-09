import sqlite3

conn = None


def create_connection():
    return sqlite3.connect('flexiterm.sqlite')


def get_acronyms():
    global conn
    rows = []
    try:
        if conn is None:
            conn = create_connection()
        c = conn.cursor()
        sql = """
            SELECT DISTINCT acronym FROM term_acronym
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


def get_phrases():
    global conn
    rows = []
    try:
        if conn is None:
            conn = create_connection()
        c = conn.cursor()
        sql = """
            SELECT DISTINCT phrase FROM term_phrase
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


def get_ids_of_phrases(phrase):
    global conn
    rows = []
    result = []
    try:
        if conn is None:
            conn = create_connection()
        c = conn.cursor()
        sql = """
                SELECT DISTINCT id FROM term_phrase WHERE phrase = ?
            """
        c.execute(sql, [phrase])
        columns = [col[0] for col in c.description]
        rows = [dict(zip(columns, row)) for row in c.fetchall()]
        conn.commit()
        c.close()
    except sqlite3.Error as e:
        print(e)
    except Exception as e:
        print(e)
    for row in rows:
        s = row['id'].split(".")
        result.append(s[0])
    return list(set(result))
