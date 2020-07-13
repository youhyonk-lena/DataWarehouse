import sys
import string
import unicodedata
import mysql.connector
import firebase_admin
from firebase_admin import db, credentials


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass

    try:
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass

    return False


def clean(s):

    if type(s) == int or type(s) == float:
        pass
    else:
        if s is None:
            pass
        else:
            try:
                s = float(s)
            except:
                table = s.maketrans(string.punctuation, " " * len(string.punctuation))
                s = ' '.join(s.translate(table).split())
    return s


def add_index(dictionary, table, index):

    idx = index

    for pk, dic in dictionary.items():
        primary = pk
        # if str(pk).isnumeric():
        #     pass
        # else:
        #     idx[pk] = [{"table": table, "column": 'primary', "primary": primary}]
        for k, v in dic.items():

            if v is None:
                pass
            else:
                if type(v) == str:
                    v = clean(v)
                    words = v.lower().split()
                    for w in words:
                        w = str(clean(w))
                        if w.split() == '' or is_number(w):
                            pass
                        else:
                            if w in idx.keys():
                                idx[w].append({"table": table, "column": k, "primary": primary})
                            else:
                                idx[w] = [{"table": table, "column": k, "primary": primary}]
                else:
                    pass

    return idx


def mysql_to_firebase(HOST, ID, PW, DB, ref):

    # connect to database
    connect = mysql.connector.connect(

        host = HOST,
        user = ID,
        password = PW,
        database = DB
    )

    cursor = connect.cursor(buffered=True)

    # get table names
    cursor.execute('show tables')
    tables = []
    for x in cursor:
        tables.append(str(x)[2: -3])

    #tables to firebase
    index = {}
    for table in tables:
        cursor.execute("DESC " + table)
        k = cursor.fetchall()

        keys = []
        for i in k:
            keys.append(i[0])

        table_ref = ref.child(table)
        dictionary = {}
        cursor.execute('SELECT * FROM ' + table)

        for value in cursor:
            dic = {}
            for i in range(0, len(value)):
                dic[keys[i]] = clean(value[i])
            dictionary[clean(value[0])] = dic

        table_ref.update(dictionary)

        index = add_index(dictionary, table, index)
        index_ref = ref.child('index')
        index_ref.update(index)
        print('Importing "' + table + '" from MySQL database "' + DB + '" to firebase "' + FB + '" complete')



if __name__ == "__main__":

    # gloabl variables
    HOST = 'localhost'
    ID = 'inf551'
    PW = 'inf551'
    DB = sys.argv[1]
    FB = sys.argv[2]


    cred = credentials.Certificate("inf551-bb52b-firebase-adminsdk-790xo-496bc32611.json")
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://inf551-bb52b.firebaseio.com/'
    })
    ref = db.reference('/' + FB)
    mysql_to_firebase(HOST, ID, PW, DB, ref)



