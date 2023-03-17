import os
import stat
import sqlite3
from time import time
from secrets import token_hex
from random import randint

path = "/home/martti/docs/uni/tikape/3/index.db"
open(path, "w").close()


def reset_db():
    # i dont know what this is lol
    os.remove("index.db")
    open(path, "w").close()
    os.chmod(path, stat.S_IWOTH)
    return sqlite3.connect("index.db")


def take_time(beginning_time: float) -> float:
    return round(time() - beginning_time, 5)


def check_file_size():
    return os.path.getsize(path) / 1024**2


def reset_table(db):
    cursor = db.cursor()
    sql = "DROP TABLE IF EXISTS Elokuvat"
    cursor.execute(sql)
    db.commit()


def init(db):
    cursor = db.cursor()
    reset_table(db)
    sql = """--sql
    CREATE TABLE IF NOT EXISTS Elokuvat (
            id integer primary key autoincrement not null,
            nimi varchar(9) not null,
            vuosi integer not null
            )
    """
    cursor.execute(sql)
    db.commit()


def init_with_id(db):
    cursor = db.cursor()
    sql = """--sql
    CREATE TABLE IF NOT EXISTS Elokuvat (
            id integer primary key autoincrement not null,
            nimi varchar(9) not null,
            vuosi integer not null
            )
            """
    cursor.execute(sql)
    cursor.execute("CREATE INDEX idx_vuosi ON Elokuvat (vuosi)")
    db.commit()


def insert_rows(db):
    cursor = db.cursor()
    sql = "INSERT INTO Elokuvat (nimi, vuosi) VALUES (:nimi, :vuosi)"

    for _ in range(10**6):
        cursor.execute(
            sql,
            {"nimi": token_hex(), "vuosi": randint(1900, 2000)},
        )
    db.commit()


def create_index(db):
    cursor = db.cursor()
    sql = "CREATE INDEX idx_vuosi ON Elokuvat (vuosi)"
    cursor.execute(sql)
    db.commit()


def test(db):
    cursor = db.cursor()
    sql = """SELECT COUNT(*) FROM Elokuvat WHERE vuosi=:vuosi"""
    for _ in range(1000):
        vuosi = randint(1900, 2000)
        cursor.execute(sql, {"vuosi": vuosi})

    db.commit()


def main():
    db = reset_db()
    init(db)
    rows = time()
    insert_rows(db)
    print("inserting took:", take_time(rows))
    test_time = time()
    test(db)
    print("test took:", take_time(test_time))
    print(check_file_size())
    print()

    db = reset_db()
    init_with_id(db)
    rows = time()
    insert_rows(db)
    print("inserting took:", take_time(rows))
    test_time = time()
    test(db)
    print("test took:", take_time(test_time))
    print(check_file_size())
    print()

    db = reset_db()
    init(db)
    rows = time()
    insert_rows(db)
    print("inserting took:", take_time(rows))
    create_index(db)
    test_time = time()
    test(db)
    print("test took:", take_time(test_time))
    print(check_file_size())
    print()

    db.close()


def test_test():
    db = sqlite3.connect("index.db")

    init(db)
    rows = time()
    insert_rows(db)
    print("inserting took:", take_time(rows))
    test_time = time()
    test(db)
    print("test took:", take_time(test_time))
    print(check_file_size())
    print()


if __name__ == "__main__":
    main()
