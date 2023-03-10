import os
import sqlite3

# poistaa tietokannan alussa (kätevä moduulin testailussa)
os.remove("courses.db")

db = sqlite3.connect("courses.db")
db.isolation_level = None

# luo tietokantaan tarvittavat taulut
def create_tables():
    teachersql = """--sql
    CREATE TABLE IF NOT EXISTS Teachers (
        id integer primary key autoincrement not null,
        name varchar(255) not null
    )
    """
    coursesql = """--sql
    CREATE TABLE IF NOT EXISTS Courses (
        id integer primary key autoincrement not null,
        name varchar(255) not null
    )
    """

    studentsql = """--sql
    CREATE TABLE IF NOT EXISTS Students (
        id integer primary key autoincrement not null,
        name varchar(255) not null
    )
    """

    attendeesql = """--sql
    CREATE TABLE IF NOT EXISTS Attendees (
        id integer primary key autoincrement not null,
        course_id integer not null,
        student_id integer,
        teacher_id integer,
        foreign key (course_id)
            references Courses (id)
                on delete cascade,
        foreign key (student_id)
            references Students(id)
                on delete cascade,
        foreign key (teacher_id)
            references Teachers(id)
                on delete cascade
    )
    """

    db.execute(teachersql)
    db.execute(coursesql)
    db.execute(studentsql)
    db.execute(attendeesql)


# lisää opettajan tietokantaan
def create_teacher(name):
    pass


# lisää kurssin tietokantaan
def create_course(name, credits, teacher_ids):
    pass


# lisää opiskelijan tietokantaan
def create_student(name):
    pass


# antaa opiskelijalle suorituksen kurssista
def add_credits(student_id, course_id, date, grade):
    pass


# lisää ryhmän tietokantaan
def create_group(name, teacher_ids, student_ids):
    pass


# hakee kurssit, joissa opettaja opettaa (aakkosjärjestyksessä)
def courses_by_teacher(teacher_name):
    pass


# hakee opettajan antamien opintopisteiden määrän
def credits_by_teacher(teacher_name):
    pass


# hakee opiskelijan suorittamat kurssit arvosanoineen (aakkosjärjestyksessä)
def courses_by_student(student_name):
    pass


# hakee tiettynä vuonna saatujen opintopisteiden määrän
def credits_by_year(year):
    pass


# hakee kurssin arvosanojen jakauman (järjestyksessä arvosanat 1-5)
def grade_distribution(course_name):
    pass


# hakee listan kursseista (nimi, opettajien määrä, suorittajien määrä) (aakkosjärjestyksessä)
def course_list():
    pass


# hakee listan opettajista kursseineen (aakkosjärjestyksessä opettajat ja kurssit)
def teacher_list():
    pass


# hakee ryhmässä olevat henkilöt (aakkosjärjestyksessä)
def group_people(group_name):
    pass


# hakee ryhmissä saatujen opintopisteiden määrät (aakkosjärjestyksessä)
def credits_in_groups():
    pass


# hakee ryhmät, joissa on tietty opettaja ja opiskelija (aakkosjärjestyksessä)
def common_groups(teacher_name, student_name):
    pass