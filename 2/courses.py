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
        name varchar(255) not null,
        credits int
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
        course_id integer,
        group_id integer,
        student_id integer,
        teacher_id integer,
        foreign key (course_id)
            references Courses (id)
                on delete cascade,
        foreign key (group_id)
            references Groups (id)
                on delete cascade,
        foreign key (student_id)
            references Students(id)
                on delete cascade,
        foreign key (teacher_id)
            references Teachers(id)
                on delete cascade
    )
    """

    groupsql = """--sql
    CREATE TABLE IF NOT EXISTS Groups (
        id integer primary key autoincrement not null,
        name varchar(255)
    )
    """

    completed_coursesql = """--sql
    CREATE TABLE IF NOT EXISTS Completed_courses (
        id integer primary key autoincrement not null,
        course_id integer not null,
        student_id integer not null,
        date varchar(255),
        grade integer,
        foreign key (course_id)
            references Courses(id)
                on delete cascade,
        foreign key (student_id)
            references Students(id)
                on delete cascade
    )
    """

    db.execute(teachersql)
    db.execute(coursesql)
    db.execute(studentsql)
    db.execute(attendeesql)
    db.execute(groupsql)
    db.execute(completed_coursesql)


# lisää opettajan tietokantaan
def create_teacher(name: str) -> int:
    sql = "INSERT INTO Teachers (name) VALUES (:name) RETURNING id"
    result = db.execute(sql, {"name": name}).fetchone()
    if result:
        return result[0]
    else:
        return -1


# lisää kurssin tietokantaan
def create_course(name: str, credits: int, teacher_ids: list[int]) -> int:
    sql = "INSERT INTO Courses (name, credits) VALUES (:name, :credits) RETURNING id"
    id = db.execute(sql, {"name": name, "credits": credits}).fetchone()
    if not id:
        return -1
    id = id[0]

    sql = """--sql
        INSERT INTO Attendees (course_id, teacher_id)
        VALUES (:course_id, :teacher_id)"""

    for teacher in teacher_ids:
        db.execute(sql, {"course_id": id, "teacher_id": teacher})
    return id


# lisää opiskelijan tietokantaan
def create_student(name: str) -> int:
    sql = """--sql
    INSERT INTO Students (name)
    VALUES (:name)
    RETURNING id
    """
    result = db.execute(sql, {"name": name}).fetchone()
    if result:
        return result[0]
    else:
        return -1


# antaa opiskelijalle suorituksen kurssista
def add_credits(student_id: int, course_id: int, date: str, grade: int):
    sql = """--sql
    INSERT INTO Completed_courses (course_id, student_id, date, grade)
    VALUES (:course_id, :student_id, :date, :grade)
    """
    db.execute(sql, {"course_id": course_id, "student_id": student_id, "date": date, "grade": grade})


# lisää ryhmän tietokantaan
def create_group(name, teacher_ids, student_ids):
    sql = """--sql
    INSERT INTO Groups (name) VALUES (:name) RETURNING id
    """
    group_id = db.execute(sql, {"name": name}).fetchone()[0]

    teacher_sql = """--sql
    INSERT INTO Attendees (teacher_id, group_id)
    VALUES (:teacher, :group_id)
    """
    student_sql = """--sql
    INSERT INTO Attendees (student_id, group_id)
    VALUES (:student, :group_id)
    """
    for teacher in teacher_ids:
        db.execute(teacher_sql, {"teacher": teacher, "group_id": group_id})
    for student in student_ids:
        db.execute(student_sql, {"student": student, "group_id": group_id})


# hakee kurssit, joissa opettaja opettaa (aakkosjärjestyksessä)
def courses_by_teacher(teacher_name: str) -> list[str]:
    sql = "SELECT id FROM Teachers WHERE name=:teacher_name"
    id = db.execute(sql, {"teacher_name": teacher_name}).fetchone()
    if id:
        id = id[0]
    else:
        return [""]

    sql = """--sql
    SELECT Courses.name
    FROM Attendees
        JOIN Teachers ON Teachers.id=Attendees.teacher_id
        JOIN Courses ON Attendees.course_id=Courses.id
    WHERE Attendees.teacher_id=:id
    ORDER BY Teachers.name DESC
    """
    result = db.execute(sql, {"id": id}).fetchall()
    if result:
        for index, row in enumerate(result):
            result[index] = row[0]
        return result
    else:
        return [""]


# hakee opettajan antamien opintopisteiden määrän
def credits_by_teacher(teacher_name: str) -> int:
    sql = "SELECT id FROM Teachers WHERE name=:teacher_name"
    id = db.execute(sql, {"teacher_name": teacher_name}).fetchone()
    if id:
        id = id[0]
    else:
        return 0

    sql = """--sql
    SELECT SUM(Courses.credits)
    FROM Completed_courses
        JOIN Courses ON Courses.id=Completed_courses.course_id
        JOIN Attendees ON Attendees.course_id=Courses.id
    WHERE Attendees.teacher_id=:id
    """

    result = db.execute(sql, {"id": id}).fetchone()
    if result:
        return result[0]
    return 0


# hakee opiskelijan suorittamat kurssit arvosanoineen (aakkosjärjestyksessä)
def courses_by_student(student_name: str) -> list[str]:
    sql = "SELECT id FROM Students WHERE name=:student_name"
    id = db.execute(sql, {"student_name": student_name}).fetchone()
    if id:
        id = id[0]
    else:
        return [""]

    sql = """--sql
    SELECT Courses.name, Completed_courses.grade
    FROM Completed_courses
        JOIN Courses ON Courses.id=Completed_courses.course_id
    WHERE Completed_courses.student_id=:id
    """
    result = db.execute(sql, {"id": id}).fetchall()
    if result:
        return result
    else:
        return [""]


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
