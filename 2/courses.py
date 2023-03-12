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

    course_students = """--sql
    CREATE TABLE IF NOT EXISTS Course_students (
            id integer primary key autoincrement not null,
            student_id integer,
            course_id integer,
            foreign key (student_id)
                references Students (id)
                    on delete cascade,
            foreign key (course_id)
                references Courses (id)
                    on delete cascade
    )
    """

    course_teachers = """--sql
    CREATE TABLE IF NOT EXISTS Course_teachers (
            id integer primary key autoincrement not null,
            course_id integer,
            teacher_id integer,
            foreign key (course_id)
                references Courses (id)
                    on delete cascade,
            foreign key (teacher_id)
                references Teachers (id)
                    on delete cascade
            )
    """

    groupsql = """--sql
    CREATE TABLE IF NOT EXISTS Groups (
        id integer primary key autoincrement not null,
        name varchar(255)
    )
    """

    group_students = """--sql
    CREATE TABLE IF NOT EXISTS Group_students (
            id integer primary key autoincrement not null,
            student_id integer not null,
            group_id integer not null,
            foreign key (student_id)
                references Students (id)
                    on delete cascade,
            foreign key (group_id)
                references Groups (id)
                    on delete cascade
            )
    """

    group_teachers = """--sql
    CREATE TABLE IF NOT EXISTS Group_teachers (
            id integer primary key autoincrement not null,
            teacher_id integer not null,
            group_id integer not null,
            foreign key (teacher_id)
                references Teachers (id)
                    on delete cascade,
            foreign key (group_id)
                references Groups (id)
                    on delete cascade
            )
    """

    completed_coursesql = """--sql
    CREATE TABLE IF NOT EXISTS Completed_courses (
        id integer primary key autoincrement not null,
        course_id integer not null,
        student_id integer not null,
        date date,
        grade integer,
        foreign key (course_id)
            references Courses(id)
                on delete cascade,
        foreign key (student_id)
            references Students(id)
                on delete cascade
    )
    """

    gradesql = """--sql
    CREATE TABLE IF NOT EXISTS Grades (
            id integer primary key not null
            )
    """

    db.execute(teachersql)
    db.execute(studentsql)
    db.execute(coursesql)
    db.execute(course_students)
    db.execute(course_teachers)
    db.execute(groupsql)
    db.execute(group_students)
    db.execute(group_teachers)
    db.execute(completed_coursesql)

    # because 30mins of reading stackoverflow won't give me an answer...
    db.execute(gradesql)
    for x in range(1, 6):
        db.execute("INSERT INTO Grades (id) VALUES (:x)", {"x": x})


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
        INSERT INTO Course_teachers (course_id, teacher_id)
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
    VALUES (:course_id, :student_id, :date, :grade_id)
    """
    db.execute(
        sql,
        {
            "course_id": course_id,
            "student_id": student_id,
            "date": date,
            "grade_id": grade,
        },
    )


# lisää ryhmän tietokantaan
def create_group(name, teacher_ids, student_ids):
    sql = """--sql
    INSERT INTO Groups (name) VALUES (:name) RETURNING id
    """
    group_id = db.execute(sql, {"name": name}).fetchone()[0]

    teacher_sql = """--sql
    INSERT INTO Group_teachers (teacher_id, group_id)
    VALUES (:teacher, :group_id)
    """
    student_sql = """--sql
    INSERT INTO Group_students (student_id, group_id)
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
    FROM Course_teachers
        JOIN Teachers ON Teachers.id=Course_teachers.teacher_id
        JOIN Courses ON Course_teachers.course_id=Courses.id
    WHERE Course_teachers.teacher_id=:id
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
        JOIN Course_teachers ON Course_teachers.course_id=Courses.id
    WHERE Course_teachers.teacher_id=:id
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
def credits_by_year(year: int) -> int:
    yearstr = str(year)
    sql = """--sql
    SELECT SUM(Courses.credits)
    FROM Completed_courses
        JOIN Courses ON Courses.id=Completed_courses.course_id
    WHERE SUBSTRING(Completed_courses.date, 1, 4)=:year
    """
    result = db.execute(sql, {"year": yearstr}).fetchone()
    if result:
        return result[0]
    else:
        return 0


# hakee kurssin arvosanojen jakauman (järjestyksessä arvosanat 1-5)
def grade_distribution(course_name: str) -> dict:
    sql = "SELECT id FROM Courses WHERE name=:name"
    id = db.execute(sql, {"name": course_name}).fetchone()
    if id:
        id = id[0]
    else:
        return {0: 0}
    sql = """--sql
    SELECT Grades.id, COUNT(*)
    FROM Completed_courses
        INNER JOIN Grades ON Grades.id=Completed_courses.grade
    WHERE Completed_courses.course_id=:id
    GROUP BY Grades.id
    """
    result = db.execute(sql, {"id": id}).fetchall()
    if result:
        dict = {}
        for x in range(1, 6):
            dict[x] = 0
        for item in result:
            dict[item[0]] = item[1]
        return dict
    else:
        return {0: 0}


# FIXME en ummarra miten laskea suorittajat.
# hakee listan kursseista (nimi, opettajien määrä, suorittajien määrä) (aakkosjärjestyksessä)
def course_list() -> list[str]:
    # failed attempt at doing this
    _sql = """--sql
    SELECT
        Courses.name,
        COUNT(Course_teachers.id),
        COUNT(Course_students.id)
    FROM Courses
        LEFT JOIN Course_teachers ON Course_teachers.course_id=Courses.id
        LEFT JOIN Course_students ON Course_students.course_id=Courses.id
    GROUP BY Courses.id
    ORDER BY Courses.name
    """

    # bruteforce attempt
    sql = "SELECT id FROM Courses"
    groups = db.execute(sql).fetchall()

    cursor = db.cursor()

    course_sql = "SELECT name FROM Courses WHERE id=:id"
    student_sql = """--sql
    SELECT count(*) FROM Course_students WHERE course_id=:id
    """
    teacher_sql = """--sql
    SELECT count(*) FROM Course_teachers WHERE course_id=:id
    """
    list = []
    for x in groups:
        x = x[0]
        result_course = cursor.execute(course_sql, {"id": x}).fetchall()
        result_student = cursor.execute(student_sql, {"id": x}).fetchall()[0]
        result_teacher = cursor.execute(teacher_sql, {"id": x}).fetchall()[0]
        print(str(result_course), result_teacher, result_student)
        list.append((str(result_course), result_teacher, result_student))

    if list:
        return list
    else:
        return [""]


# hakee listan opettajista kursseineen (aakkosjärjestyksessä opettajat ja kurssit)
def teacher_list() -> list[str]:
    # palauttaa ("opettajan nimi", "kurssi1, kurssi2") eikä ("opettaja", ["kurssi1", "kurssi2"])
    # enkä tiedä miten tehdä sitä fiksummin sql kanssa
    # en ollut ihan varma oliko tarkoitus saada puhtaasti sql tehtyä kaikki vai hieman sekoittaa pythonia

    # (halutun vastauksen olisi toki saanut vain yksittäinen_rivi[1].split(","),
    # ja sen tökännyt toiseksi tuplen jäseneksi)
    sql = """--sql
    SELECT Teachers.name, GROUP_CONCAT(Courses.name, ', ')
    FROM Teachers
        JOIN Course_teachers ON Course_teachers.teacher_id=Teachers.id
        JOIN Courses ON Course_teachers.course_id=Courses.id
    GROUP BY Teachers.id
    ORDER BY Teachers.name
    """
    result = db.execute(sql).fetchall()
    if result:
        return result
    else:
        return [""]


# hakee ryhmässä olevat henkilöt (aakkosjärjestyksessä)
def group_people(group_name: str) -> list[str]:
    # FIXME eepä toemi
    sql = """--sql
    SELECT id FROM Groups WHERE Groups.name=:group_name
    """
    id = db.execute(sql, {"group_name": group_name}).fetchone()
    if id:
        id = id[0]
    else:
        return [""]

    sql = """--sql
    SELECT Students.name as name
    FROM Students, Group_students
        WHERE Group_students.group_id=:id
    UNION
    SELECT Teachers.name as name
    FROM Teachers, Group_teachers
        WHERE Group_teachers.group_id=:id
    """
    result = db.execute(sql, {"id": id}).fetchone()
    if result:
        return result
    else:
        return [""]


# hakee ryhmissä saatujen opintopisteiden määrät (aakkosjärjestyksessä)
def credits_in_groups() -> list[str]:
    # FIXME meh
    sql = """--sql
    SELECT
        Groups.name,
        SUM()
    FROM
        Completed_courses,
    """
    return [""]


# hakee ryhmät, joissa on tietty opettaja ja opiskelija (aakkosjärjestyksessä)
def common_groups(teacher_name: str, student_name: str) -> list[str]:
    # FIXME cant manage lol
    sql = "SELECT id FROM Teachers WHERE name=:teacher_name"
    teacher_id = db.execute(sql, {"teacher_name": teacher_name}).fetchone()
    if teacher_id:
        teacher_id = teacher_id[0]
    else:
        return [""]
    sql = "SELECT id FROM Students WHERE name=:student_name"
    student_id = db.execute(sql, {"student_name": student_name}).fetchone()
    if student_id:
        student_id = student_id[0]
    else:
        return [""]

    sql = """--sql
    SELECT group_id FROM Group_teachers WHERE teacher_id=:teacher_id
    """
    teacher_groups = db.execute(sql, {"teacher_id": teacher_id}).fetchall()
    if teacher_groups:
        teacher_groups = [x[0] for x in teacher_groups]
    sql = """--sql
    SELECT group_id FROM Group_students WHERE student_id=:student_id
    """
    student_groups = db.execute(sql, {"student_id": student_id}).fetchall()
    if student_groups:
        student_groups = [x[0] for x in student_groups]

    groups = set(teacher_groups).intersection(student_groups)
