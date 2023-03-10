import sqlite3

db = sqlite3.connect("bikes.db")
db.isolation_level = None


def get_user_id(user: str) -> int:
    sql = "SELECT id FROM Users WHERE name=:user"
    result = db.execute(sql, {"user": user}).fetchone()
    if result:
        return result[0]
    else:
        raise ValueError("No user with username found")


def get_city_id(city: str) -> int:
    sql = "SELECT id FROM Cities WHERE name=:city"
    result = db.execute(sql, {"city": city}).fetchone()
    if result:
        return result[0]
    else:
        raise ValueError("No city with id found")


def distance_of_user(user: str) -> int:
    try:
        id = get_user_id(user)
    except ValueError as e:
        return f"Error, {e}"

    sql = "SELECT SUM(Trips.distance) FROM Trips WHERE Trips.user_id=:id"
    result = db.execute(sql, {"id": id}).fetchone()
    if result:
        return result[0]
    return "0"


def speed_of_user(user: str) -> int:
    try:
        id = get_user_id(user)
    except ValueError as e:
        return f"Error, {e}"

    sql = "SELECT sum(distance), sum(duration) FROM Trips WHERE user_id=:id"
    result = db.execute(sql, {"id": id}).fetchone()
    if result:
        return f"{(result[0] / result[1]*60)/1000:.2f}"
    return "0"


def duration_in_each_city(day: str) -> int:
    sql = """
    SELECT Cities.name, SUM(Trips.duration) as durSum
    FROM Trips
        JOIN Bikes ON Trips.bike_id=Bikes.id
        JOIN Cities ON Bikes.city_id=Cities.id
    WHERE Trips.day=:day
    GROUP BY Bikes.city_id
    ORDER BY Cities.name
    """
    result = db.execute(sql, {"day": day}).fetchall()
    if result:
        return result
    else:
        return "No durations found"


def users_in_city(city: str) -> int:
    try:
        id = get_city_id(city)
    except ValueError as e:
        return f"Error, {e}"

    # count all users who have city_id in stops
    sql = """
    SELECT count(*) FROM (
    SELECT count(*)
    FROM Trips 
        JOIN Stops fromT ON Trips.from_id=fromT.id
        JOIN Stops toT ON Trips.to_id=toT.id
    WHERE fromT.city_id=:id OR toT.city_id=:id
    GROUP BY Trips.user_id
    )
    """
    result = db.execute(sql, {"id": id}).fetchone()
    if result:
        return result[0]
    else:
        return "0"


def trips_on_each_day(city: str) -> int:
    try:
        id = get_city_id(city)
    except ValueError as e:
        return f"Error, {e}"
    
    sql = """
    SELECT Trips.day, count(*)
    FROM Trips
        JOIN Stops fromT ON Trips.from_id=fromT.id
        JOIN Stops toT ON Trips.to_id=toT.id
    WHERE fromT.city_id=:id OR toT.city_id=:id
    GROUP BY Trips.day
    ORDER BY Trips.day
    """

    result = db.execute(sql, {"id": id}).fetchall()
    if result:
        return result
    else:
        return "No trips"



def most_popular_start(city: str) -> int:
    try:
        id = get_city_id(city)
    except ValueError as e:
        return f"Error, {e}"

    sql = """
    SELECT Stops.name, count(*) as tripCount
    FROM Stops
        JOIN Trips ON Trips.from_id=Stops.id
    WHERE Stops.city_id=:id
    GROUP BY Stops.id
    ORDER BY tripCount DESC
    LIMIT 1
    """ 

    result = db.execute(sql, {"id": id}).fetchone()
    if result:
        return result
    else:
        return "No starts"
