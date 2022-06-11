#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    conn = openconnection
    cur = conn.cursor()
    cur.execute("CREATE TABLE " + ratingstablename + 
                """(UserID INTEGER, 
                char1 char,
                MovieID INTEGER, 
                char2 char,
                Rating float,
                char3 char,
                Timestamp bigint)""")

    data = open(ratingsfilepath, 'r')
    cur.copy_from(data, ratingstablename, sep = ':')
    cur.close()
    conn.commit()

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    conn = openconnection
    cur = conn.cursor()

    partitionrange = 5.0/ numberofpartitions
    x = 0
    n = 0

    while x < 5.0:
        if x == 0:
            cur.execute ("CREATE TABLE range_part" + str(n) + 
            "(UserID INTEGER, MovieID INTEGER, Rating float)")

            cur.execute ("INSERT INTO range_part" + str(n) +
            """(UserID, MovieID, Rating) 
            SELECT Userid, Movieid, Rating
            FROM """ + ratingstablename +
            " WHERE Rating >= " + str (x) +
            " AND Rating <= " + str (x + partitionrange))
            x = x + partitionrange
            n = n + 1
        else:
            cur.execute ("CREATE TABLE range_part" + str(n) + 
            "(UserID INTEGER, MovieID INTEGER, Rating float)")
            
            cur.execute ("INSERT INTO range_part" + str(n) +
            """(UserID, MovieID, Rating) 
            SELECT UserID, MovieID, Rating
            FROM """ + ratingstablename +
            " WHERE Rating > " + str (x) +
            " AND Rating <= " + str (x + partitionrange))
            x = x + partitionrange
            n = n + 1

    cur.close()
    conn.commit()
    

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    conn = openconnection
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE 'range_part%'")
    partitionnum = cur.fetchone()[0]
    partitionrange = 5.0/ partitionnum
    
    x = 0
    n = 0

    while x < 5.0:
        if x == 0:
            if rating >= 0 and rating <= x + partitionrange:
                x = x + partitionrange
                cur.execute("INSERT INTO range_part" + str(n) +
                    " (UserID, MovieID, Rating) VALUES (" 
                    + str(userid) 
                    + ", " + str(itemid) 
                    + ", " + str(rating) + ")")
                n = n + 1
            else: 
                x = x + partitionrange
                cur.execute("INSERT INTO range_part" + str(n) +
                    " (UserID, MovieID, Rating) VALUES (" 
                    + str(userid) 
                    + ", " + str(itemid) 
                    + ", " + str(rating) + ")")
                n = n + 1
        else: 
            if rating > x and rating <= x + partitionrange:
                x = x + partitionrange
                cur.execute("INSERT INTO range_part" + str(n) +
                    " (UserID, MovieID, Rating) VALUES (" 
                    + str(userid) 
                    + ", " + str(itemid) 
                    + ", " + str(rating) + ")")
                n = n + 1
            else: 
                x = x + partitionrange
                cur.execute("INSERT INTO range_part" + str(n) +
                    " (UserID, MovieID, Rating) VALUES (" 
                    + str(userid) 
                    + ", " + str(itemid) 
                    + ", " + str(rating) + ")")
                n = n + 1

    cur.close()
    conn.commit()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    conn = openconnection
    cur = conn.cursor()

    n = 0

    while n < numberofpartitions:
        cur.execute ("CREATE TABLE rrobin_part" + str(n) +
        """ AS SELECT UserID, MovieID, Rating 
            FROM (
                SELECT UserID, MovieID, Rating, ROW_NUMBER() OVER() AS row
                FROM """ + ratingstablename + ") AS x" +
            " WHERE " + str(n) + " = (x.row-1) % 5" )

        n = n + 1
  
    cur.close()
    conn.commit()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    conn = openconnection
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE 'rrobin_part%'")
    partitionnum = cur.fetchone()[0]

    n = 0
    for n in range(0, partitionnum):
        cur.execute ("INSERT INTO rrobin_part" + str(n) +
                    "(UserID, MovieID, Rating) VALUES(" + str(userid) + ", " + str(itemid) + ", " + str(rating) + ")" )
        n = n + 1

    cur.close()
    conn.commit()


def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()
