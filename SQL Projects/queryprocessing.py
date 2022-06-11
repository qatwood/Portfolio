#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys

# Donot close the connection inside this file i.e. do not perform openconnection.close()

def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
   
    conn = openconnection
    cur = conn.cursor()

    cur.execute("SELECT * FROM RangeRatingsMetadata")
    rangepartitions = cur.fetchall() 
    
    for r in rangepartitions:
     
        cur.execute("SELECT * FROM RangeRatingsPart" + str(r[0]) + " WHERE Rating >=" + str(ratingMinValue) + " AND Rating <=" + str(ratingMaxValue))
        rowswanted = cur.fetchall()
        rangename = "RangeRatingsPart" + str(r[0])
        
        writeToFile("RangeQueryOut.txt", rangename, rowswanted )
        

    num = 0
    while num < 5:
        
        cur.execute("SELECT * FROM RoundRobinRatingsPart" + str(num) + " WHERE Rating >=" + str(ratingMinValue) + " AND Rating <=" + str(ratingMaxValue))
        #table with rows (userid, movieid, rating) of rating between ratingMinValue and ratingMaxValue
        rrowswanted = cur.fetchall()
        roundname = "RoundRobinRatingsPart" + str(num)
    
        writeToFile("RangeQueryOut.txt", roundname, rrowswanted )
        num = num + 1
            
    cur.close()
    conn.commit()

def PointQuery(ratingsTableName, ratingValue, openconnection):
    conn = openconnection
    cur = conn.cursor()

    cur.execute("SELECT * FROM RangeRatingsMetadata")
    rangepartitions = cur.fetchall() 
    
    for r in rangepartitions:
     
        cur.execute("SELECT * FROM RangeRatingsPart" + str(r[0]) + " WHERE Rating =" + str(ratingValue))
        rowswanted = cur.fetchall()
        rangename = "RangeRatingsPart" + str(int(r[0]))
        
        writeToFile("PointQueryOut.txt", rangename, rowswanted )
        
    num = 0
    while num < 5:
        
        cur.execute("SELECT * FROM RoundRobinRatingsPart" + str(num) + " WHERE Rating =" + str(ratingValue))
        #table with rows (userid, movieid, rating) of rating between ratingMinValue and ratingMaxValue
        rrowswanted = cur.fetchall()
        roundname = "RoundRobinRatingsPart" + str(num)
    
        writeToFile("PointQueryOut.txt", roundname, rrowswanted )
        num = num + 1
            
    cur.close()
    conn.commit()


def writeToFile(filename, partitionname, rows):
    f = open(filename, 'a')
    for line in rows:
        f.write(partitionname + "," 
                    + str(line[0]) + "," + str(line[1]) 
                    + "," + str(line[2]) + "\n")
    f.close()
