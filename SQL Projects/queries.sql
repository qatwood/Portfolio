--QUERY 1
CREATE TABLE query1 AS
SELECT genres.name, COUNT(hasagenre.genreid) as "moviecount"
FROM genres, hasagenre
WHERE genres.genreid = hasagenre.genreid
GROUP BY name;

--QUERY 2
CREATE TABLE query2 AS
SELECT genres.name, AVG(ratings.rating) as "rating"
FROM genres, ratings, hasagenre
WHERE ratings.movieid = hasagenre.movieid AND genres.genreid = hasagenre.genreid
GROUP BY name;

--QUERY 3
CREATE TABLE query3 AS
SELECT movies.title, COUNT(ratings.rating) AS "countofratings"
FROM movies, ratings
WHERE movies.movieid= ratings.movieid
GROUP BY title
HAVING COUNT(ratings.rating) >= 10;

--QUERY 4
CREATE TABLE query4 AS
SELECT movies.movieid, movies.title
FROM movies, hasagenre, genres
WHERE movies.movieid = hasagenre.movieid AND hasagenre.genreid = genres.genreid
		AND genres.name LIKE '%Comedy%';

--QUERY 5
CREATE TABLE query5 AS
SELECT movies.title, AVG(ratings.rating) AS "average"
FROM movies, ratings
WHERE ratings.movieid = movies.movieid
GROUP BY title;

--QUERY 6
CREATE TABLE query6 AS
SELECT AVG(ratings.rating) AS "average"
FROM ratings, hasagenre, genres
WHERE ratings.movieid = hasagenre.movieid AND hasagenre.genreid = genres.genreid
	AND genres.name LIKE '%Comedy%';

--QUERY 7
CREATE TABLE query7 AS
SELECT AVG(ratings.rating) AS "average"
FROM ratings
WHERE ratings.movieid IN
				(SELECT comedy.movieid
				FROM
						(SELECT movieid
						FROM hasagenre, genres
						WHERE hasagenre.genreid= genres.genreid AND genres.name LIKE '%Comedy%') AS comedy
					JOIN
						(SELECT movieid
						FROM hasagenre, genres
						WHERE hasagenre.genreid= genres.genreid AND genres.name LIKE '%Romance%') AS romance
				ON comedy.movieid = romance.movieid);

--QUERY 8
CREATE TABLE query8 AS
SELECT AVG(ratings.rating) AS "average"
FROM ratings
WHERE ratings.movieid IN
				(SELECT hasagenre.movieid
				FROM hasagenre, genres
				WHERE hasagenre.genreid= genres.genreid AND genres.name LIKE '%Romance%' AND hasagenre.movieid NOT IN
  						(SELECT hasagenre.movieid
  						FROM hasagenre, genres
  						WHERE hasagenre.genreid= genres.genreid AND genres.name LIKE '%Comedy%'));


--QUERY 9
CREATE TABLE query9 AS
SELECT ratings.movieid, ratings.rating
FROM ratings
WHERE ratings.userid = :v1;

--QUERY 10
--average rating for movies rated
CREATE TABLE l_avgrating AS
SELECT query9.movieid AS movieid1, AVG(ratings.rating) AS lavg
FROM ratings, query9
WHERE query9.movieid = ratings.movieid
GROUP BY movieid1;

--average rating for movies unrated
CREATE TABLE i_avgrating AS
SELECT ratings.movieid AS movieid2, AVG(ratings.rating) AS iavg
FROM  query9, ratings
WHERE ratings.movieid <> query9.movieid
GROUP BY movieid2;

--similarity table
CREATE TABLE similarity AS
SELECT movieid1, movieid2, (1-(ABS(iavg-lavg)/5)) AS sim
FROM l_avgrating
CROSS JOIN i_avgrating;

--recommendation table
--rating for movies rated
CREATE TABLE rating_l AS
SELECT l_avgrating.movieid1, ratings.rating
FROM ratings, l_avgrating
WHERE ratings.movieid = l_avgrating.movieid1
			AND ratings.userid = :v1;

--presummation numerator
CREATE TABLE sim_times_rating AS
SELECT similarity.movieid2, similarity.sim,
			similarity.sim * rating_l.rating AS numerator
FROM similarity, rating_l;

--summation
CREATE TABLE prediction AS
SELECT movieid2
FROM sim_times_rating
GROUP BY movieid2
HAVING SUM(numerator) /
			SUM(sim) > 3.9;

CREATE TABLE recommendation AS
SELECT movies.title
FROM movies, prediction
WHERE movies.movieid = prediction.movieid2;
