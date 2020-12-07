DROP TABLE PUBLIC.name;
CREATE TABLE PUBLIC.name(
	index int,
	nconst varchar,
	primaryName varchar,
	birthYear float8,
	deathYear float8
);
COPY PUBLIC.name FROM '/Users/xintongli/PycharmProjects/Project/Film Data Analysis/name_temp.csv' 
WITH CSV HEADER DELIMITER ',';
ALTER TABLE name
DROP COLUMN index;

ALTER TABLE name
ADD COLUMN age float8;
UPDATE name SET age = name.deathyear - name.birthyear;
SELECT * FROM name
ORDER BY nconst;

/*Check reliability of data*/
SELECT MIN(birthyear) minbirth, MAX(birthyear) maxbirth, AVG(birthyear) avgbirth, STDDEV(birthyear) stdbirth,
MIN(deathyear) mindeath, MAX(deathyear) maxdeath, AVG(deathyear) avgdeath, STDDEV(deathyear) stddeath,
MIN(age) minage, MAX(age) maxage, AVG(age) avgage, STDDEV(age) stdage
FROM name;
/*birthyear should not be 6| deathyear is negative| age is negative;
find and deal with them*/
SELECT * FROM name
WHERE birthyear = 6;
DELETE FROM name
WHERE birthyear = 6;

SELECT * FROM name
WHERE name.age<0;
/*Switch number*/
UPDATE name
SET birthyear=1916, deathyear=1967
WHERE age=-51;
UPDATE name
SET birthyear=1905, deathyear=1932
WHERE age=-1905;

UPDATE name SET age = name.deathyear - name.birthyear;
SELECT * FROM name
ORDER BY nconst;
select * from name;

DROP TABLE data;
CREATE TABLE PUBLIC.data(
	index int,
	title varchar,
	region varchar,
	tconst varchar,
	startYear float8,
	runtimeMinutes float8,
	genres varchar,
	directors varchar,
    averageRating float8,
	numVotes float8
);
COPY PUBLIC.data FROM '/Users/xintongli/PycharmProjects/Project/Film Data Analysis/Data_temp.csv' 
WITH CSV HEADER DELIMITER ',';
ALTER TABLE data
DROP COLUMN index

/*Check reliability of data*/
SELECT MIN(startyear) minyear, MAX(startyear) maxyear, AVG(startyear) avgyear, STDDEV(startyear) stdyear,
MIN(runtimeminutes) minruntime, MAX(runtimeminutes) maxruntime, AVG(runtimeminutes) avgruntime, STDDEV(runtimeminutes) stdruntime,
MIN(averagerating) minrate, MAX(averagerating) maxrate, AVG(averagerating) avgrate, STDDEV(averagerating) stdrate,
MIN(numvotes) minvote, MAX(numvotes) maxvote, AVG(numvotes) avgvotes, STDDEV(numvotes) stdvote
FROM data;
/*maxruntime 51420*/
SELECT * FROM data
WHERE runtimeminutes = 51420;
/*Weird, but this movie does exist!*/

/*DROP TABLE data;*/
CREATE TABLE PUBLIC.data2(
	index int,
	title varchar,
	region varchar,
	tconst varchar,
	startYear float8,
	runtimeMinutes float8,
	genres varchar,
	directors varchar,
    averageRating float8,
	numVotes float8
);
COPY PUBLIC.data2 FROM '/Users/xintongli/PycharmProjects/Project/Film Data Analysis/Data_sql.csv' 
WITH CSV HEADER DELIMITER ',';
ALTER TABLE data2
DROP COLUMN index

/*Check reliability of data*/
SELECT MIN(startyear) minyear, MAX(startyear) maxyear, AVG(startyear) avgyear, STDDEV(startyear) stdyear,
MIN(runtimeminutes) minruntime, MAX(runtimeminutes) maxruntime, AVG(runtimeminutes) avgruntime, STDDEV(runtimeminutes) stdruntime,
MIN(averagerating) minrate, MAX(averagerating) maxrate, AVG(averagerating) avgrate, STDDEV(averagerating) stdrate,
MIN(numvotes) minvote, MAX(numvotes) maxvote, AVG(numvotes) avgvotes, STDDEV(numvotes) stdvote
FROM data2;
/*Derived from DATA, same statistics data*/

/*Table to analyze relation between production and year*/
CREATE TABLE yeartem AS
SELECT directors,
ROUND(CAST(AVG(startyear) AS NUMERIC),0) startyearavg,
MIN(startyear) startyearmin,
MAX(startyear) startyearmax,
MAX(averagerating) toprate,
SUM(numvotes) numvotes
FROM data2
GROUP BY directors;
SELECT * FROM yeartem;

/*Toprated film year*/
DROP TABLE yearanalysis;
CREATE TABLE yearanalysis AS
SELECT *
FROM
	/*Merge name.birthyear and to yeartem*/
((
	SELECT
		directors tempnamea1,
		birthyear
	FROM
		name
		RIGHT JOIN yeartem
		ON yeartem.directors = name.nconst)a
INNER JOIN
 	/*Find the toprated film's startyear(s) and # of toprated film*/
(
	SELECT
		*
	FROM
		yeartem
	LEFT JOIN(
		SELECT
			directors tempnameb1,
			MIN(startyear) topyearmin,
			MAX(startyear) topyearmax,
			ROUND(CAST(AVG(startyear) AS NUMERIC),0)  topyearavg,
			COUNT(tconst)
		FROM
			(SELECT
				directors,
				averagerating,
				startyear,
				tconst
			 FROM data2
			 INNER JOIN(
				SELECT
					directors tempnameb2, toprate
				FROM
					yeartem
			)maxrate
			ON maxrate.toprate = data2.averagerating
			AND maxrate.tempnameb2 = data2.directors
			ORDER BY data2.directors)temptable
			GROUP BY directors
			ORDER BY directors)temptable
	ON temptable.tempnameb1 = yeartem.directors)b
ON b.directors = a.tempnamea1)
ORDER BY directors;

ALTER TABLE yearanalysis
DROP COLUMN tempnamea1,
DROP COLUMN tempnameb1;

DROP TABLE IF EXISTS
	yeartem;



/*export for further analysis*/
COPY data_total TO '/Users/xintongli/PycharmProjects/Project/Film Data Analysis/SQL_exp.csv' DELIMITER ',' csv HEADER;

DROP TABLE data_exp;
CREATE TABLE data_exp AS
SELECT directors,
COUNT(tconst) numberall,
ROUND(CAST(AVG(startyear) AS NUMERIC),0) startyear,
ROUND(CAST(MAX(startyear) AS NUMERIC),0) startyearmax,
ROUND(CAST(MIN(startyear) AS NUMERIC),0) startyearmin,
ROUND(CAST(AVG(runtimeminutes) AS NUMERIC),1) runtimeminutes,
ROUND(CAST(AVG(averagerating) AS NUMERIC), 2) averagerating,
SUM(numvotes) numvotes
FROM data2
GROUP BY directors;
SELECT * FROM data_exp;

