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
 
UPDATE name SET
	age = name.deathyear - name.birthyear;
UPDATE name SET
	age = (SELECT EXTRACT(YEAR FROM CURRENT_DATE)) - name.birthyear
WHERE birthyear IS NOT NULL AND deathyear IS NULL;
DELETE FROM name
WHERE age>110 AND deathyear IS NULL;
DELETE FROM name
WHERE birthyear IS NULL;


/*There is in name data a change after yearanalysis*/
COPY name TO '/Users/xintongli/PycharmProjects/Project/Film Data Analysis/SQL_name.csv' DELIMITER ',' csv HEADER;

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

DROP TABLE data2;
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
/*ALTER TABLE data2
DROP COLUMN index;*/

COPY data2 TO '/Users/xintongli/PycharmProjects/Project/Film Data Analysis/SQL_data.csv' DELIMITER ',' csv HEADER;


/*Check reliability of data*/
SELECT MIN(startyear) minyear, MAX(startyear) maxyear, AVG(startyear) avgyear, STDDEV(startyear) stdyear,
MIN(runtimeminutes) minruntime, MAX(runtimeminutes) maxruntime, AVG(runtimeminutes) avgruntime, STDDEV(runtimeminutes) stdruntime,
MIN(averagerating) minrate, MAX(averagerating) maxrate, AVG(averagerating) avgrate, STDDEV(averagerating) stdrate,
MIN(numvotes) minvote, MAX(numvotes) maxvote, AVG(numvotes) avgvotes, STDDEV(numvotes) stdvote
FROM data2;
/*Derived from DATA, same statistics data*/

select * from data2;

/*Table to analyze relation between production and year*/
DROP TABLE yeartem;
CREATE TABLE yeartem AS
SELECT directors,
ROUND(CAST(AVG(startyear) AS NUMERIC),0) startyearavg,
MIN(startyear) startyearmin,
MAX(startyear) startyearmax,
MAX(averagerating) toprate,
SUM(numvotes) numvotes,
COUNT(tconst) numberall
FROM
	/*Filter out the films release after the the director past away*/
	(SELECT * FROM
	(SELECT a.directors, a.startyear, a.averagerating, a.numvotes, a.tconst,
	 b.age, b.birthyear,
	 a.startyear-b.birthyear tempyear
	 FROM data2 a
	 INNER JOIN(
		 SELECT nconst, age, birthyear
		 FROM name
	 )b
	 on a.directors = b.nconst) c
	 WHERE c.tempyear<c.age
	 ORDER BY directors
	)d
GROUP BY directors
ORDER BY directors;

/*Toprated film year*/
DROP TABLE yearanalysis;
CREATE TABLE yearanalysis AS
SELECT *
FROM(	(/*Merge name.birthyear and to yeartem*/
		SELECT
			yeartem.*,
			name.birthyear,
			name.age
		FROM
			name
			RIGHT JOIN yeartem
			ON yeartem.directors = name.nconst
		)a
	INNER JOIN(/*The third join, find the year related info grouped by directors*/
		SELECT
			directors tempnameb1,
			MIN(startyear) topyearmin,
			MAX(startyear) topyearmax,
			ROUND(CAST(AVG(startyear) AS NUMERIC),0)  topyearavg,
			COUNT(tconst)
		FROM(/*Secondly, find which year/film has the top rate:
				by join with the table 'maxrate'--
				contains multiple year with same top rate*/
			SELECT
				directors,
				averagerating,
				startyear,
				tconst
			 FROM data2
			 INNER JOIN(
				/*Firstly, call the toprate from yeartem*/
				SELECT
					directors tempnameb2, toprate
				FROM
					yeartem)maxrate
			ON maxrate.toprate = data2.averagerating
			AND maxrate.tempnameb2 = data2.directors
			ORDER BY data2.directors)temptable
		GROUP BY directors
		ORDER BY directors)b
	ON b.tempnameb1 = a.directors)
ORDER BY directors;

SELECT * FROM yearanalysis;

ALTER TABLE yearanalysis
DROP COLUMN tempnameb1;

ALTER TABLE yearanalysis
ADD COLUMN startyearlen int,
ADD COLUMN topyearlen int,
ADD COLUMN topagemin int,
ADD COLUMN topagemax int;

UPDATE yearanalysis
SET
	startyearlen = startyearmax - startyearmin,
	topyearlen = topyearmax - topyearmin,
	topagemin = topyearmin - birthyear,
	topagemax = topyearmax - birthyear;

/*Check reliability of data*/
SELECT MIN(topagemin) minagemin, MAX(topagemax) maxagemax
FROM yearanalysis;
	 
DELETE FROM name
WHERE nconst =
	(SELECT directors FROM yearanalysis
	 WHERE topagemin<0
	);
COPY name TO '/Users/xintongli/PycharmProjects/Project/Film Data Analysis/SQL_name.csv' DELIMITER ',' csv HEADER;

/*export for further analysis*/
DROP TABLE summary;
CREATE TABLE summary AS
SELECT directors,
COUNT(tconst) numberall,
ROUND(CAST(AVG(runtimeminutes) AS NUMERIC),1) runtimeminutes,
ROUND(CAST(AVG(averagerating) AS NUMERIC), 2) averagerating,
SUM(numvotes) numvotes
FROM data2
GROUP BY directors;
SELECT * FROM summary;

DELETE FROM summary
WHERE directors =
	(SELECT directors FROM yearanalysis
	 WHERE topagemin<0
	);

DROP TABLE IF EXISTS
	yeartem;

COPY yearanalysis TO '/Users/xintongli/PycharmProjects/Project/Film Data Analysis/SQL_exp.csv' DELIMITER ',' csv HEADER;
COPY summary TO '/Users/xintongli/PycharmProjects/Project/Film Data Analysis/SQL_summary.csv' DELIMITER ',' csv HEADER;

