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

SELECT * FROM name;
SELECT * FROM data;

CREATE TABLE data_exp AS
SELECT COUNT(tconst) number, SUM(startyear) startyear, SUM(runtimeminutes) runtimeminutes,
directors, SUM(averagerating) averagerating, SUM(numvotes) numvotes
FROM data2
GROUP BY directors;
SELECT * FROM data_exp;

SELECT startyear/