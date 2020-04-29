
/* loading users data */
usersRawData = LOAD '/user/cloudera/prac06/input/users.dat' USING PigStorage(':') AS (user:int, n1, gender:chararray, n2, age:int, n3, occupation:int, n4, zip:chararray);
/* clean users data */
usersData = FOREACH usersRawData GENERATE user, gender, age, occupation, zip;
/* loading ratings data */
ratesRawData = LOAD '/user/cloudera/prac06/input/ratings.dat' USING PigStorage(':') AS (user:int, p1, movieID:int, p2, rate:int, p3, timestamp:int, p4);
/* clean ratings data */
ratesData = FOREACH ratesRawData GENERATE user, movieID, rate, timestamp;
/* loading movie data */
moviesInfoRawData = LOAD '/user/cloudera/prac06/input/movies.dat' USING PigStorage(':') AS (movieID:int, m2, name:chararray, m3, genre:chararray, m4);
/* clean movie data */
moviesInfoData = FOREACH moviesInfoRawData GENERATE movieID, name;
/* join users.dat and ratings.data */
usersRatingJoinRawData = JOIN usersData by user, ratesData by user;
/* clean users.dat and ratings.data */
usersRatingJoinData = FOREACH usersRatingJoinRawData GENERATE usersData::age, ratesData::movieID;

/* group usersRatingJoinData by age and movieID */
clustersAgeMovieID = GROUP usersRatingJoinData by (usersData::age, ratesData::movieID);
/* count movieID */
clustersAgeCountMovieID = FOREACH clustersAgeMovieID GENERATE FLATTEN(group) as (usersData::age, ratesData::movieID), COUNT($1) as maxvalue;
/* group by age */
clustersGroupAge = GROUP clustersAgeCountMovieID BY usersData::age;

/* age and max count movieID list */
ageMaxMovieCountList = FOREACH clustersGroupAge GENERATE group,clustersAgeCountMovieID.$1, MAX(clustersAgeCountMovieID.$2) AS maxvalue;

/* create ageMovieidCount based on age movieID count movieID  */
ageMovieidCount = FOREACH clustersAgeCountMovieID GENERATE usersData::age, ratesData::movieID, maxvalue;
/* count list  */
movieIDCountList = FOREACH ageMaxMovieCountList GENERATE maxvalue;
/* join count age and movieID  */
ageMovieJoinMaxCount = JOIN ageMovieidCount by maxvalue, movieIDCountList by maxvalue;


/* removie duplicate elements */
ageMovieIDCountGroupAge = GROUP ageMovieJoinMaxCount BY $0;
finalAgeMaxMovieIDList = FOREACH ageMovieIDCountGroupAge {
    sortedList = ORDER ageMovieJoinMaxCount BY $3 DESC;
    top_record = LIMIT sortedList 1;
    GENERATE FLATTEN(top_record);
}

/* join with movie data */
ageMaxMovieIdJoinNameRawData = JOIN finalAgeMaxMovieIDList by $1, moviesInfoData by $0;
/* create final table with age movieID and movie name */
ageMaxMovieIDName = FOREACH ageMaxMovieIdJoinNameRawData GENERATE $0, $1, $5;

/* age , Movie , Name */
DUMP ageMaxMovieIDName;
STORE ageMaxMovieIDName INTO '/user/cloudera/prac06/pig_output' USING PigStorage('\t');




