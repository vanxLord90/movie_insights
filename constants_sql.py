"""
This module stores sql statements that will do query and manipulate the db
"""
SQL_SELECT_UNIQUE_CERT = \
    "select distinct certificate from \"table_of_content\";"
SQL_SELECT_UNIQUE_GENRE =\
    "select distinct genre from \"table_of_content\";"
SQL_SELECT_UNIQUE_STARS =\
    "select distinct stars from \"table_of_content\" GROUP BY stars HAVING COUNT(stars) > 50;"
SQL_SELECT_VOTES = \
    "select votes from \"table_of_content\" where votes>100000"

#this command was done on pgadmin
SQL_TABLE_INSERT_COMMAND= \
"copy content_recommender_db from 'D:\python-da\cleaned_csv25.csv' DELIMITER ',' CSV"

#this command was done on pgadmin
SQL_COPY_BOX_OFFICE_DATA=\
     "INSERT INTO imdb_cleaned\
select * from \"content_recommender_db\"\
 WHERE position('$' in votes)=1;"

SQL_SELECT_BAD_CERTIFICATE =\
"select certificate from \"content_recommender_db\" \
where certificate = \'Approved\' OR certificate = \'Passed\' OR \
certificate = \'(Banned)\'"

#this command was done on pgadmin
SQL_DELETE_BAD_CERTIFICATE =\
"delete from \"content_recommender_db\" \
where (certificate = 'Passed') OR \
(certificate ='Approved') OR \
(certificate ='(Banned)')"

SQL_SELECT_NULL_DIRECTOR= \
"select director from \"table_of_content\"\
where director = 'NULL'"

# this command was done on pgadmin
SQL_REPLACE_NULL_DIRECTOR=\
"update \"table_of_content\"\
set director = REPLACE(director, 'NULL', 'multiple')\
WHERE director LIKE 'NULL';"

SELECT_NULL_EMPTY_MOVIES = \
"select movie from \"table_of_content\"\
where movie = 'NULL' or movie = ''"


SELECT_NULL_EMPTY_GENRES = \
"select genre from \"table_of_content\"\
where genre = 'NULL'"

SELECT_EMPTY_GENRES = \
"select genre from \"table_of_content\"\
where genre = '' "

SELECT_MUSIC_GENRE =\
"select genre from \"table_of_content\"\
where genre ='Music'"

#query done on pgadmin
DELETE_MUSIC_GENRE =\
"delete from \"table_of_content\"\
where genre = 'Music'"

SELECT_SHORT_GENRE =\
"select genre from \"table_of_content\"\
where genre ='Short'"

SELECT_ALL_BASED_ON_STARS_OVR_30_MOVIES_GENRE_SHORT= \
"select * from table_of_content \
where stars in \
(select stars from table_of_content\
 GROUP BY stars HAVING COUNT(stars)>30)\
    AND genre ='Short'"

SELECT_ALL_BASED_ON_STARS_OVR_30_MOVIES= \
"select * from table_of_content \
where stars in \
(select stars from table_of_content\
 GROUP BY stars HAVING COUNT(stars)>30)"

# query done on pgadmin
DELETE_ALL_BASED_ON_STARS_OVR_30_MOVIES= \
"delete from table_of_content \
where stars in \
(select stars from table_of_content\
 GROUP BY stars HAVING COUNT(stars)>30)\
 and genre = 'Short'"

# query done on pgadmin
SQL_COPY_NULL_STARS_DATA=\
     "INSERT INTO null_actors_table\
 select * from \"table_of_content\"\
 WHERE stars = 'NULL';"

# query done on pgadmin
DELETE_NULL_STARS= \
"delete from table_of_content WHERE stars = 'NULL'"

SQL_SELECT_ALL_FROM_NULL_ACTORS_TABLE =\
    "select * from null_actors_table"

SQL_SELECT_ALL_UNTITLED_MOVIES =\
"select * from table_of_content \
where stars in \
(select stars from table_of_content\
 GROUP BY stars HAVING COUNT(stars)<30)\
 and runtime =0 and genre ='NULL' and movie like 'Untitled %'"

DELETE_ALL_UNTITLED_MOVIES =\
"delete from table_of_content where movie "

SELECT_ALL_UPCOMING_MOVIES= \
"select * from table_of_content \
 where runtime =0 and certificate ='NULL' and rating = 'NULL' and votes =0"
# query done on pgadmin
DELETE_ALL_UPCOMING_MOVIES= \
"delete from table_of_content \
 where runtime =0 and certificate ='NULL' and rating = 'NULL' and votes =0"

# query done on pgadmin
REPLACE_ALL_NULL_RATINGS_0= \
"update table_of_content \
 set rating = REPLACE(rating, 'NULL', '0')\
 where rating like 'NULL'"


ALTER_RATING_TYPE = \
 "ALTER TABLE table_of_content ALTER COLUMN rating TYPE float USING rating::float;"

# done in pgadmin
SQL_COMMAND_TO_GET_LOW_VOTES_AND_LOW_RATING =\
"""SELECT * from table_of_content
where stars in 
(select stars from table_of_content
 GROUP BY stars HAVING COUNT(stars)<2)
 and certificate ='NULL' and genre ='NULL' AND rating <8"""

DELETE_COMMAND_TO_RID_LOW_VOTES_AND_LOW_RATING = \
"""
DELETE from table_of_content
where stars in 
(select stars from table_of_content
 GROUP BY stars HAVING COUNT(stars)<2)
 and certificate ='NULL' and genre ='NULL' AND rating <8
"""
"""
SELECT_NULL_COLUMN_VALUES= \
"select * from \"table_of_content\" \
    where movie ='NULL' or \
    genre = 

OR certificate = \'Passed\' OR \
certificate = \'(Banned)\'
"""

#i used a kaggle data set to recommend content, tv and movies. I cleaned data, am processing data , then i am going to make a PowerBI dashboard with it.
#
"""
create table table_of_content_VD as
select 
movies_base.*, 
case when runtime=0 and genre='NULL' and certificate='NULL' and rating=0 then 1 else 0 end as exclusion_cri_1,
case when runtime<60 then 1 else 0 end as runtime_lt60min_ind,
case when runtime<60 then 'Less than an hour'
when runtime>=60 and runtime<120 then ' 1 hour to 2 hours'
else ' 2 hrs+' end as duration_bin,
stars_cnt.stars_cnt,
case when stars_cnt.stars_cnt >=10 then 'Celebrity' else 'Unknown' end as Celeb_Bin
from table_of_content as movies_base

left join 	( 
	select stars,
	count(stars) as stars_cnt 
	from table_of_content 
	group by 1
			) as stars_cnt on (movies_base.stars=stars_cnt.stars)

-- Validate the columns I just created to make sure they work and pass the sniff test 

select 
duration_bin,
count(runtime) as movie_freq

from (
select 
*, 
case when runtime<60 then 1 else 0 end as runtime_lt60min_ind,
case when runtime<60 then 'Less than an hour'
when runtime>=60 and runtime<120 then ' 1 hour to 2 hours'
else ' 2 hrs+' end as duration_bin
from table_of_content 
	) movies
	group by 1
	
select 
case when runtime<60 then 'Less than an hour'
when runtime>=60 and runtime<120 then ' 1 hour to 2 hours'
else ' 2 hrs+' end as duration_bin,
genre,
count(genre) as genre_distro
from table_of_content 
group by 1,2
order by 1,2


select director, avg(rating) as average_rating
from table_of_content_vd
group by distinct director
 
-- If you see a actors name 10 or mores times then the actor is a star 
-- Observed that runtime =0and stars =0 and someother stuff means the movie as not come out yet 



select movies_tbcd.*, 
case when rating =0 then 'Not Yet Rated'
when rating >0 and rating<3 then 'Bad'
when rating>3 and rating<6 then 'Average'
when rating>6 and rating<8 then 'Good'
else 'Great' end as movie_text_rating
from table_of_content_vd as movies_tbcd


create table table_of_content_vd_movie_rating as
select movies_tbcd.*, 
case when rating =0 then 'Not Yet Reviewed'
when rating >0 and rating<3 then 'Bad'
when rating>3 and rating<6 then 'Average'
when rating>6 and rating<8 then 'Good'
else 'Great' end as movie_text_rating
from table_of_content_vd as movies_tbcd


-- left join(
select director,
count(director)
from table_of_content_vd
group by director
-- ) as avg_dir_rating on (movies_tbcd.director= avg_dir_rating.director)


UPDATE table_of_content_vd_movie_rating AS t
set director_rating = subqueryt.avg_rating
from(
	select director, avg(rating) as avg_rating
	from table_of_content_vd_movie_rating
	group by director
) as subqueryt
where t.director= subqueryt.director


update table_of_content_vd_movie_rating 
set director_rating_text = (

case when director_rating = 0 then 'Unrated'
when director_rating >0 and director_rating<3 then 'Bad'
when director_rating>3 and director_rating<6 then 'Average '
when director_rating>6 and director_rating<8 then 'Good'
else 'Great' end)

delete from table_of_content_vd_movie_rating
where movie_text_rating ='Not Yet Reviewed' and director_rating_text = 'Unrated' and votes =0 and genre = 'NULL'


delete from table_of_content_vd_movie_rating
where movie_text_rating ='Not Yet Reviewed' and director_rating_text = 'Unrated' and votes =0 and runtime=0


SELECT genre, COUNT(genre) AS number_each_genre,
       DENSE_RANK() OVER (ORDER BY COUNT(genre) DESC) AS genre_descending_rank
FROM table_of_content_vd_movie_rating
GROUP BY genre


WITH RankedGenres AS (
    SELECT genre, DENSE_RANK() OVER (ORDER BY COUNT(genre) DESC) AS genre_descending_rank
    FROM table_of_content_vd_movie_rating
    GROUP BY genre
)
UPDATE table_of_content_vd_movie_rating AS t
SET genre_descending_rank = rg.genre_descending_rank
FROM RankedGenres AS rg
WHERE t.genre = rg.genre;
"""