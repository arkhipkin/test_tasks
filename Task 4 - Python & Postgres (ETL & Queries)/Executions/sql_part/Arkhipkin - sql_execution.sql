select
		*
	from occupations;
	
/*
Query an alphabetically ordered list of all names in OCCUPATIONS,
immediately followed by the first letter of each profession as a parenthetical (i.e.: enclosed in parentheses). 
For example: AnActorName(A), ADoctorName(D), AProfessorName(P), and ASingerName(S)	
*/

select
		CONCAT(o."name", '(', substring(coalesce(o.occupation, ''), 1, 1), ')') as "result"
	from occupations o
	order by 1;
	
/*
Query the number of ocurrences of each occupation in OCCUPATIONS. 
Sort the occurrences in ascending order, and output them in the following format:
> There are a total of [occupation_count] [occupation]s.
where [occupation_count] is the number of occurrences of an occupation in OCCUPATIONS 
and [occupation] is the lowercase occupation name. 
If more than one OCCUPATIONS has the same [occupation_count], they should be ordered alphabetically.
Note: There will be at least two entries in the table for each type of occupation.
*/

with occupations_cte as (
	select
			o.occupation, count(*) as cnt
		from occupations as o
		group by o.occupation
		order by 2, 1
	)
	select 
			concat('There are a total of ', cnt, ' ', lower(occupation), 's.')
		from occupations_cte

		
		
-- in case you need to get 2 results in one query
with names_cte as (
	select
			CONCAT(o."name", '(', substring(coalesce(o.occupation, ''), 1, 1), ')') as "result"
		from occupations o	
	)
	, occupations_cte as (
		select
				o.occupation, count(*) as cnt
			from occupations as o
			group by o.occupation
			--order by 2, 1
		)
	, occupations_string_cte as (
		select 
				concat('There are a total of ', cnt, ' ', lower(occupation), 's.') as "result"
				, occupation, cnt			
			from occupations_cte
		)
	, union_cte as(
		select
			*
			, 1 as lvl
			, ROW_NUMBER() over (ORDER BY "result") rnk
		from names_cte
		union all 
		select 
				"result"
				, 2 as lvl
				, ROW_NUMBER() over (ORDER BY cnt, occupation) rnk
			from occupations_string_cte
		)
	select 
			"result"
		from union_cte
		order by lvl, rnk
			
	