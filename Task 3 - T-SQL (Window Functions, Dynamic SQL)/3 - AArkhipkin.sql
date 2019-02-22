/*****************************     1   *********************************************/
select distinct g.good_name
		--actual year
		, (select isnull(sum(amount), 0)
				from [dbo].[sales] s 
				where s.id_good = g.id 
					and datepart(year, s.s_date) = datepart(year, getdate()) 
					and cast(s.s_date as date) <= cast( getdate() as date)
			) as YTD 
			
		, (select isnull(sum(amount), 0)
				from [dbo].[sales] s 
				where s.id_good = g.id 
					and datepart(year, s.s_date) = datepart(year, getdate()) 
					and datepart(month, s.s_date) = datepart(month, getdate()) 
					and cast(s.s_date as date) <= cast( getdate() as date)
			) as MTD 
			
		, (select isnull(sum(amount), 0)
				from [dbo].[sales] s 
				where s.id_good = g.id 
					and datepart(year, s.s_date) = datepart(year, getdate()) 
					and datepart(quarter, s.s_date) = datepart(quarter, getdate()) 
					and cast(s.s_date as date) <= cast( getdate() as date)
			) as QTD 
		
		--previous year
		, (select isnull(sum(amount), 0)
				from [dbo].[sales] s 
				where s.id_good = g.id 
					and datepart(year, s.s_date) = (datepart(year, getdate())-1)
					and cast(s.s_date as date) <= cast( dateadd(year, -1, getdate()) as date)
			) as PYTD  
			
		, (select isnull(sum(amount), 0)
				from [dbo].[sales] s 
				where s.id_good = g.id 
					and datepart(year, s.s_date) = datepart(year, dateadd(year, -1, getdate())) 
					and datepart(month, s.s_date) = datepart(month, getdate()) 
					and cast(s.s_date as date) <= cast( dateadd(year, -1, getdate()) as date)
			) as PMTD 
			
		, (select isnull(sum(amount), 0)
				from [dbo].[sales] s 
				where s.id_good = g.id 
					and datepart(year, s.s_date) = datepart(year, dateadd(year, -1, getdate())) 
					and datepart(quarter, s.s_date) = datepart(quarter, getdate()) 
					and cast(s.s_date as date) <= cast( dateadd(year, -1, getdate()) as date)
			) as PQTD 
	from [dbo].ref_goods g


/*****************************     2   *********************************************/
select 
		tmp.WeekNum, tmp.id, g.good_name, gg.good_group_name, tmp.s_date, tmp.amount, tmp.rate
	from
		(
		select 
				case
					when cast(d.s_date as date) between '2013-12-01' and '2013-12-08' then 1
					when cast(d.s_date as date) between '2013-12-09' and '2013-12-15' then 2
					when cast(d.s_date as date) between '2013-12-16' and '2013-12-22' then 3
					when cast(d.s_date as date) between '2013-12-23' and '2013-12-31' then 4
				end as WeekNum,
				* 
				, ROW_NUMBER() OVER(PARTITION BY 
										case
											when cast(d.s_date as date) between '2013-12-01' and '2013-12-08' then 1
											when cast(d.s_date as date) between '2013-12-09' and '2013-12-15' then 2
											when cast(d.s_date as date) between '2013-12-16' and '2013-12-22' then 3
											when cast(d.s_date as date) between '2013-12-23' and '2013-12-31' then 4
										end ,
										d.id_good
								
									ORDER BY cast(d.s_date as date) desc, rate, d.s_date desc
									) rnk
			from [dbo].[docs] d
			where 
				datepart(year, d.s_date) = 2013
				and datepart(month, d.s_date) = 12

		) tmp
			join [dbo].[ref_goods] g on tmp.id_good = g.id
			join [dbo].[ref_good_groups] gg on gg.id = g.id_good_group
		where rnk=1


/*****************************    3    *********************************************/
declare @begin date = '2016-12-31'
declare @end date = '2017-01-03'

DECLARE @sqlText nvarchar(max)

DECLARE @ColumnName AS NVARCHAR(MAX)
SELECT @ColumnName = ISNULL(@ColumnName + ',','') 
       + QUOTENAME(dat)
	FROM (select 
				distinct cast(cast(s.s_date as date) as nvarchar) dat
			from [dbo].[sales] s
			where cast(s.s_date as date) between @begin and @end
			--order by 1
		) AS dates
--select (@ColumnName)

DECLARE @ColumnName_isnull AS NVARCHAR(MAX)
SELECT @ColumnName_isnull = ISNULL(@ColumnName_isnull + ', ','') 
       + 'isnull(' + QUOTENAME(dat) + ',0) as ' + QUOTENAME(dat)
	FROM (select 
				distinct cast(cast(s.s_date as date) as nvarchar) dat
			from [dbo].[sales] s
			where cast(s.s_date as date) between @begin and @end
			--order by 1
		) AS dates
--select (@ColumnName_isnull)

set @sqlText = '
select good_name, ' + @ColumnName_isnull + '--*
	from
		(
		select 
				g.good_name, cast(s.s_date as date) dat, sum(s.amount) sum_amount
			from [dbo].[sales] s
				join [dbo].[ref_goods] g on s.id_good = g.id
			where cast(s.s_date as date) between ''' + cast(@begin as nvarchar) + ''' and ''' + cast(@end as nvarchar) + '''
			group by g.good_name, cast(s.s_date as date)
		) t
	pivot
		(
		sum(sum_amount) for dat in (' + @ColumnName + ')
		) piv
'

--select (@sqlText)
Exec (@sqlText)

