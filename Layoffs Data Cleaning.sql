select * from layoffs

--1- remove duplicates
--2- standardize the data
--3- null values 
--4- remove any useless columns
----------------------------------------------



-- create a copy of the table to work with



-- remove duplicates 

select * into layoffs_staging from layoffs;

select * from layoffs_staging;




/* -- sub query to delete duplicates rows --
select *,
ROW_NUMBER()over(partition by company, location,industry,
total_laid_off, percentage_laid_off, date,stage, country, funds_raised_millions order by company ) as row_num
from layoffs_staging;
*/

select *,
ROW_NUMBER()over(partition by company, location,industry,
total_laid_off, percentage_laid_off, date,stage, country, funds_raised_millions order by company ) as row_num
from layoffs_staging

with duplicate_cte as (

select *,
ROW_NUMBER()over(partition by company, location,industry,
total_laid_off, percentage_laid_off, date,stage, country, funds_raised_millions order by company ) as row_num
from layoffs_staging

)
select *  
from duplicate_cte
where row_num > 1;





-- standardizing data 

-- delete unnecessary spaces by TRIM --

select company, LTRIM(company)
from layoffs_staging;

update layoffs_staging
set company=LTRIM(company);


-- delete repetation -- #1 industry #2 country
/*
select distinct industry 
from layoffs_staging
order by 1;
*/

select * 
from layoffs_staging
where industry like 'Crypto%';

update layoffs_staging
set industry = 'Crypto'
where industry Like 'Crypto%';


select distinct industry 
from layoffs_staging;

------

select distinct country
from layoffs_staging
order by 1;

update layoffs_staging
set country = 'United States'
where country Like 'United States%';



-------convert date datatype & format-----------
/*update ls
set ls.date = l.date
from layoffs_staging ls
join layoffs as l
on ls.company=l.company*/

alter table layoffs_staging
add new_date date;

select date,try_convert(date, date)
from layoffs_staging

update layoffs_staging
set date = try_convert(date, date)

alter table layoffs_staging
alter column date date;




-- null values


select * from layoffs_staging


-- industry's null values --

select * from layoffs_staging
where industry = 'null'
or industry = '';

update layoffs_staging
set industry = null
where industry = '' or industry = 'null' ;


select * from layoffs_staging
where company = 'airbnb';



select t1.industry,t2.industry 
from layoffs_staging t1
join layoffs_staging t2
	on t1.company=t2.company
	and t1.location=t2.location
where (t1.industry is null or t1.industry='')
and t2.industry is not null ;

update t1
set t1.industry=t2.industry
from layoffs_staging t1
join layoffs_staging t2
	on t1.company=t2.company
	and t1.location=t2.location
where t1.industry is null
and t2.industry is not null;


-- laid_off null values --

select * from layoffs_staging
where total_laid_off = 'null' 
and percentage_laid_off = 'null';


delete from layoffs_staging
where total_laid_off = 'null' 
and percentage_laid_off = 'null';


-- remove column -- 
select * from layoffs_staging;

alter table layoffs_staging
drop column new_date
