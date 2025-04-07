# DATA CLEANING PROJECT

select*
from layoffs;

#1. Remove duplicates
#2. Standardise the Data
#3. Null values or Blank Values
#4. Remove Any Columns


create table layoffs_staging
like layoffs;

select*
from layoffs_staging;

insert layoffs_staging
select*
from layoffs;

select*,
row_number() over(
partition by company, industry,total_laid_off, percentage_laid_off, 'date') as row_num
from layoffs_staging;

## Creating CTEs
with duplicates_cte as
(
select*,
row_number() over(
partition by company, location, industry,total_laid_off, percentage_laid_off, 'date',  stage,country, funds_raised_millions) as row_num
from layoffs_staging
)
select*
from duplicates_cte
where row_num > 1;

select*
from layoffs_staging
where company = 'cazoo';

##deleting duplicates
with duplicates_cte as
(
select*,
row_number() over(
partition by company, location, industry,total_laid_off, percentage_laid_off, 'date',  stage,country, funds_raised_millions) as row_num
from layoffs_staging
)
delete
from duplicates_cte
where row_num > 1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select*
from layoffs_staging2;

insert into layoffs_staging2
select*,
row_number() over(
partition by company,location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions)
from layoffs_staging;

select*
from layoffs_staging2
where row_num>1;

delete
from layoffs_staging2
where row_num>1;

select*
from layoffs_staging2
;



## Standardizing data

select company,trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select*
from layoffs_staging2
where industry like 'crypto%'
;

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct industry
from layoffs_staging2;

### Country
select distinct country
from layoffs_staging2
order by 1
;

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = 'United States'
where country like 'United States%'
;
### OR

update layoffs_staging2
set country= trim(trailing '.' from country)
where country like 'United States%'
;

select*
from layoffs_staging2
order by 1
;

### Date
select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

select `date`
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y')
;

alter table layoffs_staging2
modify column `date` date;

select*
from layoffs_staging2;

### Eliminating Null Values

select*
from layoffs_staging2
where total_laid_off is Null
and percentage_laid_off is null;

update layoffs_staging2
set industry = null
where industry = '';

select *
from layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2
where company = 'Airbnb';

select t1.industry,t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null
;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;

select *
from layoffs_staging2
where company like "bally's Interactive";


select*
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;

select*
from layoffs_staging2
;



# Exploratory Data Analysis


select*
from layoffs_staging2
where industry = 'food'and country = 'united states' and funds_raised_millions >=1000 and location = 'philadelphia'
;

select MAx(total_laid_off), Min(total_laid_off),max(percentage_laid_off), min(percentage_laid_off)
from layoffs_staging2
;

select*
from layoffs_staging2
where percentage_laid_off =1
order by funds_raised_millions desc
;

select company,sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select min(date), max(date)
from layoffs_staging2;

select industry,sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

select  YEAR(`date`), sum(total_laid_off)
from layoffs_staging2
group by Year(`date`)
order by 1 desc;

select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;

select company, avg(percentage_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select substring(`date`,1,7) as 'Month', sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by Month
order by Month;


with rolling_total as
(
select substring(`date`,1,7) as 'Month', sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by Month
order by 1 asc
)
select Month,  total_off,
sum(total_off) over(order by Month) as rolling_total
from rolling_total
;

select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
order by company asc;

select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
order by 3 desc;

with Company_Year(Company, Years,Total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
), Company_Year_Rank as
(
select*, 
dense_rank() over(partition by years order by total_laid_off desc) as Ranking
from Company_Year
where years is not null
)
select*
from Company_Year_Rank
where Ranking <= 5
;