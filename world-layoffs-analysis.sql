use world_layoffs;

-- data cleaning
select * from layoffs;

-- creating a new table layoffs_staging similar to layoffs but, we'll be working on, i.e. data cleaning, the new table
create table layoffs_staging
like layoffs;

select * from layoffs_staging;

insert into layoffs_staging
select * from layoffs;

-- checking for the duplicates in layoffs_staging
select *,
row_number() over(
partition by company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions
) as row_num
from layoffs_staging;

with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions
) as row_num
from layoffs_staging
)
select * from duplicate_cte
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

select * from layoffs_staging2;

insert into layoffs_staging2
select *,
row_number() over(
partition by company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions
) as row_num
from layoffs_staging;

select * from layoffs_staging2
where row_num > 1;

set sql_safe_updates=0;

delete from layoffs_staging2
where row_num > 1;

-- Standardization of the data
-- removing white spacesfrom company column using trim()
update layoffs_staging2
set company = trim(company);

-- setting industry to be same for the 'crypto' industry since it has many variations
select distinct industry
from layoffs_staging2;

select distinct industry
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

-- standardizing the country column using trim for a trailing period(.) on one of the records
select distinct country
from layoffs_staging2
order by 1;

select distinct country
from layoffs_staging2
where country like 'United States%';

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

-- standarding the `date` column by changing it from string format to date format
select `date`
from layoffs_staging2;

  -- changing to date format
update layoffs_staging2
set date = str_to_date(`date`, '%m/%d/%Y');

  -- updating the data type for `date` column
alter table layoffs_staging2
modify column `date` date;

select * from layoffs_staging2;
