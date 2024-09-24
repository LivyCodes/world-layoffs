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

-- checking for null values and managing them
select * from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;
 
  -- cannot manage blanks or null values in total_laid_off column and percentage_laid_off column due to unsufficient data
  -- checking for null values in the industry column
select *
from layoffs_staging2
where industry=' '
or industry is null;

select * from layoffs_staging2
where company like 'Bally%';

  -- checking for similar companies and locations for industries with null values or blank spaces in order to populate with the similar ones
select *
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
and t1.location = t2.location
where (t1.industry = ' ' or t1.industry is null) and t2.industry is not null;

  -- updating the industries with blanks to null so as to be able to populate
update layoffs_staging2
set industry = null
where industry =' ';

  -- populating null values in industry column
update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
and t1.location = t2.location
set t1.industry = t2.industry
where t1.industry is null and t2.industry is not null;


-- Removing unnecessary records and columns 
  -- Removing records where total_laid_off is null and percentage_laid_off is null;
select * from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

delete from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

  -- Dropping the row_num column that we created
alter table layoffs_staging2
drop row_num;

select * from layoffs_staging2;