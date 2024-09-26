use world_layoffs;

-- DATA CLEANING
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

-- EXPLORATORY DATA ANALYSIS(EDA)
	-- layoffs(total_laid_off) and their average_percentage_laid_off by industry, ordered by total layoffs by industry in descending order 
    select industry, sum(total_laid_off) as total_layoffs, round(avg(percentage_laid_off),3) as avg_percentage_layoffs
    from layoffs_staging2
    group by industry
    order by 2 desc; -- top two industries are consumer at 45182 total layoffs and 0.265 average percent layoffs followed by retail at 43613 and 0.269 and others follow
					 -- bottom two being Manufacturing at the bottom at 20 layoffs and 0.05 average percent layoffs and 2nd last being fintech at 215 layoffs and 0.243 average percent layoffs
    
		-- analysing the layoffs based on average percentage to better understand the size of the industries in relation to the average percentages and total layoffs
    with avg_industry_percentage_layoffs as
    (select industry, sum(total_laid_off) as total_layoffs, round(avg(percentage_laid_off),3) as avg_percentage_layoffs
    from layoffs_staging2
    group by industry
    order by 2 desc
    )
    select ROW_NUMBER() OVER (ORDER BY avg_percentage_layoffs asc) AS row_index, industry, avg_percentage_layoffs, total_layoffs
    from avg_industry_percentage_layoffs
    group by industry
    order by 3 desc; -- aerospace industry had the highest average percent layoffs 0.565 and 661 total layoffs suggesting that the aerospace industry may not be very populated
					 -- the second higest average percent layoffs being education at 0.357 and 13338 total layoffs
                     -- the least average percent layoffs was Manufacturing industry at 0.05 and 20 total layoffs
		
        -- industry with maximum number of layoffs
    select * from layoffs_staging2
    where  total_laid_off=(
		select max(total_laid_off)
        from layoffs_staging2
	); 	-- returns google with 12000 total_lay_offs and its corresponding percentage 0.06
		
		-- analysing companies in consumer since the consumer industry ranks top in terms of total_laid_off and ranks 13th in terms of average percentage layoffs 
        -- while google, which is a company in consumer industry, ranked top in terms of layoffs at 12000 with 0.06 percentage_laid_off
    select * from layoffs_staging2
    where industry = 'consumer'
    order by 4 desc; -- returns google top followed by meta at 11000 layoffs and a 0.13 percentage_laid_off then twitter at a 3500 layoffs and 0.5 percentage_laid_off
					 -- number and other smaller companies with small number of layoffs and high percentages follow hence explaining the rank of the consumer industry  
                     -- as 1st in terms of total layoffs and 13th in terms of avg_percentage at 0.265 given the topbrands have significantly low percentages
                     
    -- top 3 layoffs in each industry grouped by company and their stages
    with ranked_companies as
    (
    select industry, company, stage, total_laid_off,
    row_number() over (partition by industry order by total_laid_off desc) as `rank`
    from layoffs_staging2
    )
    select *
    from ranked_companies
    where `rank` <= 3
    order by industry, `rank`;
    
    -- layoffs by stages
			-- querying from the top 3 ranked companies in each industry which stage had the most layoffs
   with ranked_companies as
    (
    select industry, company, stage, total_laid_off,
    row_number() over (partition by industry order by total_laid_off desc) as `rank`
    from layoffs_staging2
    ),
    top_3_companies as
    (
    select *
    from ranked_companies
    where `rank` <= 3
    order by industry, `rank`
    )
    select stage, count(*)
    from top_3_companies
    group by stage
    order by 2 desc;  -- Post-IPO had the most layoffs out of the top 3 ranked companies in each industry, followed by Unknown then Acquired
    
			-- overall check of the stage that registered the most layoffs
    select count(distinct stage) from layoffs_staging2;
    
    select stage, count(*)
    from layoffs_staging2
    group by stage
    order by 2 desc;  -- Generally Post-IPO stage still had the highest layoffs followed by an Unknown stage then Series B, Series C, Series D and so on
    
    
    -- layoffs by year
    select substring(`date`,1,4) as `year`, sum(total_laid_off)
    from layoffs_staging2
    where `date` is not null
    group by `year` 
    order by 1 asc;	-- 2022 had the most layoffs, followed by 2023, then 2020 and 2021 had the least layoffs with a significantly low number compared to the rest of the years
    
    -- layoffs by month
    with rolling_total as
    (
    select substring(`date`, 1,7) as `month`, sum(total_laid_off) as total_layoffs
    from layoffs_staging2
    where `date` is not null
    group by `month`
    order by 1 asc
    )
    select `month`, sum(total_layoffs), sum(total_layoffs) over(order by `month`) as cumulative_total
    from rolling_total
    group by `month`;	-- 383159 people had been laid of from the 2020-03 till 2023-03 ie a span of 36 months
    
			-- months with most and least layoffs
	with monthly_totals as
    (
    select substring(`date`, 1,7) as `month`, sum(total_laid_off) as total_layoffs
    from layoffs_staging2
    where `date` is not null
    group by `month`
    order by 1 asc
    )
    select `month`, total_layoffs
    from monthly_totals
    where total_layoffs =
    (select max(total_layoffs)
    from monthly_totals)
    or
    total_layoffs =
    (select min(total_layoffs)
    from monthly_totals)
    ;		-- 2023-01 had the most layoffs at 84714 layoffs while 2021-10 had the least at 22 layoffs
    
    -- layoffs by country
    select country, sum(total_laid_off) as total_layoffs, count(distinct company) as companies_count, count(distinct industry) as industries_count
    from layoffs_staging2
    group by country
    order by 2 desc;  -- United States had the most layoffs at 256559 layoffs with 1055 companies laying people off across 30 industries
    
    
    select * from layoffs_staging2;