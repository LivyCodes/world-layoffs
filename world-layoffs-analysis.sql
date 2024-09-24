use world_layoffs;

-- data cleaning
select * from layoffs;

-- creating a new table layoffs_staging similar to layoffs but, we'll be working on, i.e. data cleaning, the new table
create table layoffs_staging
like layoffs;

select * from layoffs_staging;

insert into layoffs_staging
select * from layoffs;