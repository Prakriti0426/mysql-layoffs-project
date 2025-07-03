-- Data Cleaning project




select * from layoffs;

-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens
create table layoff_staging
like layoffs;

insert layoff_staging 
select * from layoffs;

-- now When we are data cleaning we usually follow a few steps
-- 1. remove duplicates if any
-- 2. standarized the data and fix errors
-- 3. look at null values or blank values
-- 4. remove any columns and rows that are not necesary 




-- 1. Remove duplicates
# first let's check for duplicates
 
select * from layoff_staging;

select *,
row_number() over(partition by company, industry, total_laid_off, percentage_laid_off, 'date') as row_num
from layoff_staging;

SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		layoff_staging
) duplicates
WHERE 
	row_num > 1;

-- lets just look at oda to confirm 
select * from layoff_staging 
where company = 'Oda';

-- these are our real duplicates
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions 
			) AS row_num
	FROM 
		layoff_staging
) duplicates
WHERE 
	row_num > 1;
    
-- these are the ones we want to delete where the row number is > 1 or 2or greater essentially
-- now you may want to write it like this:
with delete_cte as (
	SELECT *
FROM (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions 
			) AS row_num
	FROM 
		layoff_staging
) duplicates
WHERE 
	row_num > 1
)
delete from delete_cte;


CREATE TABLE `layoff_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
   row_num int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoff_staging2;
insert into layoff_staging2
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoff_staging;
select * from layoff_staging2
where row_num > 1;

delete from layoff_staging2
where row_num > 1;









-- 2.Standarizing data

select company, trim(company) from layoff_staging2;

update layoff_staging2
set company = trim(company);

-- I noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypt
select * from layoff_staging2
where industry like 'Crypto%';

update layoff_staging2
set industry = 'Crypto'
where industry like 'Crypto%';
 
 -- now that's taken care of :
select distinct industry from layoff_staging2;

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
select distinct country, trim(trailing '.' from country)
from layoff_staging2
;

update layoff_staging2
set country = trim(trailing '.' from country)
where industry like 'United States%';


-- Let's also fix the date columns:
-- we can use 'str to date' to update this field
select `date`,
STR_TO_DATE (`date`, '%m/%d/%Y')
from layoff_staging2;

update layoff_staging2
set `date` = STR_TO_DATE (`date`, '%m/%d/%Y');

-- now we can convert the data type properly
alter table layoff_staging2
modify column `date` date;

select * from layoff_staging2;


-- if we look at industry it looks like we have some null and empty rows, let's take a look at these
SELECT DISTINCT industry
FROM layoff_staging2
ORDER BY industry;

SELECT *
FROM layoff_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- let's take a look at these
SELECT *
FROM layoff_staging2
WHERE company LIKE 'Bally%';
-- nothing wrong here
SELECT *
FROM layoff_staging2
WHERE company LIKE 'airbnb%';

-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-- we should set the blanks to nulls since those are typically easier to work with
UPDATE layoff_staging2
SET industry = NULL
WHERE industry = '';

-- now if we check those are all null

SELECT *
FROM layoff_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- now we need to populate those nulls if possible

UPDATE layoff_staging2 t1
JOIN layoff_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- and if we check it looks like Bally's was the only one without a populated row to populate this null values
SELECT *
FROM layoff_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;






-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- so there isn't anything I want to change with the null values



-- 4. remove any columns and rows we need to

SELECT *
FROM layoff_staging2
WHERE total_laid_off IS NULL;


SELECT *
FROM layoff_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM layoff_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoff_staging2;

ALTER TABLE layoff_staging2
DROP COLUMN row_num;

SELECT * 
FROM layoff_staging2;



