-- Data cleaning
-- SQL portfolio Project  - Data Cleaning
-- https://www.kaggle.com/datasets/swaptr/layoffs-2022


SELECT *
FROM layoffs;


-- now when i'm doing data cleaning i usually follow a few steps
-- 1. check for duplicates and remove any duplicate
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways

-- note, -- first thing I want to do is create a staging table. This is the one i will work on and clean the data. i want a table with the raw data in case something happens

CREATE TABLE layoffs_staging 
LIKE layoffs;

SELECT *
FROM layoffs_staging ;
-- I wii insert information to my table

INSERT  layoffs_staging
SELECT *
FROM layoffs;


-- 1. Remove Duplicates

# First let's check for duplicates

Select *
from layoffs_staging;

-- we are going to use row nuber over partition by to get the duplicates

Select *,
Row_number() OVER(
partition by company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
from layoffs_staging;


-- creat CTE 

WITH duplicate_cte AS
(
Select *,
Row_number() OVER(
partition by company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
from layoffs_staging
)
SELECT*
FROM duplicate_cte
WHERE row_num > 1;

-- these are the ones we want to delete where the row number is > 1 or 2 or greater essentially
--- create table to delete row number greater > 1
-- one solution, which I think is a good one. Is to create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column
-- so let's do it!!

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

Select *
from layoffs_staging2;

--- lets insert in to information to the table created layoffs_staging2

INSERT INTO layoffs_staging2
Select *,
Row_number() OVER(
partition by company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
from layoffs_staging;

--- we can filter where the row is > 1
Select *
from layoffs_staging2
where row_num > 1;

--- delete
DELETE 
from layoffs_staging2
where row_num > 1;

--- TO CHECK MAYBE THE DUPLICATE HAS DELETED

Select *
from layoffs_staging2;

-- Standardize Data is finding issues in ur data and then fix it


SELECT company,TRIM(company)
from layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT*
from layoffs_staging2
where industry like 'Crypto%';
-- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto

UPDATE layoffs_staging2
SET  industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- now that's taken care of:
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

-- i also need to look at 
select *
from layoffs_staging2;

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- now if we run this again it is fixed
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;

-- Let's also fix the date columns: becos the date column is in text i need to convert it to Str( time series)
SELECT *
FROM layoffs_staging2;

-- we can use str to date to update this field
SELECT `date`,
STR_TO_DATE (`date`, '%m/%d/%Y')
FROM layoffs_staging2;
--- lets update the date

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE (`date`, '%m/%d/%Y');
-- now we can convert the data type properly `date` column
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- so there isn't anything I want to change with the null values


-- 4. remove any columns and rows we need to


SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;





















