-- SQL Project - Data Cleaning


SELECT *
FROM world_layoffs.layoffs;

-- 1. first thing is to create a staging table. This is the table to use to clean the data. Well keeping the raw one safe.

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- 2. Checking and Removing Duplicates

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,total_laid_off,percentage_laid_off,`date`) AS row_name
FROM layoffs_staging;

WITH duplicute_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry, total_laid_off,percentage_laid_off,`date`, stage,country,
funds_raised_millions ) AS row_name
FROM layoffs_staging
) 
SELECT *
FROM duplicute_cte
WHERE row_name >1;

SELECT * 
FROM world_layoffs.layoffs_staging
where company = 'Cazoo'
;


-- I created another table that I will use to delete all the duplicutes without changing anying on the original dataset

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
  `row_name` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry, total_laid_off,percentage_laid_off,`date`, stage,country,
funds_raised_millions ) AS row_name
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_name >1;

-- 3. Standardazing Data

SELECT company, trim(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = trim(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';


SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country, TRIM( TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country =  TRIM( TRAILING '.' FROM country)
WHERE country LIKE 'United states%';


SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

ALTER TABLE  layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 4. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- so there isn't anything I want to change with the null values


-- 5. remove any columns and rows 

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry ='';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company ='Airbnb';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL )
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL )
AND t2.industry IS NOT NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2 
DROP COLUMN row_name;


-- https://www.kaggle.com/datasets/swaptr/layoffs-2022









