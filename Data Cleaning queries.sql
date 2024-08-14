-- 1. Remove Duplicates
-- 2. Standardize the data
-- 3. NULL values or blank columns
-- 4. Remove any columns 

-- We wil perform operations and data cleaning on a new table
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

-- 1. REMOVE DUPLICATES
SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, `date`, percentage_laid_off, date, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, `date`, percentage_laid_off, date, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)

SELECT * 
FROM duplicate_cte
WHERE row_num > 1;
/* This query gave is the duplicate values which are there in the table. We need to delete these values
We will create a new table because we cannot delete from a CTE */

/* Copied the create statement of 'layoffs_staging' tot create a new table */
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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, `date`, percentage_laid_off, date, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Duplicates Removed

-- 2. Standardizing Data

SELECT company, TRIM(company)
FROM layoffs_staging2;

/* Removed extra spaces */
UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

/* Crypto, Crypto Currency and CryptoCurrency are one and the same thing so we will update it with a single name */

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT location
FROM layoffs_staging2; /* Everything is fine here */

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

/* Updating Date column from 'text' to 'Date' */

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')	
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. Handling NULL and blank columns

SELECT *
FROM layoffs_staging2
WHERE industry is NULL
OR industry LIKE '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

/* We will populate the blank columns */

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
AND t1.location = t2.location
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = null
WHERE industry = '';
 
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

/* Handling NULL and empty values is done */

-- 4. Deleting row_num

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Data Cleaning is Done! 

