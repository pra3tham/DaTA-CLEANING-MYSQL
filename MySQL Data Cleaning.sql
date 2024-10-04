/* Data Cleaning*/ 
SELECT * 
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardise the data
-- 3. Null  Values or Black values
-- 4. Remove unwanted or na columns

CREATE TABLE layoff_staging
LIKE layoffs;
SELECT *
FROM layoff_staging;
INSERT layoff_staging 
SELECT *
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off,`date`) as row_num
FROM layoff_staging;

with duplicate_cte as
(SELECT *,
ROW_NUMBER() OVER(PARTITION BY company,location, industry, total_laid_off, percentage_laid_off,`date`, stage, funds_raised_millions) as row_num
FROM layoff_staging
)
select *
from duplicate_cte
where row_num>1;

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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoff_staging2
where row_num > 1;

Insert into layoff_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company,location, industry, total_laid_off, percentage_laid_off,`date`, stage, funds_raised_millions) as row_num
FROM layoff_staging;

DELETE
FROM layoff_staging2
where row_num > 1;

-- Standardising the Data

select company, trim(company)
from layoff_staging2;

update layoff_staging2
set company = trim(company);

select distinct(industry)
from layoff_staging2
order by 1;

update layoff_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct(country)
from layoff_staging2
where country like 'United States%'
order by 1;

update layoff_staging2
set country = 'United States' /* or set country = trim(trailing '.' from country)*/
where country like 'United States%';

select `date`
from layoff_staging2;

update layoff_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoff_staging2
modify column `date` date;

select *
from layoff_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoff_staging2
WHERE industry is null
or industry='';

SELECT *
FROM layoff_staging2
WHERE company='Airbnb';

select t1.industry,t2.industry
from layoff_staging2 t1
join layoff_staging2 t2
	on t1.company=t2.company
    and t1.location=t2.location
where (t1.industry is null or t1.industry='')
and t2.industry is not null;

update layoff_staging2
set industry=null
where industry='';

update layoff_staging2 t1
join layoff_staging2 t2
	on t1.company=t2.company
    and t1.location=t2.location
set t1.industry=t2.industry
where (t1.industry is null or t1.industry='')
and t2.industry is not null;

select *
from layoff_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;
 
delete
from layoff_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

select *
from layoff_staging2;

alter table layoff_staging2
drop column row_num;
