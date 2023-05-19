{{ config(schema = 'Inventory') }}

WITH date_range AS (
  SELECT
    DATE '2000-01-01' + INTERVAL '1 day' * generate_series(0, 36524) AS date
)

SELECT
  TO_CHAR(DATE, 'yyyy-mm-dd') AS Calendar_date,
  EXTRACT('year' FROM date) AS year,
  EXTRACT('quarter' FROM date) AS quarter,
  EXTRACT('month' FROM date) AS month,
  EXTRACT('week' FROM date) AS week,
  EXTRACT('day' FROM date) AS day,
  EXTRACT('dow' FROM date) AS day_of_week,
  EXTRACT('doy' FROM date) AS day_of_year,
  CASE WHEN EXTRACT('isodow' FROM date) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
  CASE WHEN EXTRACT('isodow' FROM date) IN (1, 2, 3, 4, 5) THEN TRUE ELSE FALSE END AS is_weekday
FROM date_range

