SELECT DISTINCT
  refresh_date,
  rank,
  term
FROM `bigquery-public-data.google_trends.international_top_rising_terms`
WHERE country_name = 'Japan'
  AND refresh_date BETWEEN DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 DAY)
                       AND CURRENT_DATE('Asia/Tokyo')
ORDER BY refresh_date DESC, rank ASC
LIMIT 10
