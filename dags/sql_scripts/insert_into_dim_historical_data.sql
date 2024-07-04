INSERT INTO real_estate.dim_historical_data (webpage)
SELECT ft.webpage
FROM real_estate.fact_table ft
LEFT JOIN real_estate.all_current_webpages acw ON ft.webpage = acw.current_webpage
WHERE acw.current_webpage IS NULL and ft.german_state_id = 6
ON CONFLICT (webpage) DO NOTHING;