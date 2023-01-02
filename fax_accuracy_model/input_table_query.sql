WITH fax_counts AS (
SELECT fax
    , COUNT(DISTINCT lkr_id) AS locations
    , COUNT(DISTINCT npi) AS npis
  FROM precompute_faxes
 GROUP BY 1
), location_counts AS (
SELECT lkr_id
    , COUNT(DISTINCT fax) AS faxes
  FROM precompute_faxes pf
 GROUP BY 1
), data AS (
SELECT pf.*
    , ARRAY_LENGTH(pf.fax_sources, 1) AS source_count
    , fc.locations 					          AS num_locations_with_this_fax
    , fc.npis 						            AS num_npis_with_this_fax
    , lc.faxes 						            AS num_fax_for_this_location
    , CASE WHEN cpn.record->'provider_types' ? 'Doctor' THEN 1 ELSE 0 END AS is_doc
  FROM precompute_faxes pf
  LEFT JOIN fax_counts fc
    ON fc.fax = pf.fax
  LEFT JOIN location_counts lc
    ON lc.lkr_id = pf.lkr_id
  LEFT JOIN compiled_providers_new cpn
    ON cpn.npi = pf.npi::bigint
 WHERE pf.lkr_id IS NOT NULL
   AND pf.fax IS NOT NULL
)

SELECT *
  FROM data
