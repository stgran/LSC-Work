WITH accepted_parties AS (
    SELECT case_key,
        IF (
            CARDINALITY (
                FILTER (
                    ARRAY_AGG(party_label), r -> r = 'entity'
                )
            ) > 0, 'commercial', 'non-commercial') AS eviction_type,
        IF (
            CARDINALITY (
                FILTER (
                    ARRAY_AGG(party_label_score), r -> r < .9
                )
            ) > 0, 'uncertain', 'certain') AS eviction_type_certainty,
        ARRAY_AGG(party_name ORDER BY ordinality_in_party_type) AS defendants,
        ARRAY_AGG(party_label ORDER BY ordinality_in_party_type) AS party_labels,
        ARRAY_AGG(party_label_score ORDER BY ordinality_in_party_type) AS party_label_scores
    FROM {{ ref('party_information') }} -- Table name changed
    WHERE party_type = 'defendant'
    GROUP BY case_key
),
raw AS (
    -- These are evictions with default judgments closed in 2022
    -- We only keep evictions for which we are certain all parties are people, not business entities
    SELECT a.*, b.eviction_type, b.eviction_type_certainty, b.defendants, b.party_labels, b.party_label_scores
    FROM {{ ref('standardized_data') }} a INNER JOIN accepted_parties b ON a.case_key = b.case_key -- Table name changed
    WHERE a.state = 'state_name' AND a.clean_case_type = 'eviction' -- State name removed for privacy
    AND YEAR(a.date_disposed) = 2022 AND a.disposition = 'default judgment'
    AND b.eviction_type = 'non-commercial' AND b.eviction_type_certainty = 'certain'
),
raw_2 AS (
    -- We are specifically interested in cases in which a Default Judgment was rendered during the first hearing
    SELECT a.*,
        TRY_CAST(
            REPLACE(REPLACE(b.judgment_information.attorney_fees, '$', ''), ',', '')
        AS DOUBLE) AS attorney_fees
    FROM raw a LEFT JOIN {{ ref('unstandardized_data') }} b ON a.case_key = b.case_key -- Table name
    WHERE ELEMENT_AT(b.hearing_information, -1).result = 'Default Judgment'
)
SELECT DISTINCT * FROM raw_2
WHERE attorney_fees > 0