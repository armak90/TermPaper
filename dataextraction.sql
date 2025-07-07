WITH cleaned AS (
  SELECT DISTINCT
    c.j->>'business_id' AS checkin_business_id,
    DATE(unnest(string_to_array(c.j->>'date', ','))) AS checkin_date,

    b.j->>'name' AS business_name,
    b.j->>'address' AS business_address,
    b.j->>'city' AS business_city,
    b.j->>'state' AS business_state,
    b.j->>'postal_code' AS postal_code,
    (b.j->>'latitude')::float AS business_lat,
    (b.j->>'longitude')::float AS business_long,
    (b.j->>'stars')::float AS star_rating,
    (b.j->>'review_count')::int AS total_reviews,
    b.j->>'categories' AS categories,
    b.j->'attributes' AS business_attributes,

    (b.j->'attributes' ->> 'HasTV')::text ILIKE 'true' AS has_tv,
    (b.j->'attributes' ->> 'Caters')::text ILIKE 'true' AS caters,

    regexp_replace(
      b.j->'attributes' ->> 'Alcohol',
      '^u''|''$',
      '',
      'g'
    ) AS alcohol,

    CASE 
      WHEN lower(
        regexp_replace(
          b.j->'attributes' ->> 'Alcohol',
          '^u''|''$',
          '',
          'g'
        )
      ) = 'none' OR b.j->'attributes' ->> 'Alcohol' IS NULL THEN 0
      ELSE 1
    END AS alcohol_flag,

    (b.j->'attributes' ->> 'DriveThru')::text ILIKE 'true' AS drive_thru,
    (b.j->'attributes' ->> 'GoodForKids')::text ILIKE 'true' AS good_for_kids,
    (b.j->'attributes' ->> 'RestaurantsTakeOut')::text ILIKE 'true' AS take_out,
    (b.j->'attributes' ->> 'RestaurantsDelivery')::text ILIKE 'true' AS delivery,
    (b.j->'attributes' ->> 'BusinessAcceptsCreditCards')::text ILIKE 'true' AS accepts_credit_cards,

    -- Clean Ambience string and cast to jsonb
    regexp_replace(
      regexp_replace(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              b.j->'attributes' ->> 'Ambience',
              'u''', '"', 'g'
            ),
            '''', '"', 'g'
          ),
          'False', 'false', 'g'
        ),
        'True', 'true', 'g'
      ),
      'None', 'null', 'g'
    )::jsonb AS ambience_json,

    -- Clean BusinessParking string and cast to jsonb
    regexp_replace(
      regexp_replace(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              b.j->'attributes' ->> 'BusinessParking',
              'u''', '"', 'g'
            ),
            '''', '"', 'g'
          ),
          'False', 'false', 'g'
        ),
        'True', 'true', 'g'
      ),
      'None', 'null', 'g'
    )::jsonb AS business_parking_json

  FROM public.checkin AS c
  INNER JOIN public.business AS b
    ON c.j->>'business_id' = b.j->>'business_id'
  WHERE b.j->>'city' = 'New Orleans'
    AND (b.j->>'is_open')::int = 1
    AND b.j->>'categories' ILIKE '%Restaurant%'
)

SELECT
  checkin_business_id,
  checkin_date,
  business_name,
  business_address,
  business_city,
  business_state,
  postal_code,
  business_lat,
  business_long,
  star_rating,
  total_reviews,
  categories,
  
  has_tv,
  caters,
  alcohol_flag,
  drive_thru,
  good_for_kids,
  take_out,
  delivery,
  accepts_credit_cards,

  COALESCE(ambience_json ->> 'romantic', 'false') = 'true' AS romantic,
  COALESCE(ambience_json ->> 'intimate', 'false') = 'true' AS intimate,
  COALESCE(ambience_json ->> 'classy', 'false') = 'true' AS classy,
  COALESCE(ambience_json ->> 'hipster', 'false') = 'true' AS hipster,
  COALESCE(ambience_json ->> 'divey', 'false') = 'true' AS divey,
  COALESCE(ambience_json ->> 'touristy', 'false') = 'true' AS touristy,
  COALESCE(ambience_json ->> 'trendy', 'false') = 'true' AS trendy,
  COALESCE(ambience_json ->> 'upscale', 'false') = 'true' AS upscale,
  COALESCE(ambience_json ->> 'casual', 'false') = 'true' AS casual,

  -- Flatten BusinessParking attributes
  COALESCE(business_parking_json ->> 'garage', 'false') = 'true' AS parking_garage,
  COALESCE(business_parking_json ->> 'street', 'false') = 'true' AS parking_street,
  COALESCE(business_parking_json ->> 'validated', 'false') = 'true' AS parking_validated,
  COALESCE(business_parking_json ->> 'lot', 'false') = 'true' AS parking_lot,
  COALESCE(business_parking_json ->> 'valet', 'false') = 'true' AS parking_valet

FROM cleaned
WHERE checkin_date >= DATE '2019-01-01' AND checkin_date < DATE '2021-01-01';
