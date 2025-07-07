SELECT 
    r.j ->> 'review_id'          AS review_identifier,
    r.j ->> 'date'               AS review_date,
    r.j ->> 'business_id'        AS biz_id,
    r.j ->> 'text'               AS review_text,
    r.j ->> 'user_id'            AS reviewer_id,
    r.j ->> 'stars'              AS rating,
    u.j ->> 'review_count'       AS total_reviews_by_user,
    u.j ->> 'elite'              AS elite_status,
    u.j ->> 'friends'            AS friend_list,
    u.j ->> 'name'               AS reviewer_name,
    u.j ->> 'fans'               AS number_of_fans
FROM public.review AS r
JOIN public.business AS b 
    ON r.j ->> 'business_id' = b.j ->> 'business_id'
JOIN public.users AS u 
    ON r.j ->> 'user_id' = u.j ->> 'user_id'
WHERE 
    b.j ->> 'city' = 'New Orleans'
    AND (b.j ->> 'is_open')::int = 1
    AND b.j ->> 'categories' ILIKE '%Restaurant%';
