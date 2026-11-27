CREATE OR REPLACE FUNCTION {}(value text)
RETURNS integer
LANGUAGE sql
IMMUTABLE STRICT PARALLEL SAFE
AS $function$
    SELECT CASE
        WHEN digits = '' THEN 0
        WHEN length(digits) < 10 THEN digits::integer
        WHEN length(digits) = 10 THEN
            CASE WHEN digits::bigint <= 2147483647 THEN digits::integer END
    END
    FROM (SELECT ltrim(substring(value FROM '^(?:ID_)?([0-9]+)$'), '0') AS digits) AS key;
$function$;

CREATE OR REPLACE FUNCTION {}(value text, expected text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE STRICT PARALLEL SAFE
AS $function$
    SELECT lower(value) = lower(expected);
$function$;
