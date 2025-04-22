CREATE SEQUENCE popes_id_seq;

CREATE TABLE popes AS
SELECT
    NEXTVAL('popes_id_seq') AS id,
    number,
    name_full,
    name,
    suffix,
    canonization,
    TRY_CAST(birth AS DATE) AS birth_date,
    TRY_CAST(start AS DATE) AS reign_start,
    TRY_CAST("end" AS DATE) AS reign_end,
    age_start,
    age_end,
    tenure
FROM read_csv('{csv_path}',
header=TRUE,
auto_detect=TRUE,
nullstr=['NA']);

ALTER TABLE popes ADD CONSTRAINT popes_pk PRIMARY KEY (id);
