UPDATE popes
SET
    reign_end = '2025-04-21',
    age_end =
        EXTRACT(year FROM AGE(reign_end, birth_date))
      + EXTRACT(month FROM AGE(reign_end, birth_date)) / 12.0
      + EXTRACT(day FROM AGE(reign_end, birth_date)) / 365.0,
    tenure =
        EXTRACT(year FROM AGE(reign_end, reign_start))
      + EXTRACT(month FROM AGE(reign_end, reign_start)) / 12.0
      + EXTRACT(day FROM AGE(reign_end, reign_start)) / 365.0
WHERE id = {pope_francis_id};
