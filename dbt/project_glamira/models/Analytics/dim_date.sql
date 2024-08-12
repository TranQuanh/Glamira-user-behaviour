
SELECT  DISTINCT TIMESTAMP_SECONDS(time_stamp) AS time_stamp,
        EXTRACT(HOUR FROM TIMESTAMP_SECONDS(time_stamp)) AS hour,
        EXTRACT(MINUTE FROM TIMESTAMP_SECONDS(time_stamp)) AS minute,
        EXTRACT(SECOND FROM TIMESTAMP_SECONDS(time_stamp)) AS second,
        EXTRACT(DAY FROM TIMESTAMP_SECONDS(time_stamp)) AS day,
        EXTRACT(MONTH FROM TIMESTAMP_SECONDS(time_stamp)) AS month,
        EXTRACT(YEAR FROM TIMESTAMP_SECONDS(time_stamp)) AS year
FROM
  `project-glamira.Glamira.behaviour`