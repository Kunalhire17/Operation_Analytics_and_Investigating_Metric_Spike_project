CREATE DATABASE project3;
use project3;
CREATE TABLE project3 (
    ds DATE,
    job_id INT,
    actor_id INT,
    event VARCHAR(20),
    language VARCHAR(20),
    time_spent INT,
    org CHAR(1)
);

show tables;

INSERT INTO project3 (ds, job_id, actor_id, event, language, time_spent, org)
VALUES
    ('2020-11-30', 21, 1001, 'skip', 'English', 15, 'A'),
    ('2020-11-30', 22, 1006, 'transfer', 'Arabic', 25, 'B'),
    ('2020-11-29', 23, 1003, 'decision', 'Persian', 20, 'C'),
    ('2020-11-28', 23, 1005, 'transfer', 'Persian', 22, 'D'),
    ('2020-11-28', 25, 1002, 'decision', 'Hindi', 11, 'B'),
    ('2020-11-27', 11, 1007, 'decision', 'French', 104, 'D'),
    ('2020-11-26', 23, 1004, 'skip', 'Persian', 56, 'A'),
    ('2020-11-25', 20, 1003, 'transfer', 'Italian', 45, 'C');

ALTER TABLE project3 RENAME TO job_data;

select * from job_data;

-- task 1 - job reviwed over time
SELECT
    DATE(ds) AS review_date,
    HOUR(ds) AS review_hour,
    COUNT(*) AS jobs_reviewed
FROM job_data
WHERE ds >= '2020-11-01' AND ds < '2020-12-01'
GROUP BY review_date, review_hour
ORDER BY review_date, review_hour;

-- task 2 - throghput analysis
SELECT
    ds,
    AVG(events_per_second) OVER (ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg
FROM (
    SELECT
        ds,
        COUNT(*) / 86400.0 AS events_per_second -- 86400 seconds in a day
    FROM job_data
    GROUP BY ds
) daily_events
ORDER BY ds;

-- task 3- lanaguge share analysis =
SELECT
    language,
    (COUNT(*) * 100.0) / total_events_in_last_30_days AS percentage_share
FROM job_data
JOIN (
    SELECT
        COUNT(*) AS total_events_in_last_30_days
    FROM job_data
    WHERE ds >= '2020-11-01' AND ds < '2020-12-01'
) last_30_days_events ON 1 = 1
GROUP BY language, total_events_in_last_30_days
ORDER BY percentage_share DESC;

-- task 4- Duplicate Rows Detection
SELECT
    ds,
    job_id,
    actor_id,
    event,
    language,
    time_spent,
    org,
    COUNT(*) AS duplicate_count
FROM job_data
GROUP BY ds, job_id, actor_id, event, language, time_spent, org
HAVING COUNT(*) > 1;

-- CASE STUDY 2


use project3;
create table users (
user_id int,
created_at varchar(100),
company_id int,
language varchar(100),
activated_at varchar(100),
state varchar(50));

SHOW VARIABLES LIKE 'secure_file_priv';

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv'
INTO TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

select * from users;

ALTER TABLE users add column temp_created_at datetime;
SET SQL_SAFE_UPDATES = 0;

UPDATE users SET temp_created_at = STR_TO_DATE(created_at, '%d-%m-%Y %H.%i');


alter table users drop column created_at;

alter table users change column temp_created_at created_at datetime;

SET SQL_SAFE_UPDATES = 1;
select * from users;

-- table2 event

create table event (
user_id int,
occurred_at varchar(100),
event_type varchar(50),
event_name varchar(100),
location varchar(50),
device varchar(50),
user_type int
);

SHOW VARIABLES LIKE 'secure_file_priv';

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv'
INTO TABLE event
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

desc event;

select * from event;

ALTER TABLE event add column temp_occurred_at datetime;
SET SQL_SAFE_UPDATES = 0;

UPDATE event SET temp_occurred_at= STR_TO_DATE(occurred_at, '%d-%m-%Y %H.%i');


alter table event drop column occurred_at;

alter table event change column temp_occurred_at occurred_at datetime;

SET SQL_SAFE_UPDATES = 1;
select * from event
