-- starship.sql

-- 10 torpedos carried by every ship
-- 10 types of ships
-- 10 admiral has 1 from each ship_type

drop table if exists admirals;
drop table if exists ship_types;
drop table if exists ships;
drop table if exists torpedos;

create table ships (id integer,ship_type_id integer,crew_size integer);
create table ship_types (id integer,type_name string);
insert into ship_types values
    (1,'galaxy class'),
    (2,'nebula class'),
    (3,'orion class'),
    (4,'first class'),
    (5,'last pass'),
    (6,'last pass'),
    (7,'akira class'),
    (8,'aeon type'),
    (9,'antares type'),
    (10,'apollo class')
    ;

create table admirals as
    select id from ship_types;

create table torpedos (id integer,ship_id integer,admiral_id integer);

insert into ships
select row_number() over (),t.id,row_number() over (partition by t.id) from ship_types t join ship_types t2;

insert into torpedos
select row_number() over (),s.id,row_number() over (partition by s.id) from ships s join ship_types t2;

-- grouplens

drop table if exists u_data;
drop view if exists ok_movies;

CREATE TABLE u_data (
  userid INT,
  movieid INT,
  rating INT,
  unixtime STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH 'examples/files/u.data'
OVERWRITE INTO TABLE u_data;

create view ok_movies as
select * from u_data
where rating >= 2 and rating <=4;

--
-- Materialized views
--

drop table if exists emps;
drop table if exists depts;

-- These are needed for transactional tables to work

SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.support.concurrency=true;
SET hive.enforce.bucketing=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.compactor.initiator.on=true;
SET hive.compactor.worker.threads=1;

CREATE TABLE emps (
  empid INT,
  deptno INT,
  name VARCHAR(256),
  salary FLOAT,
  hire_date TIMESTAMP)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');
 
CREATE TABLE depts (
  deptno INT,
  deptname VARCHAR(256),
  locationid INT)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

insert into emps values
    (101, 201, 'Alice Johnson', 55000.0, '2022-04-15'),
    (102, 203, 'Bob Smith', 60000.0, '2023-01-22'),
    (103, 205, 'Claire Brown', 50000.0, '2022-09-10'),
    (104, 204, 'David Wilson', 65000.0, '2023-03-05'),
    (105, 202, 'Emily Davis', 58000.0, '2022-11-30'),
    (106, 201, 'Frank Miller', 52000.0, '2023-06-18'),
    (107, 205, 'Grace Thompson', 62000.0, '2022-08-07'),
    (108, 203, 'Henry Garcia', 59000.0, '2023-04-25'),
    (109, 202, 'Isabel Martinez', 61000.0, '2022-02-14'),
    (110, 204, 'Jackie Clark', 63000.0, '2023-09-01'),
    (111, 201, 'Kevin Adams', 54000.0, '2022-05-20'),
    (112, 205, 'Lily Hall', 58000.0, '2023-10-12'),
    (113, 203, 'Mike Lee', 56000.0, '2022-07-03'),
    (114, 204, 'Nancy Baker', 60000.0, '2023-02-28'),
    (115, 202, 'Olivia White', 57000.0, '2022-12-17'),
    (116, 201, 'Peter Young', 54000.0, '2023-08-14'),
    (117, 203, 'Quinn Taylor', 59000.0, '2023-06-22'),
    (118, 205, 'Rachel Martinez', 61000.0, '2022-03-18'),
    (119, 202, 'Samuel Brown', 63000.0, '2023-01-05'),
    (120, 204, 'Tina Johnson', 55000.0, '2022-09-30'),
    (121, 201, 'Ulysses Clark', 57000.0, '2023-11-25'),
    (122, 205, 'Victoria White', 54000.0, '2022-07-12'),
    (123, 203, 'Wendy Garcia', 58000.0, '2023-04-02'),
    (124, 204, 'Xavier Lee', 59000.0, '2022-12-19'),
    (125, 202, 'Yvonne Adams', 60000.0, '2023-08-28')
    ;

insert into depts values
    (201, 'Marketing', 501),
    (202, 'Engineering', 502),
    (203, 'Finance', 503),
    (204, 'Human Resources', 504),
    (205, 'Sales', 505)
    ;

CREATE MATERIALIZED VIEW mv1
AS
SELECT empid, deptname, hire_date
FROM emps JOIN depts
  ON (emps.deptno = depts.deptno)
WHERE hire_date >= '2023-01-01';
