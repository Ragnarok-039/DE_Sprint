/* В работе использую ClickHouse, синтаксис может немного отличаться от лекций, но смысл тот же. */


create table users
(
    uniq_id       Int32,
    full_name     String,
    birth_date    Date,
    first_day     Date,
    position      String,
    experience    String,
    salary        Int32,
    department_id Int32,
    rules         Bool
)
    engine = MergeTree()
        order by uniq_id;


create table department
(
    department_id   Int32,
    department_name String,
    full_name       String,
    count_users     Int32
)
    engine = MergeTree()
        order by department_id;


create table premium
(
    uniq_id       Int32,
    estimate_1_qt String,
    estimate_2_qt String,
    estimate_3_qt String,
    estimate_4_qt String
)
    engine = MergeTree()
        order by uniq_id;


insert into users
values (1, 'Иванов Иван Иванович', '01.01.1990', '01.01.2010', 'developer', 'Senior', 100500, 1, True),
       (1, 'Иванова Ольга Петровна', '01.01.1995', '01.01.2015', 'developer', 'Middle', 50500, 1, True),
       (1, 'Петров Петр Петрович', '02.01.1990', '01.05.2011', 'developer', 'Senior', 100500, 1, True),
       (1, 'Медведев Димон Иванович', '01.01.1980', '01.01.2005', 'administration', 'Junior', 100500500, 2, True),
       (1, 'Рогозин Космос Владимирович', '01.01.1980', '01.01.2005', 'administration', 'Junior', 100500500, 2, True);

insert into department
values (3, 'data_mining', 'Греф Герман Семенович', 3);


select uniq_id, full_name, experience
from users;


select uniq_id, full_name, experience
from users
order by uniq_id
limit 3;


select uniq_id
from users
         left join department on users.department_id = department.department_id
where department.department_name = 'Водитель';


select full_name
from users
         left join premium on users.uniq_id = premium.uniq_id
where premium.estimate_1_qt in ('D', 'E')
   or premium.estimate_2_qt in ('D', 'E')
   or premium.estimate_3_qt in ('D', 'E')
   or premium.estimate_4_qt in ('D', 'E');


select max(salary)
from users;
