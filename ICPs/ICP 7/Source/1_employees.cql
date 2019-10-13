USE icp7;

drop table if exists employees;

CREATE TABLE employees (
	employee_id int,
	department text,
	last_name text,
	years_with_company int,
	hire_date date,
	job_title text,
	salary int,
	manager_id int,
	PRIMARY KEY(employee_id)
);

COPY employees (employee_id, department, last_name, years_with_company,hire_date,job_title,salary,manager_id) FROM 'employee_entries.csv' WITH HEADER=True AND DELIMITER='|';

CREATE INDEX job_index ON employees (job_title);
CREATE INDEX hire_date_index ON employees (hire_date);
CREATE INDEX salary_index ON employees (salary);

select * from employees;