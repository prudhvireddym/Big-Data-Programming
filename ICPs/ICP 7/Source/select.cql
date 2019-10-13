USE icp7;

//1.List the empID,ename,jobtitle,and hiredate of employee from the employee table.

select employee_id,last_name,job_title,hire_date from employees;

//2.List the name,salary of the employees who are clerks.

select last_name,salary from employees where job_title = 'CLERK';

//3.List the name,job,salary of every employee joined on ‘february18,2000’,
select last_name,job_title,salary from employees where hire_date = '2000-02-18';

//4.List name and annual salary of all the employees. - no airthamatic support for cassandra
select last_name, salary from employees;

//5.Display employees’ names, salary and manager values of those employees whose salary is 45000 from EMP table using SELECT statement.
select last_name,salary,manager_id from employees where salary = 45000;

// Bonus - Write a query that displays last names with first letter capitalized and all other letters in lowercase and length of lastnames whose lastnames start with J, G or S.
SELECT SUBSTRING(last_name, 1, 8) AS name FROM employees;