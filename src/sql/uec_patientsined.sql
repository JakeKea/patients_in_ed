CREATE TABLE dummy_name (
    date_activity DATE NOT NULL,
    hour INT NOT NULL,

    fin_year CHAR(5) NOT NULL,
    fin_month INT NOT NULL,
    month_name CHAR(3) NOT NULL,
    date_weekstarting DATE NOT NULL,
    date_weekending DATE NOT NULL,

    site_code NVARCHAR(5) NOT NULL,
    shorthand NVARCHAR(4) NOT NULL,
    department_type_desc VARCHAR(500) NOT NULL,
    department_type_id NVARCHAR(2),

    count_patients INT NOT NULL,
    count_arrivals INT NOT NULL

    --Set Primary Key to prevent duplication
    CONSTRAINT PK_patientsined 
        PRIMARY KEY (
            date_activity, hour, site_code, department_type_id)
);