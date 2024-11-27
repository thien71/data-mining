USE nuclear_outages;

CREATE TABLE IF NOT EXISTS national_outages (
    id INT AUTO_INCREMENT PRIMARY KEY,
    period DATE NOT NULL,
    capacity DECIMAL(10, 2),
    outage DECIMAL(10, 2),
    percent_outage DECIMAL(5, 2),
    capacity_units VARCHAR(50),
    outage_units VARCHAR(50),
    percent_outage_units VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS facility_outages (
    id INT AUTO_INCREMENT PRIMARY KEY,
    period DATE NOT NULL,
    facility_id INT,
    facility_name VARCHAR(255),
    capacity DECIMAL(10, 2),
    outage DECIMAL(10, 2),
    percent_outage DECIMAL(5, 2),
    capacity_units VARCHAR(50),
    outage_units VARCHAR(50),
    percent_outage_units VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS generator_outages (
    id INT AUTO_INCREMENT PRIMARY KEY,
    period DATE NOT NULL,
    facility_id INT,
    facility_name VARCHAR(255),
    generator_id INT,
    percent_outage DECIMAL(5, 2),
    percent_outage_units VARCHAR(50)
);
