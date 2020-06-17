CREATE TABLE ny_precinct_votes (
    `election_id` VARCHAR(256),
    `precinct` VARCHAR(256),
    `party` VARCHAR(256),
    `candidate` VARCHAR(256),
    `absentee` INT,
    `election_day` INT,
    `votes` INT,
    `assembly district` INT,
    `election district` INT,
    `machine` INT,
    `affidavit` INT,
    `federal` INT,
    `absentee_affidavit` INT,
    `absentee_hc` INT,
    `military` INT,
    `uocava` INT,
    `assembly_district` INT,
    `public_counter_votes` INT,
    `emergency_votes` INT,
    `absentee_military_votes` INT,
    `federal_votes` INT,
    `affidavit_votes` INT,
    `machine_votes` INT,
    PRIMARY KEY (`election_id`, `precinct`, `party`, `candidate`)
)

CREATE TABLE ny_county_votes (
    `election_id` VARCHAR(256),
    `county` VARCHAR(256),
    `party` VARCHAR(256),
    `candidate` VARCHAR(256),
    `votes` INT,
    PRIMARY KEY (`election_id`, `county`, `party`, `candidate`)
)