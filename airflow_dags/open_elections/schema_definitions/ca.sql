
CREATE TABLE ca_precinct_votes (
    `election_id` VARCHAR(256),
    `precinct` VARCHAR(256),
    `party` VARCHAR(256),
    `candidate` VARCHAR(256),
    `votes` INT,
    `mail` INT,
    `election_day` INT,
    `absentee` INT,
    `reg_voters` INT,
    `total_ballots` INT,
    `early_votes` INT,
    `provisional` INT,
    `polling` FLOAT,
    PRIMARY KEY (`election_id`, `precinct`, `party`, `candidate`)
)

CREATE TABLE ca_county_votes (
    `election_id` VARCHAR(256),
    `county` VARCHAR(256),
    `party` VARCHAR(256),
    `candidate` VARCHAR(256),
    `votes` INT,
    PRIMARY KEY (`election_id`, `county`, `party`, `candidate`)
)