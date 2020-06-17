CREATE TABLE elections (
    `state` VARCHAR(256),
    `year` INT,
    `date` DATETIME,
    `election` VARCHAR(256),
    `special` BOOLEAN,
    `office` VARCHAR(256),
    `district` VARCHAR(256),
    `hash_id` VARCHAR(256),
    `count` INT,
    PRIMARY KEY (`hash_id`)
)