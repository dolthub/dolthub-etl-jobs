ALTER TABLE ref_state ADD CHANGE_USER character varying(100);

ALTER TABLE nibrs_month ADD MONTH_PUB_STATUS int;
ALTER TABLE nibrs_month ADD INC_DATA_YEAR int;
ALTER TABLE nibrs_month DROP COLUMN PREPARED_DATE;
ALTER TABLE nibrs_month DROP COLUMN FF_LINE_NUMBER;

ALTER TABLE nibrs_cleared_except ADD CLEARED_EXCEPT_DESC character varying(300);

ALTER TABLE nibrs_incident DROP COLUMN DDOCNAME;
ALTER TABLE nibrs_incident DROP COLUMN FF_LINE_NUMBER;

ALTER TABLE nibrs_bias_list ADD BIAS_DESC character varying(100);

ALTER TABLE nibrs_offense DROP COLUMN FF_LINE_NUMBER;

ALTER TABLE nibrs_offender DROP COLUMN FF_LINE_NUMBER;

ALTER TABLE nibrs_prop_loss_type ADD PROP_LOSS_DESC character varying(250);

ALTER TABLE nibrs_property DROP COLUMN FF_LINE_NUMBER;

ALTER TABLE nibrs_victim DROP COLUMN FF_LINE_NUMBER;
ALTER TABLE nibrs_victim DROP COLUMN AGENCY_DATA_YEAR;

ALTER TABLE nibrs_criminal_act_type ADD CRIMINAL_ACT_DESC character varying(300);

ALTER TABLE nibrs_offense_type ADD OFFENSE_GROUP character(5);

CREATE TABLE agencies (
    YEARLY_AGENCY_ID integer,
    AGENCY_ID bigint NOT NULL,
    DATA_YEAR integer,
    ORI character varying(25),
    LEGACY_ORI character varying(25),
    COVERED_BY_LEGACY_ORI character varying(25),
    DIRECT_CONTRIBUTOR_FLAG character varying(1),
    DORMANT_FLAG character varying(1),
    DORMANT_YEAR integer,
    REPORTING_TYPE character varying(1),
    UCR_AGENCY_NAME character varying(100),
    NCIC_AGENCY_NAME character varying(100),
    PUB_AGENCY_NAME character varying(100),
    PUB_AGENCY_UNIT character varying(100),
    AGENCY_STATUS character varying(1),
    STATE_ID smallint NOT NULL,
    STATE_NAME character varying(100),
    STATE_ABBR character varying(2),
    STATE_POSTAL_ABBR character varying(2),
    DIVISION_CODE integer,
    DIVISION_NAME character varying(100),
    REGION_CODE integer,
    REGION_NAME character varying(100),
    REGION_DESC character varying(100),
    AGENCY_TYPE_NAME character varying(100),
    POPULATION integer,
    SUBMITTING_AGENCY_ID integer,
    SAI character varying(25),
    SUBMITTING_AGENCY_NAME character varying(200),
    SUBURBAN_AREA_FLAG character varying(1),
    POPULATION_GROUP_ID integer,
    POPULATION_GROUP_CODE character varying(2),
    POPULATION_GROUP_DESC character varying(100),
    PARENT_POP_GROUP_CODE integer,
    PARENT_POP_GROUP_DESC character varying(100),
    MIP_FLAG character varying(1),
    POP_SORT_ORDER integer,
    SUMMARY_RAPE_DEF character varying(1),
    PE_REPORTED_FLAG character varying(1),
    MALE_OFFICER integer,
    MALE_CIVILIAN integer,
    MALE_TOTAL integer,
    FEMALE_OFFICER integer,
    FEMALE_CIVILIAN integer,
    FEMALE_TOTAL integer,
    OFFICER_RATE integer,
    EMPLOYEE_RATE integer,
    NIBRS_CERT_DATE date,
    NIBRS_START_DATE date,
    NIBRS_LEOKA_START_DATE date,
    NIBRS_CT_START_DATE date,
    NIBRS_MULTI_BIAS_START_DATE date,
    NIBRS_OFF_ETH_START_DATE date,
    COVERED_FLAG character varying(1),
    COUNTY_NAME character varying(100),
    MSA_NAME character varying(100),
    PUBLISHABLE_FLAG character varying(1),
    PARTICIPATED character varying(1),
    NIBRS_PARTICIPATED character varying(1),
    NIBRS_AGENCY_ID VARCHAR(36),
    INDEX AGENCY_ID_INDEX (AGENCY_ID),
    INDEX STATE_ID_INDEX (STATE_ID),
    INDEX DATA_YEAR_INDEX (DATA_YEAR),
    INDEX NIBRS_AGENCY_ID_INDEX (NIBRS_AGENCY_ID),
    PRIMARY KEY (STATE_ID, DATA_YEAR, NIBRS_AGENCY_ID),
    CONSTRAINT AGENCIES_STATE_ID_FK FOREIGN KEY (STATE_ID) REFERENCES ref_state(STATE_ID)
);

DROP TABLE agency_participation;
DROP TABLE cde_agencies;
