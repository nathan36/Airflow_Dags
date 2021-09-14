ALTER TABLE property
PARTITION BY RANGE(TO_DAYS(parse_dt))
(
    PARTITION p{{ ds_nodash }} VALUES LESS THAN (TO_DAYS( '{{ macros.ds_add(ds, 1) }}' ))
)