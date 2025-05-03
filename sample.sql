sensor.41112493779_2_bat2_temp_max	475
sensor.41112493779_2_bat2_temp_min	475
sensor.41112493779_2_bat2_temp_moyenne

UPDATE statistics
SET    metadata_id = (SELECT id
                      FROM   statistics_meta
                      WHERE  statistic_id = 'sensor.41112493779_2_bat2_temp_moyenne')
WHERE  metadata_id = (SELECT id
                      FROM   statistics_meta
                      WHERE  statistic_id = 'sensor.bat2_temp_moyenne')
  and start_ts not in (
    select start_ts from statistics
    where metadata_id in (SELECT id
                          FROM   statistics_meta
                          WHERE  statistic_id = 'sensor.41112493779_2_bat2_temp_moyenne')
);

bat0_temp_moyenne

UPDATE statistics_short_term
SET    metadata_id = (SELECT id
                      FROM   statistics_meta
                      WHERE  statistic_id = 'sensor.41112493779_2_bat2_temp_moyenne')
WHERE  metadata_id = (SELECT id
                      FROM   statistics_meta
                      WHERE  statistic_id = 'sensor.bat2_temp_moyenne')
  and start_ts not in (
    select start_ts from statistics_short_term
    where metadata_id in (SELECT id
                          FROM   statistics_meta
                          WHERE  statistic_id = 'sensor.41112493779_2_bat2_temp_moyenne')
);


delete from statistics where metadata_id = (SELECT id
                                            FROM   statistics_meta
                                            WHERE  statistic_id = 'sensor.bat1_temp_min');
UPDATE statistics
SET metadata_id=u.new_id
FROM (select m1.id old_id, m2.id new_id, s1.start_ts
      FROM   (select m1.id, m1.statistic_id, 'sensor.bat2' || substr(m1.statistic_id, 30) as statistic_id_2
              FROM   statistics_meta as m1
              WHERE  m1.statistic_id like 'sensor.41112493779_2_battery2%') as m1,
             statistics_meta as m2, statistics s1
      WHERE  s1.metadata_id = m1.id
        and (m1.id, s1.start_ts) not in (select statistics.metadata_id, statistics.start_ts from statistics where
          statistics.metadata_id=m1.statistic_id_2)
        and m2.statistic_id = m1.statistic_id_2) AS u
WHERE statistics.metadata_id = u.old_id and statistics.start_ts=u.start_ts

;

UPDATE statistics SET metadata_id=u2.new_id
FROM (select u.*
    from (select m1.id old_id, m2.id new_id, s1.start_ts
       FROM   (select m1.id, m1.statistic_id, 'sensor.bat0' || substr(m1.statistic_id, 28) as statistic_id_2
               FROM   statistics_meta as m1
               WHERE  m1.statistic_id like 'sensor.41101494805_battery0%') as m1,
              statistics_meta as m2, statistics s1
       WHERE  s1.metadata_id = m1.id
         and m2.statistic_id = m1.statistic_id_2) AS u
    left join statistics_meta m2 on m2.id = u.new_id
    left join statistics s2 on s2.metadata_id=u.old_id and s2.start_ts = u.start_ts
    left join statistics s3 on s3.metadata_id=u.new_id and s3.start_ts = u.start_ts
where s2.metadata_id is not null and s3.metadata_id is null) u2
where statistics.metadata_id = u2.old_id and statistics.start_ts=u2.start_ts;


select m1.id old_id, m2.id new_id, m1.statistic_id_2
 FROM   (select m1.id, m1.statistic_id, 'sensor.bat0' || substr(m1.statistic_id, 28) as statistic_id_2
         FROM   statistics_meta as m1
         WHERE  m1.statistic_id like 'sensor.41101494805_battery3%') as m1
    left join statistics_meta as m2 ON m2.statistic_id = m1.statistic_id_2
 WHERE  m2.id is null;


select u.*
from (select m1.id old_id, m2.id new_id, s1.start_ts, m1.statistic_id_2
      FROM   (select m1.id, m1.statistic_id, 'sensor.bat0' || substr(m1.statistic_id, 28) as statistic_id_2
              FROM   statistics_meta as m1
              WHERE  m1.statistic_id like 'sensor.41101494805_battery0%') as m1,
             statistics_meta as m2, statistics s1
      WHERE  s1.metadata_id = m1.id
        and m2.statistic_id = m1.statistic_id_2) AS u
         left join statistics_meta m2 on m2.id = u.new_id
         left join statistics s2 on s2.metadata_id=u.old_id and s2.start_ts = u.start_ts
         left join statistics s3 on s3.metadata_id=u.new_id and s3.start_ts = u.start_ts
where s3.metadata_id is not null;
 
delete from statistics where metadata_id = (SELECT id
                                            FROM   statistics_meta
                                            WHERE  statistic_id like 'sensor.41101494805_battery0%');


select statistic_id, count(*) from statistics_short_term 
 inner join statistics_meta on    metadata_id=statistics_meta.id
  WHERE  statistic_id like '%_temp_m%'
group by statistic_id;




SELECT statistic_id
FROM   statistics_meta
WHERE  statistic_id like '%bat1_temp_m%';


UPDATE statistics_short_termUPDATE statistics
SET    metadata_id = (SELECT id
                      FROM   statistics_meta
                      WHERE  statistic_id = 'sensor.bat1_total_energy_charge_me')
WHERE  metadata_id = (SELECT id
                      FROM   statistics_meta
                      WHERE  statistic_id = 'sensor.40618491744_battery1_total_energy_charge_me')
  and start_ts not in (
    select start_ts from statistics
    where metadata_id in (SELECT id
                          FROM   statistics_meta
                          WHERE  statistic_id = 'sensor.bat1_total_energy_charge_me')
);

SELECT id
 FROM   statistics_metasensor.40618491744_battery1_balancemos_temperature
 WHERE  m1 statistic_id = '')
     and start_ts not in (
         select start_ts from statistics_short_term
                         where metadata_id in (SELECT id
                                                FROM   statistics_meta
                                                WHERE  statistic_id = 'sensor.bat1_balance_current')
                         );


SET    metadata_id = (SELECT id
                      FROM   statistics_meta
                      WHERE  statistic_id = 'sensor.bat1_balancemos_temperature')
WHERE  metadata_id = (SELECT id
                      FROM   statistics_meta
                      WHERE  statistic_id = 'sensor.40618491744_battery1_balancemos_temperature')
     and start_ts not in (
         select start_ts from statistics_short_term
                         where metadata_id in (SELECT id
                                                FROM   statistics_meta
                                                WHERE  statistic_id = 'sensor.bat1_balance_current')
                         );

select m1.metadata_id, statistics.start_ts
    FROM   statistics_meta m1, statistics_meta m1
    
WHERE  m1.start_ts = m2.start_ts and m1.statistic_id like 'sensor.40618491744_battery1_balance_current'
    and m1.statistic_id = 'sensor.bat1_balance_current'

sensor.40618491744_bat1_temp_max
sensor.40618491744_bat1_temp_min
sensor.40618491744_bat1_temp_moyenne
sensor.41101494805_3_bat0_temp_max_2
sensor.41101494805_3_bat0_temp_min_2
sensor.41101494805_3_bat0_temp_moyenne_2
sensor.41112493779_2_bat2_temp_max
sensor.41112493779_2_bat2_temp_min
sensor.41112493779_2_bat2_temp_moyenne

 - sensor.41101494805_3_bat0_temp_max
 - sensor.41101494805_3_bat0_temp_moyenne
 - sensor.41101494805_3_bat0_temp_min
 - sensor.bat0_temp_max
 - sensor.bat0_temp_min
 - sensor.bat0_temp_moyenne
 - sensor.bat1_temp_max
 - sensor.bat1_temp_max_2
 - sensor.bat1_temp_min
 - sensor.bat1_temp_min_2
 - sensor.bat1_temp_moyenne
 - sensor.bat1_temp_moyenne_2
 - sensor.bat2_temp_max
 - sensor.bat2_temp_max_2
 - sensor.bat2_temp_min
 - sensor.bat2_temp_min_2
 - sensor.bat2_temp_moyenne
 - sensor.bat2_temp_moyenne_2


sensor.40618491744_bat1_temp_max	13
sensor.40618491744_bat1_temp_min	13
sensor.40618491744_bat1_temp_moyenne	13
sensor.41101494805_3_bat0_temp_max	2
sensor.41101494805_3_bat0_temp_max_2	12
sensor.41101494805_3_bat0_temp_min	2
sensor.41101494805_3_bat0_temp_min_2	12
sensor.41101494805_3_bat0_temp_moyenne	2
sensor.41101494805_3_bat0_temp_moyenne_2	12
sensor.41112493779_2_bat2_temp_max	2932
sensor.41112493779_2_bat2_temp_min	2932
sensor.41112493779_2_bat2_temp_moyenne	2932
sensor.bat1_temp_max_2	2
sensor.bat1_temp_min_2	2
sensor.bat1_temp_moyenne_2	2
sensor.bat2_temp_max_2	2
sensor.bat2_temp_min_2	2
sensor.bat2_temp_moyenne_2	2
«