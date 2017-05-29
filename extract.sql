--get starting aiso_seq
drop TABLE TMP_GVVMC_START;
CREATE TABLE TMP_GVVMC_START AS
select T1.vessel_gid,T1.DESTINATION AS ORIGIN,T2.AISO_SEQ AS START_AISO,T2.DESTINATION--,T2.ETA
from GVVMC_AIS_OBS T1, GVVMC_AIS_OBS T2
where T1.vessel_gid=T2.vessel_gid
AND T1.AISO_SEQ=T2.AISO_SEQ-1
AND T1.DESTINATION IN ('SHA')
AND T2.DESTINATION IN ('NIN')
UNION ALL
select T1.vessel_gid,T1.DESTINATION AS ORIGIN,T2.AISO_SEQ AS START_AISO,T2.DESTINATION--,T2.ETA
from GVVMC_AIS_OBS T1, GVVMC_AIS_OBS T2
where T1.vessel_gid=T2.vessel_gid
AND T1.AISO_SEQ=T2.AISO_SEQ-1
AND T1.DESTINATION IN ('NIN')
AND T2.DESTINATION IN ('SHA')
order by vessel_gid,origin,start_aiso,destination;

--get start aiso with enough data (>= 50 ais data in between)
DROP TABLE TMP_GVVMC_START_FINAL;
CREATE TABLE TMP_GVVMC_START_FINAL AS
SELECT VESSEL_GID,ORIGIN,START_AISO,DESTINATION FROM (
SELECT VESSEL_GID,ORIGIN,START_AISO,DESTINATION
,NVL(START_AISO-LAG(START_AISO) OVER (PARTITION BY VESSEL_GID ORDER BY START_AISO),100) AS LG
FROM TMP_GVVMC_START)
WHERE LG >= 50
ORDER BY VESSEL_GID,START_AISO;

--get end aiso
DROP TABLE TMP_GVVMC_START_END;
CREATE TABLE TMP_GVVMC_START_END AS
SELECT VESSEL_GID,ORIGIN,START_AISO,DESTINATION
,NVL(LEAD(START_AISO) OVER (PARTITION BY VESSEL_GID ORDER BY START_AISO)-1,99999) AS END_AISO
FROM TMP_GVVMC_START_FINAL
ORDER BY VESSEL_GID,START_AISO;

--validate end aiso_seq; should be NOT have a long skipped aiso_seq
DROP TABLE TMP_GVVMC_START_END_FINAL;
CREATE TABLE TMP_GVVMC_START_END_FINAL AS
SELECT VESSEL_GID,ORIGIN,DESTINATION,START_AISO,MIN(END_AISO) AS END_AISO
FROM (
SELECT T.VESSEL_GID,T.ORIGIN,T.START_AISO,T.DESTINATION,aiso_seq AS END_AISO
, NVL(LEAD(aiso_seq) OVER (PARTITION BY T.VESSEL_GID ORDER BY aiso_seq)-aiso_seq,1) AS LG
FROM TMP_GVVMC_START_END t, gvvmc_ais_obs g
where g.vessel_gid=t.vessel_gid
and g.aiso_seq between start_aiso and t.end_aiso
and g.destination = t.destination
) WHERE LG > 10
GROUP BY VESSEL_GID,ORIGIN,DESTINATION,START_AISO
order by VESSEL_GID,ORIGIN,DESTINATION,START_AISO;


--validate destination if complete and should be within lat/lon
drop table tmp_gvvmc_completed;
create table tmp_gvvmc_completed as
select t2.*
from gvvmc_ais_obs t1, tmp_gvvmc_start_end_final t2 
where 
(case when (t2.destination = 'SHA') 
        and (t1.lat-31.25 between -.5 and .5)  --sha
        and (t1.lon-121.57 between -.5 and .5) then 1 
       when (t2.destination = 'NIN') 
        and (t1.lat-29.87 between -.5 and .5)  --nin
        and (t1.lon-121.86 between -.5 and .5) then 1  else 0 end)=1
and t1.vessel_gid=t2.vessel_gid
and t1.aiso_seq=t2.end_aiso
order by t1.vessel_gid,start_aiso;

--remove start and ending speed with no change on lat/lon
drop table tmp_gvvmc_actual_start;
create table tmp_gvvmc_actual_start parallel 8 as
select vessel_gid,origin,destination,start_aiso,end_aiso,min(max_aiso) as start_, max(min_aiso) as end_ from (
SELECT distinct t.vessel_gid,t.origin,t.destination,t.start_aiso,t.end_aiso,g.lat,g.lon,min(g.aiso_seq) min_aiso,max(g.aiso_seq) max_aiso
FROM tmp_gvvmc_completed t, gvvmc_ais_obs g
where g.vessel_gid=t.vessel_gid
and g.aiso_seq between t.start_aiso and t.end_aiso
and g.destination = t.destination
group by t.vessel_gid,t.origin,t.destination,t.start_aiso,t.end_aiso,g.lat,g.lon
order by t.vessel_gid,t.origin,t.destination,t.start_aiso,min_aiso
)
having max(min_aiso) -  min(max_aiso) > 30
group by vessel_gid,origin,destination,start_aiso,end_aiso
order by vessel_gid,origin,destination,start_aiso,end_aiso;

--extract the final cleaned table
drop table gvvmc_ais_obs_final; 
create table gvvmc_ais_obs_final parallel 4 as 
select g.VESSEL_GID
,AISO_SEQ
,SPEED
,DRAUGHT
,WIDTH
,LENGTH
,DIM_C
,DIM_D
,TIME_SEEN_UTC  
,LAT
,LON
,t.origin
,t.DESTINATION
,(select eta from gvvmc_ais_obs where vessel_gid=g.vessel_gid and aiso_seq=t.end_ ) as eta
,COURSE
,HEADING
,VESSEL_NAME
,CALL_SIGN
FROM TMP_GVVMC_ACTUAL_START t, gvvmc_ais_obs g
where g.vessel_gid=t.vessel_gid
and g.aiso_seq between start_ and t.end_
and g.destination = t.destination
order by t.vessel_gid,g.aiso_seq;

drop table gvvmc_nin_sha;
create table gvvmc_nin_sha as
select VESSEL_GID
,round(avg(DRAUGHT),2) AVG_DRAUGHT
,round(avg(WIDTH),2) AVG_WIDTH
,round(avg(LENGTH),2) AVG_LENGTH
,round(avg(DIM_C),2) AVG_DIM_C
,round(avg(DIM_D),2) AVG_DIM_D
,TO_CHAR(( EXTRACT(MONTH FROM MIN(TIME_SEEN_UTC)))) AS YRMO
,to_char(MIN(TIME_SEEN_UTC),'WW') AS  YRWK
,round(24*60*(max(TIME_SEEN_UTC)-min(time_seen_utc)),2)   AS TRAVEL_TIME_MINUTES
,round(24*60*(eta-min(TIME_SEEN_UTC) ),2)   AS TRAVEL_TIME_MINUTES_EST
,eta from GVVMC_AIS_OBS_FINAL
WHERE ORIGIN IN ('NIN')
and destination IN ('SHA')
group by vessel_gid,origin,destination,eta
--having (max(TIME_SEEN_UTC)- eta) > 0
order by vessel_gid asc;

drop table gvvmc_SHA_NIN;
create table gvvmc_SHA_NIN as
select VESSEL_GID
,round(avg(DRAUGHT),2) AVG_DRAUGHT
,round(avg(WIDTH),2) AVG_WIDTH
,round(avg(LENGTH),2) AVG_LENGTH
,round(avg(DIM_C),2) AVG_DIM_C
,round(avg(DIM_D),2) AVG_DIM_D
,TO_CHAR( EXTRACT(MONTH FROM MIN(TIME_SEEN_UTC))) AS  YRMO
,to_char(MIN(TIME_SEEN_UTC),'WW') AS  YRWK
,round(24*60*(max(TIME_SEEN_UTC)-min(time_seen_utc)),2)   AS TRAVEL_TIME_MINUTES
,round(24*60*(eta-min(TIME_SEEN_UTC) ),2)   AS TRAVEL_TIME_MINUTES_EST
,eta from GVVMC_AIS_OBS_FINAL
WHERE ORIGIN IN ('SHA')
and destination IN ('NIN')
group by vessel_gid,origin,destination,eta
--having (max(TIME_SEEN_UTC)- eta) > 0
order by vessel_gid asc;



--extract the average speed and YEAR wk and month
drop table tmp_gvvmc_speed;
create table tmp_gvvmc_speed as
select t1.vessel_gid,t1.origin,t1.destination,
round(avg(speed),2) as avg_speed,
min(speed) as min_speed, max(speed) as max_speed,
min(draught) as min_draught, max(draught) as max_draught,
to_char(to_date(MIN(time_seen_utc)),'WW') AS YRWK,
TO_CHAR(( EXTRACT(MONTH FROM MIN(time_seen_utc)))) AS YRMONTH
from gvvmc_ais_obs_final t1, TMP_GVVMC_ACTUAL_START t2
where t1.vessel_gid=t2.vessel_gid
and t1.aiso_seq between t2.start_ and t2.end_
group by t1.vessel_gid,t1.origin,t1.destination,t1.eta
order by t1.vessel_gid,t1.origin,t1.destination;

--extract nubmer of stops or delay 

drop table tmp_gvvmc_delay;
create table tmp_gvvmc_delay as
select t1.vessel_gid,t1.origin,t1.destination,
to_char(to_date(mIN(t1.time_seen_utc)),'WW') AS YRWK,
TO_CHAR(( EXTRACT(MONTH FROM MIN(t1.time_seen_utc)))) AS YRMONTH
,count(1) as rdelay
from GVVMC_AIS_OBS_FINAL t1, TMP_GVVMC_ACTUAL_START t2
where t1.vessel_gid=t2.vessel_gid
and t1.aiso_seq between t2.start_ and t2.end_
and t1.speed <= .1
group by t1.vessel_gid,t2.start_,t2.end_,t1.origin,t1.destination,t1.eta
order by t1.eta,t1.vessel_gid;
