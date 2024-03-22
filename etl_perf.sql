

CREATE OR REPLACE FUNCTION public.etl_perf(
    host_name varchar,
    header text,
    "values" text)
  RETURNS text AS
$BODY$declare

 h_tmp varchar[];
 v_tmp text[];
 h_t text[];
 vals text[];
 vals_a text;
 fields text[];
 fields_a text[];
 fields_a_formula text[];
 insert_template_command text;
 update_template_command text;
 fields_list_for_update_in_select_templ text;
 fields_list_for_insert text;
 table_name	text;
 t_n varchar[];
 fields_values_for_insert text;
 fields_list_for_update_in_select text;
 fields_list_for_update text;
 date_time text;
 d_tmp text;
 dh varchar(30);
 dhm_l varchar(30);
 dh_l varchar(30);
 dd_l varchar(30);
 dm_l varchar(30);
 dd varchar(30);
 dm varchar(30);
 host_n varchar(40);
 server_n varchar(40);
 ret_code varchar(40);

 d timestamp with time zone;
 pos int;
 t varchar(64000);
i integer;
begin


 ret_code:='ok';
h_tmp:= concat('{',replace(header,'/min','_min'),'}');-- specific fields like 'MicroStrategy Server Jobs(CastorServer)/Element Browse Submission Rate/min' 
						      -- replace to ...._min because delimeter is '/'
v_tmp:= concat('{',values,'}');


if array_upper(h_tmp,1)<>array_upper(v_tmp,1) then return 'incorrect counts of headers and values';
end if;

begin
  h_t:=regexp_split_to_array(h_tmp[2],'\/+');
  server_n:=concat('''',coalesce(h_t[array_upper(h_t,1)-2],'unknown'),'''');
  host_n:=concat('''',coalesce(host_name,'unknown'),'''');
  
for i in array_lower(h_tmp,1) .. array_upper(h_tmp,1)

loop
  h_t:=regexp_split_to_array(h_tmp[i],'\/+'); -- parsing header delimiter '/'
  h_tmp[i]:=concat(h_t[array_upper(h_t,1)-1],'/',h_t[array_upper(h_t,1)]);--get the text from the penultimate '/' to the end of the line

end loop;


  -- getting list of table names
select array_agg(e.tb) into t_n
from (
	select ef.table_name as tb 
	from etl_fields ef
	group by ef.table_name
	) e
 ;
exception
  WHEN others THEN
   insert into etl_errors(date_event,host,error_code,error_message) values(now(),host_name,SQLSTATE,concat('"',header,'"|"',values,'"'));
    ret_code:= SQLSTATE;
end;

for j in array_lower(t_n,1) .. array_upper(t_n,1)
loop




insert_template_command:='insert into <table_name> (<fields_list_for_insert>) values (<fields_values_for_insert>)'; -- template for insert
-- template for update where <fields_list_for_update>  for instanse will replace by fields list 
update_template_command:='update <table_name> t set <fields_list_for_update>  
	from(
		select * from
			(select date_time_key, server_name, host_name ,
				<fields_list_for_update_in_select> 
			from <table_name> 
			where date_time_key between timestamp <date_time> - time ''10:10''  and <date_time> 
			) ct where ct.date_time_key = <date_time> and server_name=<server_name> and host_name=<host_name>) i where t.date_time_key=i.date_time_key and t.server_name=i.server_name and t.host_name=i.host_name';

table_name:=t_n[j];


vals:=null;
fields:=null;
fields_a:=null;
-- mapping fields description with fields name and get agregate fields names and fourmulas

select 
	array_agg(v.val) val_list,array_agg(e.field_name) field_list ,array_agg(e.field_agg_name) agg_list ,array_agg(e.field_agg_formula)
into vals,fields,fields_a,fields_a_formula
from
	(select unnest(h_tmp) as field_description, -- join array with  descriptions   
		generate_subscripts(h_tmp,1) AS nr  
	) f,
	(select unnest(v_tmp) as val,		    -- and array with values
	generate_subscripts(v_tmp,1) as nr   	    -- by row number
	) v,
etl_fields e 
where f.nr=v.nr and ((e.field_description=f.field_description) or (replace(e.field_description,' ','_')=f.field_description))  and e.table_name=t_n[j];
-- join array with  descriptions and table etl_fields.field_description 

fields_list_for_insert:='';
fields_values_for_insert:='';
fields_list_for_update:='';
fields_list_for_update_in_select:='';
-- continue when no metrics for this table
continue when fields is null;

for i in  array_lower(fields,1) .. array_upper(fields,1)
loop
 -- generating fields lists and values for insert command
 fields_list_for_insert:=concat(fields_list_for_insert,fields[i],',');
 fields_values_for_insert:=concat(fields_values_for_insert,vals[i],',');
 if fields_a[i]<>'NULL' then
    -- generating fields lists for update command
     fields_list_for_update:=concat(fields_list_for_update,fields_a[i],'=i.',fields_a[i],',');
     fields_list_for_update_in_select:=concat(fields_list_for_update_in_select,fields_a_formula[i],',');
    
 end if;
end loop;

begin

date_time:=concat('''',left(v_tmp[1],17),'00''');
d_tmp:=concat(left(v_tmp[1],17),'00'); -- get date_time_key 

d:= TO_TIMESTAMP(d_tmp, 'MM/DD/YYYY HH24:MI:SS');
dh:=concat('''',date_trunc('hour',d),'''');
dd:=concat('''',date_trunc('day',d),'''');
dm:=concat('''',date_trunc('month',d),'''');
dhm_l:=concat('''',to_char(d,'YYYY-MM-DD HH24:MI'),'''');
dh_l:=concat('''',to_char(date_trunc('hour',d),'YYYY-MM-DD HH24'),'''');
dd_l:=concat('''',to_char(date_trunc('day',d),'YYYY-MM-DD'),'''');
dm_l:=concat('''',to_char(date_trunc('month',d),'YYYY-MM'),'''');
fields_list_for_insert:=concat('date_time_key,server_name,host_name,date_time_hour_key,date_time_day_key,date_time_month_key,label_yyyy_mm_dd_hh_mm,label_yyyy_mm_dd_hh,label_yyyy_mm_dd,label_yyyy_mm,',left(fields_list_for_insert,length(fields_list_for_insert)-1));
fields_values_for_insert:=concat(date_time,',',server_n,',',host_n,',',dh,',',dd,',',dm,',',dhm_l,',',dh_l,',',dd_l,',',dm_l,',',left(fields_values_for_insert,length(fields_values_for_insert)-1));
fields_list_for_update_in_select:=left(fields_list_for_update_in_select,length(fields_list_for_update_in_select)-1);
fields_list_for_update:=left(fields_list_for_update,length(fields_list_for_update)-1);
 -- replaceing in templates 
insert_template_command:=replace(insert_template_command,'<fields_list_for_insert>',fields_list_for_insert);
insert_template_command:=replace(insert_template_command,'<fields_values_for_insert>',fields_values_for_insert);
insert_template_command:=replace(insert_template_command,'<table_name>',table_name);
update_template_command:=replace(update_template_command,'<fields_list_for_update>',fields_list_for_update);
update_template_command:=replace(update_template_command,'<table_name>',table_name);
update_template_command:=replace(update_template_command,'<date_time>',date_time);
update_template_command:=replace(update_template_command,'<fields_list_for_update_in_select>',fields_list_for_update_in_select);
update_template_command:=replace(update_template_command,'<server_name>',server_n);
update_template_command:=replace(update_template_command,'<host_name>',host_n);
exception
  WHEN others THEN
   insert into etl_errors(date_event,host,error_code,error_message) values(now(),host_name,SQLSTATE,concat('"',header,'"|"',values,'"'));
    ret_code:= SQLSTATE;
end;
begin
 execute concat('insert into date_time (date_time_key,date_time_hour_key,date_time_day_key,date_time_month_key,
 label_yyyy_mm_dd_hh_mm,label_yyyy_mm_dd_hh,label_yyyy_mm_dd,label_yyyy_mm) values (',date_time,',',dh,',',dd,',',dm,',',dhm_l,',',dh_l,',',dd_l,',',dm_l,')');
exception when others then

end;

begin
 execute concat('insert into host (server_name,host_name) values (',server_n,',',host_n,')');
exception when others then

end;

begin
execute insert_template_command;  -- insert command
exception
  when unique_violation then return 'duplicate key';
  WHEN others THEN
    insert into etl_errors(date_event,host,error_code,error_message) values(now(),host_name,SQLSTATE,concat('|',insert_template_command,'"',header,'"|"',values,'"'));
    ret_code:= SQLSTATE;
    
end;
begin
execute update_template_command; -- update for agregate fields with formula from etl_fields.field_agg_formula
exception
  WHEN others THEN
    insert into etl_errors(date_event,host,error_code,error_message) values(now(),host_name,SQLSTATE,concat('|',update_template_command,'"',header,'"|"',values,'"'));
    ret_code:= SQLSTATE;
end;
--end if;
end loop;
return ret_code;


end;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

