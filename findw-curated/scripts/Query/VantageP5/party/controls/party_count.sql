with mainfile_keys
as (
select
key_qualifier,
company_code,
segment_id,
decode(operation_code, 'DELT', 'DELT', 'NON DELT') operation_code,
cast(a.batchid as int) as batchid
from
(
select
key_qualifier,
company_code,
operation_code,
segment_id,
batchid,
row_number() over (partition by key_qualifier,
company_code,
segment_id
order by
cast(a.batchid as int) desc) as ritr
from
{source_database}.gdqp5deltaextract_mainfile_keys a
where
cast(a.batchid as int) <= {batchid}
and a.system_name = 'PS'
and ((a.file_name = 'DIRECT'
and a.segment_id in ('GA', 'GB'))
or (a.file_name like 'CLPROD%'
and a.segment_id = 'DB')))
where
ritr = 1),
gdqp5clprodsegmentdb as (
select
/*+ BROADCAST(delta) */
db.*,
decode(operation_code, 'DELT', 'DELT', 'NON DELT') operation_code
from
(
select
*
from
(
select
key_qualifier,
company_code,
cast(batchid as int) as db_batchid,
row_number() over (partition by key_qualifier,
company_code
order by
cast(batchid as int) desc) as seq_num
from
{source_database}.gdqp5clprodsegmentdb
where
cast(batchid as int) <= {batchid} )
where
seq_num = 1 ) db
left join mainfile_keys delta on db.key_qualifier = delta.key_qualifier
and db.company_code = delta.company_code
and delta.segment_id = 'DB'),
ga_gb as (
select
key_qualifier,
company_code,
min(decode(segment_id, 'GB', 'NON DELT', operation_code)) as operation_code,
batchid as ga_gb_batchid
from
mainfile_keys
where
batchid = {batchid}
and segment_id in ('GA', 'GB')
group by
key_qualifier,
company_code,
batchid
)
where
operation_code != 'DELT'),
role_identification
as (
select
master_number,
a.company_code,
directory_id,
role_code,
gb_batchid,
ritr
from
(
select
master_number,
a.company_code,
directory_id,
role_code,
cast(batchid as int) gb_batchid,
rank() over(partition by directory_id,
master_number,
company_code
order by
cast(batchid as int) desc) ritr
from
{source_database}.gdqp5directsegmentgb_role_identification a
where
cast(batchid as int) <= {batchid}
and not ( role_code like 'PEX' )
and not ( role_code like 'DX' )
and not ( role_code in ('CLI', 'CAA'))) ),
directsegmentga
as (
select
/*+ BROADCAST(a) */
a.*,
ga_gb_batchid,
operation_code
from
select
coalesce(company_individual_code_override, c.company_individual_code, company_individual_code) as company_individual_code,
directory_id,
company_code,
ascii_ignore(coalesce(last_name_override, c.last_name, last_name)) as last_name,
ascii_ignore(coalesce(first_name_override, c.first_name, first_name)) as first_name,
ascii_ignore(coalesce(company_name_override, c.company_name, company_name)) as
company_name,
max(cast(batchid as int)) as ga_batchid
from
{source_database}.gdqp5directsegmentga
where
cast(batchid as int) <= {batchid}
and ( company_name <> ''
or first_name <> ''
or last_name <> '' )
group by
coalesce(company_individual_code_override, c.company_individual_code, company_individual_code),
directory_id,
company_code,
ascii_ignore(coalesce(last_name_override, c.last_name, last_name)),
ascii_ignore(coalesce(first_name_override, c.first_name, first_name)),
ascii_ignore(coalesce(company_name_override, c.company_name, company_name)))
join ga_gb b
on a.directory_id = b.key_qualifier nd a.company_code = b.company_code)
select
{batchid} batch_id,
{cycle_date} cycle_date,
'{domain_name}' domain,
'{source_system_name}' sys_nm,
'BALANCING COUNT' measure_name,
'curated' layer_name,
'recordcount' measure_field,
'INTEGER' measure_value_datatype,
'S' measure_src_typ_ind_indicator,
'gdqp5deltaextract_mainfile_keys' measure_table,
Count(*) measure_value
from
(
select
/*+ BROADCAST(c, d) */
/*directory_id,*/
last_name,
first_name,
master_number,
a.company_code,
company_individual_code,
company_name,
c.operation_code,
b.operation_code b_operation_code,
role_code,
a.directory_id,
case
when b.company_individual_code = 'I' then generateUUID(concat('PS', ':', b.key_qualifier, ':', b.company_code, ':', b.first_name, ':', b.last_name, ':', directory_id))
when b.company_individual_code = 'C' then generateUUID(concat('PS', ':', b.key_qualifier, ':', b.company_code, ':', b.company_name, ':', directory_id))
end as addcontractpartylogic,
a.ga_gb_batchid,
b.ga_batchid,
c.gb_batchid,
d.ga_batchid,
a.batchid,
row_number() over(partition by a.company_name,
last_name,
first_name,
master_number,
a.directory_id
order by
a.ga_gb_batchid desc,
b.ga_batchid desc,
c.gb_batchid desc,
d.ga_batchid desc) as ritrx
from
role_identification a
join gdqp5clprodsegmentdb c
on
a.master_number = c.key_qualifier
and a.company_code = c.company_code
and c.operation_code != 'DELT'
join (
select
distinct key_qualifier,
company_code
from
ga_gb ) d
on
a.directory_id = d.key_qualifier
and a.company_code = d.company_code
join directsegmentga b
using (directory_id,
company_code))
where
ritrx = 1;