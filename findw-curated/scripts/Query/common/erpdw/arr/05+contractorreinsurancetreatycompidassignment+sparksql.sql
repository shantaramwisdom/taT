select contractnumber,
contractsourcesystemname,
reinsurancetreatynumber,
reinsurancetreatycomponentidentifier,
sourcesystemname,
cycle_date,
batch_id
from ( select *,
row_number() over(
partition by contractnumber,
contractsourcesystemname,
reinsurancetreatynumber,
reinsurancetreatycomponentidentifier
order by cycle_date desc,
sourcesystemname desc,
batch_id desc
) fltr
from (
select sourcesystemname,
contractnumber,
contractsourcesystemname,
reinsurancetreatynumber,
reinsurancetreatycomponentidentifier,
cycle_date,
batch_id
from {curated_database}.arr_contracttoreinsurancetreatycompidassignment
where {disable_reinsurance}
cycle_date = '{cycle_date}'
and cdc_action = 'INSERT'
union all
select sourcesystemname,
contractnumber,
contractsourcesystemname,
reinsurancetreatynumber,
reinsurancetreatycomponentidentifier,
cycle_date,
batch_id
from {curated_database}.arr_contracttoreinsurancetreatycompidassignment
where {disable_reinsurance}
cycle_date <= '{cycle_date}'
and cdc_action = 'INSERT'
union all
select sourcesystemname,
contractnumber,
contractsourcesystemname,
reinsurancetreatynumber,
reinsurancetreatycomponentidentifier,
cycle_date,
batch_id
from {curated_database}.ahd_contracttoreinsurancetreatycompidassignment
where {disable_reinsurance}
cycle_date <= '{cycle_date}'
and cdc_action = 'INSERT'
)
)
where fltr = 1