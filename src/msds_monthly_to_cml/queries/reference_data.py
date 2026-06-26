org_code_to_type_map = """
with cte (location_id, org_type_description) as (
	select location_id, org_type_description
    from (
		select distinct
			org_code location_id,
			case
				when org_type_description = 'CLINICAL COMMISSIONING GROUP' then 'nhs-sub-icb-location'
				when org_type_description = 'GENERAL DENTAL PRACTICE' then 'general_dental_practice'
				when org_type_description = 'LOCAL AUTHORITY' then 'local_authority'
				when org_type_description = 'LOCAL HEALTH BOARD SITE' then 'welsh_local_health_board_site'
				when org_type_description = 'NHS ENGLAND (REGION)' then 'nhs-region'
				when org_type_description = 'NHS TRUST' then 'nhs-trust'
				when org_type_description = 'NHS TRUST SITE' then 'nhs-trust-site'
				when org_type_description = 'OPTICAL HEADQUARTERS' then 'optical_headquarters'
				when org_type_description = 'OPTICAL SITE' then 'optical_site'
				when org_type_description = 'PHARMACY' then 'pharmacy'
				when org_type_description = 'PHARMACY SITE' then 'pharmacy-site'
				when org_type_description = 'PRESCRIBING COST CENTRE' then 'prescribing-cost-centre'
				when org_type_description = 'PRIMARY CARE TRUST' then 'invalid'
				when org_type_description = 'PRISON' then 'prison'
				when org_type_description = 'NON-NHS ORGANISATION' then 'non-nhs-org'
				when org_type_description = 'LOCAL HEALTH BOARD' then 'welsh_local_health_board'
				when org_type_description = 'PRIMARY CARE NETWORK' then	'nhs-pcn'
				when org_type_description = 'CARE TRUST SITE' then 'care-home'
				when org_type_description = 'INDEPENDENT SECTOR HEALTHCARE PROVIDER' then 'independent-provider'
				when org_type_description = 'INDEPENDENT SECTOR H/C PROVIDER SITE' then 'independent-provider-site'
				when org_type_description = 'GENERAL MEDICAL PRACTITIONER' then 'nhs-gp'
				when org_type_description = 'STRATEGIC PARTNERSHIP' then 'nhs-icb'
				when org_code in ('ZZ201', 'ZZ888', 'ZZ203', 'ZZ777') then 'non-place-code'
				else null
			end org_type_description
		from
			dss_corporate.dbo.org_daily
		left join 
			(
				select
					ORG_TYPE_CODE,
					DESCRIPTION org_type_description
				from
					DSS_CORPORATE.dbo.ORG_TYPE_DAILY
			) org_type_daily
			on
			org_type_daily.ORG_TYPE_CODE = org_daily.ORG_TYPE_CODE
		where
			org_type_daily.org_type_code is not null
	) inner_q
	where
		org_type_description is not null
)
select
	*
from
	cte
"""

ons_to_ods_map = """
select distinct
	geography_code Org_Code,
	dh_geography_code ods_code
from
	dss_corporate.dbo.ons_chd_geo_equivalents
where
	date_of_termination is null
and
	dh_geography_code is not null
"""