ReferralDenials AS (

SELECT

[REFERRAL_ID] = RFL. Referralid,

RFL .BedDayAuthRequestId,

[REFERRAL_EXT_ID] = RFL. Externalid, --Sla

[COVERAGE_ID] = RFL. Coverageld,

[ENTRY_DATE] = RFL EntryDate,

[START_DATE] = RFL. StartDate,

[PRIORITY] = RFL. ReferralPriority,

[RFL_LOB_ID] = RFL. ReferralLineOfBusinessId,

[REFERRAL_ PROV_ID] = RFL. ReferralProviderId,

[REFERRING PROV_ID] = RFL. ReferringProviderId,

[RFL_TYPE_C] = RFL. ReferralTypeCategory,

rfl. CustomEntryDateTime --Sla

FROM

CookClarity.Reporting-V_CCHP_AuthHeader_Fact rfl

WHERE

0=0

AND EXISTS (

SELECT

1

FROM

Clarity.dbo.REFERRAL_HIST RFLH

WHERE

RFLH. REFERRAL_ID = nfl. Referralid

AND RFLH. NEW_RFL_STATUS_C = 5 -- Denied

union all select 1 from

Clarity.dbo.REFERRAL_BED_DAY rflbd

where

rfl. Referralid - rflbd. REFERRAL_ ID and rflb.BED_DAY_STATUS_C - 2 - -Denied

)

)



, AugDenialSla as (

select

aug. AUTH_REQUEST_ID,

aug. AUTH_REQUEST_EXTERNAL_ID ,

aug. REFERRAL_ID aug. COVERAGE_ID, 

aug. AUG_RECEIVED_DTTM,

COALESCE (AUG. START_DATE, r f1. [START_DATE]) "START_DATE" --may

aug .[PRIORITY] ,

aug. LOB_ID,

rfl. REFERRAL_PROV_ID ,

rfl. REFERRING_ PROV_ID ,

aug .RFL_TYPE_C

from

CookClarity.Reporting:V_CCHP_UMAuthorizationRequest_Fact aug 

join Clarity.dbo.REFERRAL rfl

on rf1. REFERRAL_ID = aug.REFERRAL_ ID

where



0 = 0

and exists ( select

1

from

CookClarity.Reporting..V_CCHP_UMAuthorizationRequestStatusHistory_Fact aughx

where

aughx. AUTH_REQUEST_ID = aug .AUTH_REQUEST_ID

and aughx.UM_STATUS_C = 5 --Denied

)

—sla end

)