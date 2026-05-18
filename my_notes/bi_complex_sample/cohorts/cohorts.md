# Cohorts per view

## BI_COMPLEX

### `main` (main)
- **Cohort:** *(same population as cte:ReferralDenials, cte:AugDenialSla, cte:ReferralHistoryItemValues, cte:AugHistorySla)*
- **Filters:** *(none)*

### `cte:ReferralDenials` (cte)
- **Cohort:** v cchp authheader fact
- **Filters:**
    - 0 = 0
    - there exists a row where either (from REFERRAL_HIST, where New Rfl Status C = 5) or (from REFERRAL_BED_DAY, where Bed Day Status C = 2)

### `exists:0` (exists)
- **Cohort:** referral hist
- **Filters:**
    - Referral Identifier = Referralid
    - New Rfl Status C = 5

### `exists:1` (exists)
- **Cohort:** referral bed day
- **Filters:**
    - Referralid = Referral Identifier
    - Bed Day Status C = 2

### `cte:AugDenialSla` (cte)
- **Cohort:** v cchp umauthorizationrequest fact with referral
- **Filters:**
    - 0 = 0
    - there exists a row where from V_CCHP_UMAuthorizationRequestStatusHistory_Fact, where Um Status C = 5

### `cte:AugHistorySla` (cte)
- **Cohort:** v cchp umauthorization fact with v cchp umauthorizationhistory fact
- **Base dataset(s):** cte:AugDenialSla
- **Filters:**
    - Um Status = 'Denied' or Um Status = 'Pending Review' and Um Pending Rsn = 'HP Request for Information'

### `cte:ReferralHistoryItemValues` (cte)
- **Cohort:** referral history
- **Base dataset(s):** cte:ReferralDenials
- **Filters:**
    - RFL_HX_ITEM_CHANGE(RFLHIC).ITEM_CHANGE IN (50 , 18007 , 18003 , 2081 , 2080 )
    - ((RFL_HX_ITEM_CHANGE(RFLHIC).ITEM_CHANGE = 18007 AND NOT RFL_HX_NEW_VAL(RFLHNV).NEW_VAL IS NULL) OR (RFL_HX_ITEM_CHANGE(RFLHIC).ITEM_CHANGE = 18003 AND RFL_HX_NEW_VAL(RFLHNV).NEW_VAL = 1640000010 ) OR (RFL_HX_ITEM_CHANGE(RFLHIC).ITEM_CHANGE = 50 AND RFL_HX_NEW_VAL(RFLHNV).NEW_VAL = '5' ) OR (RFL_HX_ITEM_CHANGE(RFLHIC).ITEM_CHANGE = 2081 AND RFL_HX_NEW_VAL(RFLHNV).NEW_VAL = '2' ) OR (RFL_HX_ITEM_CHANGE(RFLHIC).ITEM_CHANGE = 2080 AND NOT RFL_HX_NEW_VAL(RFLHNV).NEW_VAL IS NULL))
