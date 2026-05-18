# Datasets per view

## BI_COMPLEX

### Main query (view output)  *(main)*
- **Base dataset:** Referral Denials, Aug Denial Sla, Referral History Item Values, Aug History Sla
- **Data columns:**
    - `REFERRAL_ID`: Referral Identifier
    - `COVERAGE_ID`: Coverage Identifier
    - `ENTRY_DATE`: Entry Date
    - `START_DATE`: Start Date
    - `PRIORITY`: Priority
    - `REFERRAL_PROV_ID`: Referral Provider Identifier
    - `REFERRING_PROV_ID`: Referring Provider Identifier
    - `AUTH_REQUEST_ID`: Authorization Request Identifier
    - `AUG_RECEIVED_DTTM`: Aug Received Date/Time
    - `LOB_ID`: Lob Identifier
    - `RFL_TYPE_C`: Rfl Type C
    - `ITEM_CHANGE`: Item Change
    - `NEW_VALUE_EXTERNAL`: New Value External
    - `ACTION_DTTM`: Action Date/Time
    - `DENIAL_ROW`: Denial Row
    - `DENIAL_DATE`: Denial Date
    - `DENIAL_REASON`: Denial Reason
    - `RFI_ROW`: Rfi Row
    - `RFI_DATE`: Rfi Date

### Referral Denials  *(cte:ReferralDenials)*
- **Reads tables:** V_CCHP_AuthHeader_Fact, REFERRAL_HIST, REFERRAL_BED_DAY
- **Data columns:**
    - `REFERRAL_ID`: Referralid
    - `BedDayAuthRequestId`: Beddayauthrequestid
    - `REFERRAL_EXT_ID`: Externalid
    - `COVERAGE_ID`: Coverageid
    - `ENTRY_DATE`: Entrydate
    - `START_DATE`: Startdate
    - `PRIORITY`: Referralpriority
    - `RFL_LOB_ID`: Referrallineofbusinessid
    - `REFERRAL_PROV_ID`: Referralproviderid
    - `REFERRING_PROV_ID`: Referringproviderid
    - `RFL_TYPE_C`: Referraltypecategory
    - `CustomEntryDateTime`: Customentrydatetime
- **Filters:**
    - *[where]* 0 = 0
    - *[where]* there exists a row where either (from REFERRAL_HIST, where New Rfl Status C = 5) or (from REFERRAL_BED_DAY, where Bed Day Status C = 2)

### 0  *(exists:0)*
- **Reads tables:** REFERRAL_HIST
- **Data columns:**
    - `1`: 1
- **Filters:**
    - *[where]* Referral Identifier = Referralid
    - *[where]* New Rfl Status C = 5

### 1  *(exists:1)*
- **Reads tables:** REFERRAL_BED_DAY
- **Data columns:**
    - `1`: 1
- **Filters:**
    - *[where]* Referralid = Referral Identifier
    - *[where]* Bed Day Status C = 2

### Aug Denial Sla  *(cte:AugDenialSla)*
- **Reads tables:** V_CCHP_UMAuthorizationRequest_Fact, REFERRAL, V_CCHP_UMAuthorizationRequestStatusHistory_Fact
- **Data columns:**
    - `AUTH_REQUEST_ID`: Authorization Request Identifier
    - `AUTH_REQUEST_EXTERNAL_ID`: Authorization Request External Identifier
    - `REFERRAL_ID`: Referral Identifier
    - `COVERAGE_ID`: Coverage Identifier
    - `AUG_RECEIVED_DTTM`: Aug Received Date/Time
    - `START_DATE`: first non-null of (Start Date, Start Date)
    - `PRIORITY`: Priority
    - `LOB_ID`: Lob Identifier
    - `REFERRAL_PROV_ID`: Referral Provider Identifier
    - `REFERRING_PROV_ID`: Referring Provider Identifier
    - `RFL_TYPE_C`: Rfl Type C
- **Filters:**
    - *[where]* 0 = 0
    - *[where]* there exists a row where from V_CCHP_UMAuthorizationRequestStatusHistory_Fact, where Um Status C = 5

### Aug History Sla  *(cte:AugHistorySla)*
- **Base dataset:** Aug Denial Sla
- **Reads tables:** V_CCHP_UMAuthorization_Fact, V_CCHP_UMAuthorizationHistory_Fact
- **Data columns:**
    - `UM_AUTH_REQUEST_ID`: Um Authorization Request Identifier
    - `CHANGED_BY_USER_ID`: Changed By User Identifier
    - `DENIAL_ROW`: row number, for same Um Authorization Request Identifier, ordered by If of [Um Status = 'Denied', Changed Date/Time, '99991231']
    - `DENIAL_DATE`: If of [Um Status = 'Denied', Changed Date/Time, null]
    - `DENIAL_REASON`: If of [Um Status = 'Denied', Um Denied Rsn, null]
    - `RFI_ROW`: row number, for same Um Authorization Request Identifier, ordered by If of [Um Pending Rsn = 'HP Request for Information', Changed Date/Time, '99991231']
    - `RFI_DATE`: If of [Um Pending Rsn = 'HP Request for Information', Changed Date/Time, null]
    - `UM_STATUS`: Um Status
    - `CHANGED_DTTM`: Changed Date/Time
    - `UM_DENIED_RSN`: Um Denied Rsn
    - `UM_PENDING_RSN`: Um Pending Rsn
- **Filters:**
    - *[where]* Um Status = 'Denied' or Um Status = 'Pending Review' and Um Pending Rsn = 'HP Request for Information'

### Referral History Item Values  *(cte:ReferralHistoryItemValues)*
- **Base dataset:** Referral Denials
- **Reads tables:** REFERRAL_HISTORY, RFL_HX_ACT, RFL_HX_ITEM_CHANGE, RFL_HX_NEW_VAL
- **Data columns:**
    - `REFERRAL_ID`: Referral Identifier
    - `HX_USER_ID`: History User Identifier
    - `ACTION_DTTM`: Action Date/Time
    - `GROUP_LINE`: Group Line
    - `ITEM_CHANGE`: Item Change
    - `NEW_VALUE_EXTERNAL`: New Value External
- **Filters:**
    - *[join_on]* Referral Identifier = Referral Identifier and Group Line = Line
    - *[join_on]* Referral Identifier = Referral Identifier and Group Line = Group Line and Value Line = Value Line and Item Change in (50, 18007, 18003, 2081, 2080)
    - *[join_on]* Referral Identifier = Referral Identifier and Group Line = Group Line and Value Line = Value Line and Item Change = 18007 and New Val is not null or Item Change = 18003 and New Val = 1640000010 or Item Change = 50 and New Val = '5' or Item Change = 2081 and New Val = '2' or Item Change = 2080 and New Val is not null
