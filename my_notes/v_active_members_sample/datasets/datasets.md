# Datasets per view

## V_ACTIVE_MEMBERS

### Main query (view output)  *(main)*
- **Base dataset:** Active Members
- **Data columns:**
    - `*`: all rows
- **Filters:**
    - *[where]* Row = 1

### Active Members  *(cte:ActiveMembers)*
- **Reads tables:** COVERAGE, COVERAGE_MEMBER_LIST, PLAN_GRP_BEN_PLAN, CLARITY_EPP, CLARITY_LOB, PATIENT, VALID_PATIENT, CVG_LOC_PCP, CLARITY_LOC, CLARITY_SER, ZC_SUBSC_RACE, CVG_SUBSCR_ADDR, ZC_TAX_STATE
- **Data columns:**
    - `ID`: Patient Identifier
    - `Coverage ID`: Coverage Identifier
    - `Name`: Subscriber Name
    - `Member ID`: Mem Number
    - `Coverage Effective Date - 400`: Convert of [DataType, Coverage Eff Date]
    - `Coverage Term Date 410`: Convert of [DataType, Coverage Term Date]
    - `Member Effective From Date 320`: Convert of [DataType, Mem Eff From Date]
    - `Member Effective Term Date 330`: Convert of [DataType, Mem Eff To Date]
    - `LOB`: Lob Name
    - `Plan Name`: Benefit Plan Name
    - `PCP`: Provider Name
    - `Location Name`: Location Name
    - `Birth Date`: Subscriber Birthdate
    - `ROW`: row number, for same Patient Identifier, ordered by Mem Eff From Date, first non-null of (Mem Eff To Date, '29991231')
    - `Race`: Subscriber Race
    - `Member Address 1`: Subscriber Address
    - `Member Address 2`: Subscriber Address
    - `Member City`: Dot of [Placeholder, SUBSCR_CITY]
    - `Member State`: Tax State
    - `Member Zip Code`: Subscriber ZIP Code
    - `Coverage Line`: Line
- **Filters:**
    - *[join_on]* Coverage Identifier = Coverage Identifier and Coverage Type C = 2 and Mem Covered Yn = 'Y' and Mem Eff From Date <= today and Mem Eff To Date >= today or Mem Eff To Date is null
    - *[join_on]* Plan Grp Identifier = Plan Grp Identifier and Benefit Plan Eff Date <= today and Benefit Plan Term Date >= today or Benefit Plan Term Date is null
    - *[join_on]* Patient Identifier = Patient Identifier and Is Valid Patient Yn = 'Y'
    - *[join_on]* Coverage Identifier = Coverage Identifier and Member Identifier = Patient Identifier and Eff Date <= today and Term Date >= today or Term Date is null and Deleted Flag Yn = 'N' or Deleted Flag Yn is null
    - *[join_on]* Coverage Identifier = Coverage Identifier and Line = 1
    - *[join_on]* Coverage Identifier = Coverage Identifier and Line = 2
