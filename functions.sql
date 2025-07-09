-- Name: <Omer Tafveez>
-- Roll No: <25020254>
-- Section: <Section 2>

-- The file contains the template for the functions to be implemented in the assignment. DO NOT MODIFY THE FUNCTION SIGNATURES. Only need to add your implementation within the function bodies.

----------------------------------------------------------
-- 2.1.1 Function to compute billing days
----------------------------------------------------------
-- TASK 2.1.1
CREATE OR REPLACE FUNCTION fun_compute_BillingDays (
    p_ConnectionID IN VARCHAR2,
    p_BillingMonth IN NUMBER,
    p_BillingYear  IN NUMBER
) RETURN NUMBER

IS
    -- Variable declarations
    v_LastReadingPrevMonth DATE;
    v_LastReadingCurrMonth DATE;
    v_BillingDays NUMBER;
    v_PrevMonth NUMBER;
    v_PrevYear NUMBER;
    
BEGIN
    -- Adjust previous month and year if the current month is January
    IF p_BillingMonth = 1 THEN
        v_PrevMonth := 12;
        v_PrevYear := p_BillingYear - 1;
    ELSE
        v_PrevMonth := p_BillingMonth - 1;
        v_PrevYear := p_BillingYear;
    END IF;
    
    -- Retrieve the last reading of the previous month
    SELECT MAX(Tstamp)
    INTO v_LastReadingPrevMonth
    FROM MeterReadings
    WHERE ConnectionID = p_ConnectionID
      AND EXTRACT(MONTH FROM TSTAMP) = v_PrevMonth
      AND EXTRACT(YEAR FROM TSTAMP) = v_PrevYear;

    -- Retrieve the last reading of the current month
    SELECT MAX(TSTAMP)
    INTO v_LastReadingCurrMonth
    FROM MeterReadings
    WHERE ConnectionID = p_ConnectionID
      AND EXTRACT(MONTH FROM TSTAMP) = p_BillingMonth
      AND EXTRACT(YEAR FROM TSTAMP) = p_BillingYear;

    -- Calculate billing days as the difference between current and previous month's last reading
    v_BillingDays := v_LastReadingCurrMonth - v_LastReadingPrevMonth;

    -- Print debug information
    -- DBMS_OUTPUT.PUT_LINE('Connection ID: ' || p_ConnectionID);
    -- DBMS_OUTPUT.PUT_LINE('Billing Month: ' || p_BillingMonth || ', Billing Year: ' || p_BillingYear || ', Prev Billing Month: ' || v_PrevMonth || ', Prev Billing Year' || v_PrevYear);
    -- DBMS_OUTPUT.PUT_LINE('Previous Month: ' || v_PrevMonth || ', Previous Year: ' || v_PrevYear);
    -- DBMS_OUTPUT.PUT_LINE('Last Reading of Previous Month: ' || v_LastReadingPrevMonth);
    -- DBMS_OUTPUT.PUT_LINE('Last Reading of Current Month: ' || v_LastReadingCurrMonth);
    -- DBMS_OUTPUT.PUT_LINE('Billing Days: ' || v_BillingDays);

    -- Return the computed billing days
    RETURN v_BillingDays;

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        -- Handle case where there is no data for the connection or month
        DBMS_OUTPUT.PUT_LINE('No data found for Connection ID: ' || p_ConnectionID);
        RETURN -1;
    WHEN OTHERS THEN
        -- Handle any other errors
        DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
        RETURN -1;

END fun_compute_BillingDays;
/

----------------------------------------------------------
-- 2.1.2 Function to compute Import_PeakUnits
----------------------------------------------------------
CREATE OR REPLACE FUNCTION fun_compute_ImportPeakUnits (
    p_ConnectionID IN VARCHAR2,
    p_BillingMonth IN NUMBER,
    p_BillingYear  IN NUMBER
) RETURN NUMBER

IS
-- varaible declarations
    v_importpeakunits NUMBER;
    previmportpeakunits NUMBER;
    currimportpeakunits NUMBER;
    prevmonth NUMBER;
    prevyear NUMBER;

BEGIN
-- main processing logic

    IF p_BillingMonth = 1 THEN
    prevmonth := 12;
    prevyear := p_BillingYear -1;

    ELSE 
    prevmonth := p_BillingMonth -1;
    prevyear := p_BillingYear;

    END IF;

    SELECT IMPORT_PEAKREADING
    INTO currimportpeakunits
    FROM METERREADINGS
    WHERE connectionID = p_connectionID AND TSTAMP = (SELECT MAX(TSTAMP) FROM meterreadings WHERE connectionID = p_connectionID and EXTRACT(Month FROM TSTAMP)=p_BillingMonth
    AND EXTRACT(YEAR FROM TSTAMP)=p_BillingYear);

    
    SELECT IMPORT_PEAKREADING
    INTO previmportpeakunits
    FROM METERREADINGS
    WHERE connectionID = p_connectionID AND TSTAMP = (SELECT MAX(TSTAMP) FROM meterreadings WHERE connectionID = p_connectionID and EXTRACT(Month FROM TSTAMP)=prevmonth
    AND EXTRACT(YEAR FROM TSTAMP)=prevyear);
        
    v_importpeakunits := currimportpeakunits - previmportpeakunits;


    -- DBMS_OUTPUT.PUT_LINE('Connection ID: ' || p_ConnectionID);
    -- DBMS_OUTPUT.PUT_LINE('Billing Month: ' || p_BillingMonth || ', Billing Year: ' || p_BillingYear);
    -- DBMS_OUTPUT.PUT_LINE('Last Reading of Previous Month: ' || previmportpeakunits);
    -- DBMS_OUTPUT.PUT_LINE('Last Reading of Current Month: ' || currimportpeakunits);
    -- DBMS_OUTPUT.PUT_LINE('import peak units: ' || v_importpeakunits);

    RETURN ROUND(v_importpeakunits, 2);

EXCEPTION
-- exception handling
    WHEN NO_DATA_FOUND THEN
        -- Handle case where there is no data for the connection or month
        DBMS_OUTPUT.PUT_LINE('NO DATA FOUND');
        RETURN -1;
    WHEN OTHERS THEN
        -- Handle any other errors
        DBMS_OUTPUT.PUT_LINE('OTHER ERROR OCCURED.');
        RETURN -1;


END fun_compute_ImportPeakUnits;
/
----------------------------------------------------------
-- 2.1.3 Function to compute Import_OffPeakUnits
----------------------------------------------------------
CREATE OR REPLACE FUNCTION fun_compute_ImportOffPeakUnits (
    p_ConnectionID IN VARCHAR2,
    p_BillingMonth IN NUMBER,
    p_BillingYear  IN NUMBER
) RETURN NUMBER

IS
-- varaible declarations
    v_importoffpeakunits NUMBER;
    previmportoffpeakunits NUMBER;
    currimportoffpeakunits NUMBER;
    prevmonth NUMBER;
    prevyear NUMBER;

BEGIN
-- main processing logic

    IF p_BillingMonth = 1 THEN
    prevmonth := 12;
    prevyear := p_BillingYear -1;

    ELSE 
    prevmonth := p_BillingMonth -1;
    prevyear := p_BillingYear;

    END IF;

    SELECT IMPORT_OFFPEAKREADING
    INTO currimportoffpeakunits
    FROM METERREADINGS
    WHERE connectionID = p_connectionID AND TSTAMP = (SELECT MAX(TSTAMP) FROM meterreadings WHERE connectionID = p_connectionID and EXTRACT(Month FROM TSTAMP)=p_BillingMonth
    AND EXTRACT(YEAR FROM TSTAMP)=p_BillingYear);

    
    SELECT IMPORT_OFFPEAKREADING
    INTO previmportoffpeakunits
    FROM METERREADINGS
    WHERE connectionID = p_connectionID AND TSTAMP = (SELECT MAX(TSTAMP) FROM meterreadings WHERE connectionID = p_connectionID and EXTRACT(Month FROM TSTAMP)=prevmonth
    AND EXTRACT(YEAR FROM TSTAMP)=prevyear);
        
    v_importoffpeakunits := currimportoffpeakunits - previmportoffpeakunits;


    -- DBMS_OUTPUT.PUT_LINE('Connection ID: ' || p_ConnectionID);
    -- DBMS_OUTPUT.PUT_LINE('Billing Month: ' || p_BillingMonth || ', Billing Year: ' || p_BillingYear);
    -- DBMS_OUTPUT.PUT_LINE('Last Reading of Previous Month: ' || previmportoffpeakunits);
    -- DBMS_OUTPUT.PUT_LINE('Last Reading of Current Month: ' || currimportoffpeakunits);
    -- DBMS_OUTPUT.PUT_LINE('import off peak units: ' || v_importoffpeakunits);

    RETURN ROUND(v_importoffpeakunits, 2);

EXCEPTION
-- exception handling
    WHEN NO_DATA_FOUND THEN
        -- Handle case where there is no data for the connection or month
        DBMS_OUTPUT.PUT_LINE('NO DATA FOUND');
        RETURN -1;
    WHEN OTHERS THEN
        -- Handle any other errors
        DBMS_OUTPUT.PUT_LINE('OTHER ERROR OCCURED.');
        RETURN -1;


END fun_compute_ImportOffPeakUnits;
/

----------------------------------------------------------
-- 2.1.4 Function to compute Export_OffPeakUnits
----------------------------------------------------------
CREATE OR REPLACE FUNCTION fun_compute_ExportOffPeakUnits (
    p_ConnectionID IN VARCHAR2,
    p_BillingMonth IN NUMBER,
    p_BillingYear  IN NUMBER
) RETURN NUMBER

IS
-- varaible declarations
    v_exportoffpeakunits NUMBER;
    prevexportoffpeakunits NUMBER;
    currexportoffpeakunits NUMBER;
    prevmonth NUMBER;
    prevyear NUMBER;

BEGIN
-- main processing logic

    IF p_BillingMonth = 1 THEN
    prevmonth := 12;
    prevyear := p_BillingYear -1;

    ELSE 
    prevmonth := p_BillingMonth -1;
    prevyear := p_BillingYear;

    END IF;

    SELECT EXPORT_OFFPEAKREADING
    INTO currexportoffpeakunits
    FROM METERREADINGS
    WHERE connectionID = p_connectionID AND TSTAMP = (SELECT MAX(TSTAMP) FROM meterreadings WHERE connectionID = p_connectionID and EXTRACT(Month FROM TSTAMP)=p_BillingMonth
    AND EXTRACT(YEAR FROM TSTAMP)=p_BillingYear);

    
    SELECT EXPORT_OFFPEAKREADING
    INTO prevexportoffpeakunits
    FROM METERREADINGS
    WHERE connectionID = p_connectionID AND TSTAMP = (SELECT MAX(TSTAMP) FROM meterreadings WHERE connectionID = p_connectionID and EXTRACT(Month FROM TSTAMP)=prevmonth
    AND EXTRACT(YEAR FROM TSTAMP)=prevyear);
        
    v_exportoffpeakunits := currexportoffpeakunits - prevexportoffpeakunits;


    -- DBMS_OUTPUT.PUT_LINE('Connection ID: ' || p_ConnectionID);
    -- DBMS_OUTPUT.PUT_LINE('Billing Month: ' || p_BillingMonth || ', Billing Year: ' || p_BillingYear);
    -- DBMS_OUTPUT.PUT_LINE('Last Reading of Previous Month: ' || prevexportoffpeakunits);
    -- DBMS_OUTPUT.PUT_LINE('Last Reading of Current Month: ' || currexportoffpeakunits);
    -- DBMS_OUTPUT.PUT_LINE('export off peak units: ' || v_exportoffpeakunits);

    RETURN ROUND(v_exportoffpeakunits, 2);

EXCEPTION
-- exception handling
    WHEN NO_DATA_FOUND THEN
        -- Handle case where there is no data for the connection or month
        DBMS_OUTPUT.PUT_LINE('NO DATA FOUND');
        RETURN -1;
    WHEN OTHERS THEN
        -- Handle any other errors
        DBMS_OUTPUT.PUT_LINE('OTHER ERROR OCCURED.');
        RETURN -1;


END fun_compute_ExportOffPeakUnits;
/

----------------------------------------------------------
-- 2.2.1 Function to compute PeakAmount
----------------------------------------------------------
CREATE OR REPLACE FUNCTION fun_compute_PeakAmount (
    p_ConnectionID IN VARCHAR2,
    p_BillingMonth IN NUMBER,
    p_BillingYear IN NUMBER,
    p_BillIssueDate IN DATE
)
RETURN NUMBER
IS
    additionalUnits NUMBER;
    billingDays NUMBER;
    importPeakUnits NUMBER;
    minAmount NUMBER;
    minUnits NUMBER;
    peakAmount NUMBER := 0;
    tariffCode VARCHAR2(50);
    unitRate NUMBER;
    V_AHPC NUMBER;
BEGIN
    importPeakUnits := fun_compute_ImportPeakUnits(p_ConnectionID, p_BillingMonth, p_BillingYear);
    billingDays := fun_compute_BillingDays(p_ConnectionID, p_BillingMonth, p_BillingYear);

    v_AHPC := (importPeakUnits / (billingDays * 24));

    IF importPeakUnits = -1 OR billingDays = -1 THEN
        RETURN -1;
    END IF;

    SELECT TariffCode, MinUnit, MinAmount, RatePerUnit
    INTO tariffCode, minUnits, minAmount, unitRate
    FROM Tariff
    WHERE ConnectionTypeCode = (
        SELECT ConnectionTypeCode FROM Connections WHERE ConnectionID = p_ConnectionID
    )
      AND TariffType = 1
      AND p_BillIssueDate BETWEEN StartDate AND EndDate
      AND V_AHPC BETWEEN ThresholdLow_perHour AND ThresholdHigh_perHour
      AND ROWNUM = 1; 

    additionalUnits := importPeakUnits - (minUnits * billingDays / 30);

    IF additionalUnits > 0 THEN
        peakAmount := (additionalUnits * unitRate) + (minAmount * billingDays / 30);
    ELSE
        peakAmount := minAmount * billingDays / 30;
    END IF;

    RETURN ROUND(peakAmount, 2);

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN -1;
    WHEN OTHERS THEN
        RETURN -1;
END fun_compute_PeakAmount;
/
----------------------------------------------------------
-- 2.2.2 Function to compute OffPeakAmount
----------------------------------------------------------
CREATE OR REPLACE FUNCTION fun_compute_OffPeakAmount(
    p_ConnectionID  IN VARCHAR2,
    p_BillingMonth  IN NUMBER,
    p_BillingYear   IN NUMBER,
    p_BillIssueDate IN DATE
)
RETURN NUMBER
IS
    additionalUnitsimport NUMBER;
    additionalUnitsexport NUMBER;
    billingDays NUMBER;
    importoffPeakUnits NUMBER;
    exportoffPeakUnits Number;
    minAmount NUMBER;
    minUnits NUMBER;
    peakAmount NUMBER := 0;
    tariffCode VARCHAR2(50);
    unitRate NUMBER;
    offpeakamount NUMBER;
    offpeakamountexport NUMBER;
    offpeakamountimport NUMBER;
    V_AHOC NUMBER;
BEGIN
    importoffPeakUnits := fun_compute_ImportOffPeakUnits(p_connectionID, p_BillingMonth, p_BillingYear);
    exportoffPeakUnits := fun_compute_ExportOffPeakUnits(p_connectionID, p_BillingMonth, p_BillingYear);
    billingDays := fun_compute_BillingDays(p_ConnectionID, p_BillingMonth, p_BillingYear);

    v_AHOC := (importoffPeakUnits - exportoffPeakUnits) / (billingDays * 24);

    IF importoffPeakUnits = -1 OR billingDays = -1 OR exportoffPeakUnits = -1 THEN
        RETURN -1;
    END IF;

    SELECT TariffCode, MinUnit, MinAmount, RatePerUnit
    INTO tariffCode, minUnits, minAmount, unitRate
    FROM Tariff
    WHERE ConnectionTypeCode = (
        SELECT ConnectionTypeCode FROM Connections WHERE ConnectionID = p_ConnectionID
    )
      AND TariffType = 2
      AND p_BillIssueDate BETWEEN StartDate AND EndDate
      AND V_AHOC BETWEEN ThresholdLow_perHour AND ThresholdHigh_perHour
      AND ROWNUM = 1; 

    additionalUnitsexport := exportoffPeakUnits - (minUnits * billingDays / 30);
    additionalUnitsimport := importoffPeakUnits - (minUnits * billingDays/30);

    offpeakamountexport := (additionalUnitsexport * unitRate) + (minAmount * billingDays)/30;
    offpeakamountimport := (additionalUnitsimport * unitRate) + (minAmount * billingDays)/30;

    offpeakamount := offpeakamountimport - offpeakamountexport;


    RETURN ROUND(offpeakamount, 2);

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN -1;
    WHEN OTHERS THEN
        RETURN -1;
END fun_compute_OffPeakAmount;
/

----------------------------------------------------------
-- 2.3.1 Function to compute TaxAmount
----------------------------------------------------------
CREATE OR REPLACE FUNCTION fun_compute_TaxAmount (
    p_ConnectionID  IN VARCHAR2,
    p_BillingMonth  IN NUMBER,
    p_BillingYear   IN NUMBER,
    p_BillIssueDate IN DATE,
    p_PeakAmount    IN NUMBER,
    p_OffPeakAmount IN NUMBER
) RETURN NUMBER

IS
    v_TotalTariffAmount NUMBER;
    v_TaxAmount NUMBER := 0;  
    v_TaxRate NUMBER;

    CURSOR tax_cursor IS
        SELECT t.Rate
        FROM taxrates t 
        JOIN connections cn ON cn.connectiontypecode = t.connectiontypecode
        WHERE p_BillIssueDate BETWEEN t.StartDate AND t.EndDate
          AND cn.connectionid = p_ConnectionID;
    

BEGIN
    v_TotalTariffAmount := p_PeakAmount + p_OffPeakAmount;

    OPEN tax_cursor;

    LOOP
        FETCH tax_cursor INTO v_TaxRate;
        EXIT WHEN tax_cursor%NOTFOUND;  

        v_TaxAmount := v_TaxAmount + (v_TotalTariffAmount * v_TaxRate);
    END LOOP;

    CLOSE tax_cursor;

    -- DBMS_OUTPUT.PUT_LINE('Connection ID: ' || p_ConnectionID);
    -- DBMS_OUTPUT.PUT_LINE('Total Tariff Amount (Peak + Off-Peak): ' || v_TotalTariffAmount);
    -- DBMS_OUTPUT.PUT_LINE('Total Tax Amount: ' || v_TaxAmount);

    RETURN ROUND(v_TaxAmount, 2);

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        -- Handle case where there is no data for the connection
        DBMS_OUTPUT.PUT_LINE('No data found for Connection ID: ' || p_ConnectionID);
        RETURN -1;
    WHEN OTHERS THEN
        -- Handle any other errors
        DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
        RETURN -1;

END fun_compute_TaxAmount;
/

----------------------------------------------------------
-- 2.3.2 Function to compute FixedFee Amount
----------------------------------------------------------
CREATE OR REPLACE FUNCTION fun_compute_FixedFee (
    p_ConnectionID  IN VARCHAR2,
    p_BillingMonth  IN NUMBER,
    p_BillingYear   IN NUMBER,
    p_BillIssueDate IN DATE
) RETURN NUMBER

IS
    -- Variable declarations
    v_fixedamount NUMBER := 0;
    v_fixedfee NUMBER;

    -- Cursor to retrieve the fixed fees
    CURSOR fixedfee_cursor IS 
        SELECT fc.fixedfee
        FROM fixedcharges fc 
        JOIN connections cn ON cn.connectiontypecode = fc.connectiontypecode
        WHERE cn.connectionid = p_ConnectionID 
          AND fc.startdate <= p_BillIssueDate 
          AND fc.enddate >= p_BillIssueDate;

BEGIN
    -- Open the cursor to retrieve the fixed fees
    OPEN fixedfee_cursor;

    -- Loop through each row in the cursor
    LOOP
        FETCH fixedfee_cursor INTO v_fixedfee;
        EXIT WHEN fixedfee_cursor%NOTFOUND;  -- Exit when no more records are found

        -- Accumulate the fixed fee amounts
        v_fixedamount := v_fixedamount + v_fixedfee;
    END LOOP;

    -- Close the cursor
    CLOSE fixedfee_cursor;
 
    -- Debug output
    -- DBMS_OUTPUT.PUT_LINE('Connection ID: ' || p_ConnectionID);
    -- DBMS_OUTPUT.PUT_LINE('Total Fixed Amount: ' || v_fixedamount);

    -- Return the total fixed fee amount
    RETURN ROUND(v_fixedamount, 2);

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        -- Handle case where there is no data for the connection
        DBMS_OUTPUT.PUT_LINE('No data found for Connection ID: ' || p_ConnectionID);
        RETURN -1;
    WHEN OTHERS THEN
        -- Handle any other errors
        DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
        RETURN -1;

END fun_compute_FixedFee;
/

----------------------------------------------------------
-- 2.3.3 Function to compute Arrears
----------------------------------------------------------
CREATE OR REPLACE FUNCTION fun_compute_Arrears(
    p_ConnectionID  IN VARCHAR2,
    p_BillingMonth  IN NUMBER,
    p_BillingYear   IN NUMBER,
    p_BillIssueDate IN DATE
) RETURN NUMBER

IS
    -- Variable declarations
    v_totalArrears NUMBER := 0;
    v_dueAmount NUMBER;
    v_paidAmount NUMBER;
    v_outstandingAmount NUMBER;
    v_prevMonth NUMBER;
    v_prevYear NUMBER;

    -- Cursor to retrieve unpaid or partially paid bills for the previous month
    CURSOR arrears_cursor IS
        SELECT b.TOTALAMOUNT_BEFOREDUEDATE, p.amountpaid
        FROM bill b
        JOIN PAYMENTDETAILS p ON p.billid = b.billid
        WHERE b.ConnectionID = p_ConnectionID
          AND b.BillingYear = v_prevYear
          AND b.BillingMonth = v_prevMonth
          AND b.TOTALAMOUNT_BEFOREDUEDATE > p.amountpaid;  -- Select only unpaid or partially paid bills

BEGIN
    -- Determine the previous month and year based on p_BillingMonth and p_BillingYear
    IF p_BillingMonth = 1 THEN
        v_prevMonth := 12;           -- Set the previous month to December
        v_prevYear := p_BillingYear - 1;  -- Set the previous year
    ELSE
        v_prevMonth := p_BillingMonth - 1;  -- Set the previous month
        v_prevYear := p_BillingYear;        -- The year remains the same
    END IF;

    -- Open the cursor to retrieve arrears data for the previous month
    OPEN arrears_cursor;

    -- Loop through each bill and calculate the outstanding amount
    LOOP
        FETCH arrears_cursor INTO v_dueAmount, v_paidAmount;
        EXIT WHEN arrears_cursor%NOTFOUND;  -- Exit when no more records are found

        -- Calculate outstanding amount for each unpaid/partially paid bill
        v_outstandingAmount := v_dueAmount - v_paidAmount;

        -- Accumulate the arrears (outstanding amount)
        v_totalArrears := v_totalArrears + v_outstandingAmount;
    END LOOP;

    -- Close the cursor
    CLOSE arrears_cursor;

    -- Debug output
    -- DBMS_OUTPUT.PUT_LINE('Connection ID: ' || p_ConnectionID);
    -- DBMS_OUTPUT.PUT_LINE('Total Arrears for Previous Month: ' || v_totalArrears);

    -- Return the total arrears amount rounded to 2 decimal places
    RETURN ROUND(v_totalArrears, 2);

EXCEPTION
    WHEN OTHERS THEN
        -- Handle any other errors
        DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
        RETURN -1;

END fun_compute_Arrears;
/

----------------------------------------------------------
-- 2.3.4 Function to compute SubsidyAmount
----------------------------------------------------------
CREATE OR REPLACE FUNCTION fun_compute_SubsidyAmount (
    p_ConnectionID       IN VARCHAR2,
    p_BillingMonth       IN NUMBER,
    p_BillingYear        IN NUMBER,
    p_BillIssueDate      IN DATE,
    p_ImportPeakUnits    IN NUMBER,
    p_ImportOffPeakUnits IN NUMBER
) RETURN NUMBER

IS
    -- Variable declarations
    totalsubsidy NUMBER := 0;
    v_rateperunit NUMBER;
    unitperrate NUMBER;
    billingdays NUMBER;
    intermediate NUMBER;


    -- Cursor to retrieve the subsidy rate for the peak units
    CURSOR peak_subsidy_cursor IS
        SELECT s.rateperunit
        FROM Subsidy s
        JOIN connections cn ON cn.connectiontypecode = s.connectiontypecode
        WHERE cn.connectionid = p_ConnectionID
          AND s.startdate <= p_BillIssueDate 
          AND s.enddate >= p_BillIssueDate;

BEGIN
    billingdays := FUN_COMPUTE_BILLINGDAYS(p_connectionid, p_BillingMonth, p_BillingYear);
    unitperrate := (p_ImportPeakUnits + p_ImportOffPeakUnits)/(billingdays * 24);
    intermediate := unitperrate * billingdays * 24;

    if billingdays = -1 THEN
        RETURN -1;
    END IF;
    -- Step 1: Compute the subsidy for peak units
    OPEN peak_subsidy_cursor;

    -- Fetch the subsidy rate for peak units
    FETCH peak_subsidy_cursor INTO v_rateperunit;
    IF peak_subsidy_cursor%FOUND THEN
        totalsubsidy := intermediate * v_rateperunit;
    END IF;

    -- Close the off-peak subsidy cursor
    CLOSE peak_subsidy_cursor;

    -- Step 4: Return the rounded subsidy amount (rounded to 2 decimal places)
    RETURN ROUND(totalsubsidy, 2);

EXCEPTION
    WHEN OTHERS THEN
        -- Handle any other errors
        DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
        RETURN -1;

END fun_compute_SubsidyAmount;
/


-- ----------------------------------------------------------
-- -- 2.4.1 Function to generate Bill by inserting records in the Bill Table
-- ----------------------------------------------------------
CREATE OR REPLACE FUNCTION fun_Generate_Bill (
    p_BillID        IN NUMBER,
    p_ConnectionID  IN VARCHAR2,
    p_BillingMonth  IN NUMBER,
    p_BillingYear   IN NUMBER,
    p_BillIssueDate IN DATE
) RETURN NUMBER

IS
-- varaible declarations
    importpeakunits NUMBER;
    importoffpeakunits NUMBER;
    exportpeakunits NUMBER := 0;
    exportoffpeakunits NUMBER := 0;
    netpeakunits NUMBER;
    netoffpeakunits NUMBER;
    peakamount NUMBER;
    offpeakamount NUMBER;
    fixedfee NUMBER;
    taxamount NUMBER;
    arrears NUMBER;
    adjustmentamount NUMBER :=0;
    subsidyamount NUMBER;
    duedate DATE;
    totalamountbeforeduedate NUMBER;
    totalamountafterduedate NUMBER;

BEGIN
-- main processing logic
    importpeakunits := fun_compute_ImportPeakUnits(p_ConnectionID, p_BillingMonth, p_BillingYear);
    importoffpeakunits := fun_compute_ImportOffPeakUnits(p_ConnectionID, p_BillingMonth, p_BillingYear);
    exportoffpeakunits := fun_compute_ExportOffPeakUnits(p_connectionID, p_BillingMonth, p_BillingYear);
    peakamount := fun_compute_PeakAmount(p_ConnectionID, p_BillingMonth, p_BillingYear, p_BillIssueDate);

    SELECT Export_PeakReading
    INTO exportpeakunits
    FROM MeterReadings 
    WHERE connectionid = p_connectionID AND TSTAMP = (SELECT MAX(TSTAMP) FROM meterreadings WHERE connectionID = p_connectionID and EXTRACT(Month FROM TSTAMP)=p_BillingMonth and EXTRACT(YEAR FROM TSTAMP)=p_BillingYear);

    offpeakamount := fun_compute_OffPeakAmount(p_ConnectionID, p_BillingMonth, p_BillingYear, p_BillIssueDate);
    fixedfee := fun_compute_FixedFee(p_ConnectionID, p_BillingMonth, p_BillingYear, p_BillIssueDate);
    taxamount := fun_compute_TaxAmount(p_ConnectionID, p_BillingMonth, p_BillingYear, p_BillIssueDate, fun_compute_PeakAmount(p_ConnectionID, p_BillingMonth, p_BillingYear, p_BillIssueDate), offpeakamount);
    arrears := fun_compute_Arrears(p_ConnectionID, p_BillingMonth, p_BillingYear, p_BillIssueDate);
    subsidyamount := fun_compute_SubsidyAmount(p_ConnectionID, p_BillingMonth, p_BillingYear, p_BillIssueDate, importpeakunits, importoffpeakunits);
    netpeakunits := importpeakunits - exportpeakunits;
    netoffpeakunits := importoffpeakunits - exportoffpeakunits;

    duedate := p_BillIssueDate + Interval '10' day;

    adjustmentamount := 0;

    totalamountbeforeduedate := (peakamount + offpeakamount + taxamount + fixedfee) - (subsidyamount + adjustmentamount) + arrears;
    totalamountafterduedate := totalamountbeforeduedate * 1.10;

    INSERT INTO BILL (BILLID, CONNECTIONID, BILLINGMONTH, BILLINGYEAR, BILLISSUEDATE, IMPORT_PEAKUNITS, IMPORT_OFFPEAKUNITS, EXPORT_PEAKUNITS, EXPORT_OFFPEAKUNITS,
    NET_PEAKUNITS, NET_OFFPEAKUNITS, PEAKAMOUNT, OFFPEAKAMOUNT, FIXEDFEE, TAXAMOUNT, ARREARS, ADJUSTMENTAMOUNT, SUBSIDYAMOUNT, DUEDATE, TOTALAMOUNT_BEFOREDUEDATE, TOTALAMOUNT_AFTERDUEDATE) 
    VALUES 
    (p_BillID, p_connectionID, p_BillingMonth, p_BillingYear, p_BillIssueDate, importpeakunits, importoffpeakunits, exportpeakunits, 
    exportoffpeakunits, netpeakunits, netoffpeakunits, peakamount, offpeakamount, fixedfee, taxamount, arrears, adjustmentamount, subsidyamount, duedate, totalamountbeforeduedate, totalamountafterduedate);

    DBMS_OUTPUT.PUT_LINE('Bill generated for Connection ID: ' || p_ConnectionID || ' with Bill ID: ' || p_BillID);

    RETURN 1;

EXCEPTION
-- exception handling
    WHEN OTHERS THEN 
        RETURN -1;

END fun_Generate_Bill;
/

-- ----------------------------------------------------------
-- -- 2.4.2 Function for generating monthly bills of all consumers
-- ----------------------------------------------------------
CREATE OR REPLACE FUNCTION fun_batch_Billing (
    p_BillingMonth  IN NUMBER,
    p_BillingYear   IN NUMBER,
    p_BillIssueDate IN DATE
) RETURN NUMBER

IS
    -- Variable declarations
    v_ConnectionID VARCHAR2(50);
    v_BillID NUMBER := 0;  -- Starting BillID (can be dynamic)
    v_BillsProcessed NUMBER := 0;  -- Counter for processed bills
    v_billgenerate NUMBER;

    -- Cursor to fetch all active connections
    CURSOR connection_cursor IS
        SELECT ConnectionID
        FROM Connections
        WHERE Status = 'ACTIVE';  -- Assuming only active connections are billed

BEGIN
    -- Open the cursor to retrieve active connections
    OPEN connection_cursor;

    -- Loop through each connection and generate bills
    LOOP
        FETCH connection_cursor INTO v_ConnectionID;
        EXIT WHEN connection_cursor%NOTFOUND;

        -- Call the bill generation function for each connection
        BEGIN
            -- Generate the bill for the current connection
            v_billgenerate := fun_Generate_Bill(
                v_BillID,         -- Incremental Bill ID
                v_ConnectionID,   -- Current connection ID
                p_BillingMonth,   -- Billing month from input
                p_BillingYear,    -- Billing year from input
                p_BillIssueDate   -- Bill issue date from input
            );

            -- Increment the BillID for the next connection
            v_BillID := v_BillID + 1;

            -- Increment the counter for successfully processed bills
            v_BillsProcessed := v_BillsProcessed + 1;
        END;
    END LOOP;

    -- Close the cursor
    CLOSE connection_cursor;

    -- Return the total number of processed bills
    RETURN v_BillsProcessed;

EXCEPTION
    WHEN OTHERS THEN
        -- Handle any other errors and return -1 to indicate failure
        DBMS_OUTPUT.PUT_LINE('An error occurred during batch billing: ' || SQLERRM);
        RETURN -1;

END fun_batch_Billing;
/

-- ----------------------------------------------------------
-- -- 3.1.1 Function to process and record Payment
-- ----------------------------------------------------------
CREATE OR REPLACE FUNCTION fun_process_Payment (
    p_BillID          IN NUMBER,
    p_PaymentDate     IN DATE,
    p_PaymentMethodID IN NUMBER,
    p_AmountPaid      IN NUMBER
) RETURN NUMBER

IS
-- varaible declarations
    totalamountbeforeduedate NUMBER;
    totalamountafterduedate NUMBER;
    arrears NUMBER;
    duedate DATE;
    billexists BOOLEAN := False;
    newarrears NUMBER;
    billstatus VARCHAR2(50);
    totalamount NUMBER;
    payment_exists BOOLEAN := False;
    pay_stats VARCHAR2(50);

BEGIN
-- main processing logic

    BEGIN 
        SELECT TOTALAMOUNT_BEFOREDUEDATE, TotalAmount_AfterDueDate, Arrears, duedate
        INTO totalamountbeforeduedate, totalamountafterduedate, arrears, duedate
        FROM BILL
        WHERE BILLID = p_BillID;

        billexists := True;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('Bill ID: ' || p_BillID || ' does not exist.');
            RETURN -1;
    END;

    BEGIN 
        SELECT paymentstatus
        INTO pay_stats
        FROM PaymentDetails
        WHERE BILLID = p_BillID;
        payment_exists := True;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('Payment does not exist for Bill ID: ' || p_BillID);
    END;

    IF billexists THEN
        IF p_PaymentDate > duedate THEN
            totalamount := totalamountafterduedate;
        ELSE
            totalamount := totalamountbeforeduedate;
        END IF;

        IF p_AmountPaid > totalamount THEN
             DBMS_OUTPUT.PUT_LINE('Error: Payment exceeds total amount due for BillID ' || p_BillID);
             RETURN -2;
        END IF;
    
        IF p_AmountPaid < totalamount THEN
            newarrears := totalamount - p_AmountPaid;
            billstatus := 'Partially Paid';
        ELSE
            newarrears := 0;
            billstatus := 'Fully Paid';
        END IF;

        IF payment_exists THEN
            UPDATE PaymentDetails
            SET    PaymentDate = p_PaymentDate,
                   PaymentMethodID = p_PaymentMethodID,
                   AmountPaid = p_AmountPaid,
                   paymentstatus = billstatus
            WHERE  BILLID = p_BillID;
        ELSE
            INSERT INTO PaymentDetails (PaymentDate, PaymentMethodID, AmountPaid, BillID, paymentstatus)
            VALUES (p_PaymentDate, p_PaymentMethodID, p_AmountPaid, p_BillID, billstatus);
        END IF;

        -- INSERT INTO PaymentDetails (PaymentDate, PaymentMethodID, AmountPaid, BillID)
        -- VALUES (p_PaymentDate, p_PaymentMethodID, p_AmountPaid, p_BillID);

        UPDATE Bill
        SET Arrears = newarrears
        WHERE BillID = p_BillID;

        COMMIT;

        RETURN 1;
    
    END IF;

EXCEPTION
-- exception handling   
    WHEN OTHERS THEN
        RETURN -99;

END fun_process_Payment;
/

-- ----------------------------------------------------------
-- -- 4.1.1 Function to make Bill adjustment
-- ----------------------------------------------------------
CREATE OR REPLACE FUNCTION fun_adjust_Bill (
    p_AdjustmentID       IN NUMBER,
    p_BillID             IN NUMBER,
    p_AdjustmentDate     IN DATE,
    p_OfficerName        IN VARCHAR2,
    p_OfficerDesignation IN VARCHAR2,
    p_OriginalBillAmount IN NUMBER,
    p_AdjustmentAmount   IN NUMBER,
    p_AdjustmentReason   IN VARCHAR2
) RETURN NUMBER

IS
-- varaible declarations
    billexists BOOLEAN := False;
    newtotalamountbeforeduedate NUMBER;
    originaltotalamount NUMBER;

BEGIN
-- main processing logic

    BEGIN 
        SELECT TOTALAMOUNT_BEFOREDUEDATE
        INTO originaltotalamount
        FROM BILL
        WHERE BILLID = p_BillID;

        billexists := True;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('Bill ID: ' || p_BillID || ' does not exist.');
            RETURN -1;
    END;

    IF billexists THEN
        INSERT INTO BILLADJUSTMENTS (
            AdjustmentID, BILLID, AdjustmentAmount, AdjustmentReason, AdjustmentDate, OfficerName, OfficerDesignation, OriginalBillAmount
        ) VALUES (
            p_AdjustmentID, p_BillID,  p_AdjustmentAmount, p_AdjustmentReason, p_AdjustmentDate, p_OfficerName, p_OfficerDesignation, p_OriginalBillAmount
        );
    
        newtotalamountbeforeduedate := originaltotalamount + p_AdjustmentAmount;

        UPDATE Bill
        SET AdjustmentAmount = p_AdjustmentAmount, TotalAmount_BeforeDueDate = newtotalamountbeforeduedate
        WHERE BillID = p_BillID;

        COMMIT; 

        RETURN 1;
    END IF;

EXCEPTION
-- exception handling
    WHEN OTHERS THEN
        RETURN -99;

END fun_adjust_Bill;
/