# Server IP: 140.245.109.111

from fastapi import FastAPI, Request, Form
from fastapi.middleware.cors import CORSMiddleware 
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

import datetime
import os
import logging
import oracledb
import uvicorn

d = os.environ.get("ORACLE_HOME")               # Defined by the file `oic_setup.sh`
oracledb.init_oracle_client(lib_dir=d)          # Thick mode

# These environment variables come from `env.sh` file.
user_name = os.environ.get("DB_USERNAME")
user_pswd = os.environ.get("DB_PASSWORD")
db_alias  = os.environ.get("DB_ALIAS")

# make sure to setup connection with the DATABASE SERVER FIRST. refer to python-oracledb documentation for more details on how to connect, and run sql queries and PL/SQL procedures.

app = FastAPI()

logger = logging.getLogger('uvicorn.error')
logger.setLevel(logging.DEBUG)

origins = ['*']

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
) 
app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")


# -----------------------------
# API Endpoints
# -----------------------------

# ---------- GET methods for the pages ----------
@app.get("/", response_class=HTMLResponse)
async def get_index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# Bill payment page
@app.get("/bill-payment", response_class=HTMLResponse)
async def get_bill_payment(request: Request):
    return templates.TemplateResponse("bill_payment.html", {"request": request})

# Bill generation page
@app.get("/bill-retrieval", response_class=HTMLResponse)
async def get_bill_retrieval(request: Request):
    return templates.TemplateResponse("bill_retrieval.html", {"request": request})

# Adjustments page
@app.get("/bill-adjustments", response_class=HTMLResponse)
async def get_bill_adjustment(request: Request):
    return templates.TemplateResponse("bill_adjustment.html", {"request": request})


# ---------- POST methods for the pages ----------
@app.post("/bill-payment", response_class=HTMLResponse)
async def post_bill_payment(request: Request, bill_id: int = Form(...), amount: float = Form(...), payment_method_id: int = Form(...)):
    connection = oracledb.connect(
    user=user_name,
    password = user_pswd,
    dsn = db_alias
    )

    if bill_id <= 0 or amount <= 0 or payment_method_id <= 0:
        return JSONResponse({"error": "Incorrect BillID or Amount or Paymentmethodid"})

    cursor = connection.cursor()
    try:

        #checking the validity of the paymentmethodid
        query = """
        SELECT DISTINCT(PAYMENTMETHODID)
        FROM
        PAYMENTMETHODS
        """

        cursor.execute(query)
        paymentmethods = set(row[0] for row in cursor.fetchall())
        
        if payment_method_id not in paymentmethods:
            return JSONResponse({"error":"Payment Method ID does not exist in the billing system. Select one from 1-5."})

        current_date = datetime.datetime.now()
        
        cursor.execute(
            """
            SELECT b.totalamount_beforeduedate, b.totalamount_afterduedate, b.billissuedate
            FROM bill b
            WHERE b.billid = :bill_id
            """,
            {"bill_id": bill_id},
        )

        bill = cursor.fetchone()
        if not bill:
            return JSONResponse({"error": "Bill not found"}, status_code=404)
        

        totalamount_beforeduedate, totalamount_afterduedate, billissuedate = bill

        cursor.execute("""
        SELECT paymentstatus, amountpaid, paymentdate, paymentmethodid
        FROM paymentdetails
        WHERE billid = :bill_id
        """, {"bill_id": bill_id},)

        # intialization of old_amount_paid
        old_amount_paid = 0.0
        pay = cursor.fetchone()
        if pay:
            old_paymentstatus, old_amount_paid, old_paymentdate, old_paymentmethodid = pay

            if old_paymentstatus == "Fully Paid":
                
                heading_status = "Bill has already been Paid"
                query = """
                select paymentmethoddescription
                from paymentmethods
                WHERE paymentmethodid = :payment_method_id
                """

                cursor.execute(query, {'payment_method_id':old_paymentmethodid})
                paymentmethoddesc = cursor.fetchone()
                payment_method_description = paymentmethoddesc[0]

                query = """
                SELECT Arrears
                FROM bill
                where billid = :bill_id
                """
                cursor.execute(query, {'bill_id':bill_id})
                arrears_results = cursor.fetchone()
                old_arrears = arrears_results[0] 

                payment_details = {
                "bill_id": bill_id,
                "amount": old_amount_paid,
                "payment_method_id": old_paymentmethodid,
                "payment_method_description": payment_method_description,
                "payment_date": old_paymentdate,
                "payment_status": old_paymentstatus,
                "outstanding_amount": old_arrears,
                "heading_status": heading_status
                }

                return templates.TemplateResponse("payment_receipt.html", {"request": request, "payment_details": payment_details})

        result = cursor.callfunc(
                "FUN_PROCESS_PAYMENT",
                int,
                [bill_id, current_date, payment_method_id, amount+old_amount_paid]
            )
        if result != 1:
            return JSONResponse({"error": "Payment processing failed."}, status_code=500)
        
        connection.commit()

        cursor.execute(
            """
            SELECT pm.paymentmethoddescription, pd.paymentstatus
            FROM paymentmethods pm 
            JOIN paymentdetails pd on pd.paymentmethodid = pm.paymentmethodid
            WHERE pd.billid = :bill_id
            """,
            {"bill_id": bill_id},
        )
        payment_method = cursor.fetchone()
        payment_method_description, new_payment_status = payment_method

        query = """
            SELECT Arrears
            FROM bill
            where billid = :bill_id
            """
        cursor.execute(query, {'bill_id':bill_id})
        arrears_results = cursor.fetchone()
        new_arrears = arrears_results[0] 

        heading_status = "Bill has been successfully paid!"
        payment_details = {
            "bill_id": bill_id,
            "amount": amount,
            "payment_method_id": payment_method_id,
            "payment_method_description": payment_method_description,
            "payment_date": current_date,
            "payment_status": new_payment_status,
            "outstanding_amount": max(0.0, new_arrears),
            "heading_status": heading_status
        }

        return templates.TemplateResponse("payment_receipt.html", {"request": request, "payment_details": payment_details})

    except oracledb.Error as e:
        logger.error(f"Database error: {e}")
        return JSONResponse({"error": "An error occurred while retrieving bill details"}, status_code=500)

    finally:
        cursor.close()
        connection.close()


@app.post("/bill-retrieval", response_class=HTMLResponse)
async def post_bill_retrieval(
    request: Request,
    customer_id: str = Form(...),
    connection_id: str = Form(...),
    month: str = Form(...),
    year: str = Form(...),
):


    connection = oracledb.connect(
        user=user_name,
        password = user_pswd,
        dsn = db_alias
    )
    try:
        cursor = connection.cursor()

        query = """
        SELECT CONCAT(CONCAT(cu.firstname, ' '), cu.lastname) as customer_name, cu.address, cu.phonenumber, cu.email,
        cn.connectiontypecode, divi.divisionname, divi.subdivname, cn.installationdate, cn.metertype
        FROM customers cu 
        JOIN connections cn on cn.customerid = cu.customerid
        JOIN divinfo divi on divi.divisionid = cn.divisionid
        WHERE cu.customerid = :customer_id AND cn.connectionid = :connection_id

    """
        # Query customer details
        cursor.execute(query, {'customer_id':customer_id, "connection_id":connection_id})
        customer = cursor.fetchone()

        if not customer:
            return JSONResponse({"error": "Customer not found"}, status_code=404)


        # Query billing details
        query = """
            SELECT billissuedate, net_peakunits, net_offpeakunits, totalamount_beforeduedate, duedate, 
                   totalamount_afterduedate, arrears, fixedfee, taxamount
            FROM bill
            join connections cn on cn.connectionid = bill.connectionid
            WHERE cn.customerid = :customer_id AND cn.connectionid = :connection_id
            AND bill.billingmonth = :month
            AND bill.billingyear = :year
        """
        cursor.execute(query, {"customer_id":customer_id, "connection_id":connection_id, 
            "month": int(month), "year": int(year)})
        bill = cursor.fetchone()

        if not bill:
            return JSONResponse({"error": "Bill not found for the specified month and year"}, status_code=404)

        # GET BILLING DAYS 
        cursor.execute("""
            SELECT fun_compute_BillingDays(:connection_id, :billing_month, :billing_year) AS billing_days
            FROM dual
        """, {
            "connection_id": connection_id,
            "billing_month": int(month),
            "billing_year": int(year)
        })

        billing_days_result = cursor.fetchone()
        if billing_days_result is None:
            logger.warning("No billing days returned by the function")
            billingdays = None
        else:
            billingdays = billing_days_result[0]
            logger.info(f"Billing days computed: {billingdays}")
        
        # GET Import_PeakUnits
        query = """
        SELECT fun_compute_ImportPeakUnits(:connection_id, :billing_month, :billing_year)
        FROM DUAL
        """
        cursor.execute(query, {"connection_id":connection_id, "billing_month": int(month),
        "billing_year": int(year)})
        
        importpeakunits_results = cursor.fetchone()
        if importpeakunits_results is None:
            logger.warning("No import peakunits found")
            importpeakunits = None
        else:
            importpeakunits = importpeakunits_results[0]
            logger.info(f"Import peak units: {importpeakunits}")
        
        # GET IMPORTOFFPEAKUNITS
        query = """
        SELECT fun_compute_ImportOffPeakUnits(:connection_id, :billing_month, :billing_year)
        FROM DUAL
        """
        cursor.execute(query, {"connection_id":connection_id, "billing_month": int(month),
        "billing_year": int(year)})
        
        importoffpeakunits_results = cursor.fetchone()
        if importoffpeakunits_results is None:
            logger.warning("No import peakunits found")
            importoffpeakunits = None
        else:
            importoffpeakunits = importpeakunits_results[0]
            logger.info(f"import off peak units: {importoffpeakunits}")
        
        # GET EXPORTOFFPEAKUNITS
        query = """
        SELECT fun_compute_ExportOffPeakUnits(:connection_id, :billing_month, :billing_year)
        FROM DUAL
        """
        cursor.execute(query, {"connection_id":connection_id, "billing_month": int(month),
        "billing_year": int(year)})
        
        exportoffpeakunits_results = cursor.fetchone()
        if exportoffpeakunits_results is None:
            logger.warning("No import peakunits found")
            exportoffpeakunits = None
        else:
            exportoffpeakunits = exportoffpeakunits_results[0]
            logger.info(f"export off peak units: {exportoffpeakunits}")

        # GET AHPC AND AHOC
        AHPC = importpeakunits/(24 * billingdays)
        AHOC = (importoffpeakunits - exportoffpeakunits)/(billingdays * 24)


        # Query Peak Hour Tariffs
        query_peak = """
        SELECT t.tarrifdescription, t.minunit, t.rateperunit, 
        ((( :peakunitsimport - (t.minunit * :billingdays)/30)*t.rateperunit) + (t.minamount * :billingdays)/30) AS amount
        FROM tariff t
        JOIN connections cn on cn.connectiontypecode = t.connectiontypecode
        JOIN bill b on b.connectionid = cn.connectionid
        WHERE cn.connectiontypecode = :connection_type
        AND t.tarifftype = 1
        AND b.billingyear = :year
        AND b.billingmonth = :month
        AND cn.customerid = :customer_id AND cn.connectionid = :connection_id
        AND b.billissuedate BETWEEN t.startdate and t.enddate
        AND :AHPC BETWEEN t.thresholdlow_perhour and t.thresholdhigh_perhour 
        """

        cursor.execute(query_peak, {
            "customer_id": customer_id,
            "connection_id": connection_id,
            "connection_type": customer[4],
            "AHPC": AHPC,
            "year": int(year),
            "month": int(month),
            "peakunitsimport": importpeakunits,
            "billingdays": billingdays
        })

        # Process Peak Hour Tariffs
        peak_tariffs = [
            {"name": row[0], "units": row[1], "rate": row[2], "amount": row[3]} for row in cursor.fetchall()
        ]

        # Query Off-Peak Hour Tariffs
        query_offpeak = """
        SELECT t.tarrifdescription, (:importoffpeakunits - :exportoffpeakunits) AS unit, t.rateperunit, 
        ((:importoffpeakunits - (t.minunit * :billingdays)/30) * t.rateperunit + (t.minamount * :billingdays)/30) -
        ((:exportoffpeakunits - (t.minunit * :billingdays)/30) * t.rateperunit + (t.minamount * :billingdays)/30) AS amount
        FROM tariff t
        JOIN connections cn on cn.connectiontypecode = t.connectiontypecode
        JOIN bill b on b.connectionid = cn.connectionid
        WHERE cn.connectiontypecode = :connection_type
        AND t.tarifftype = 2
        AND b.billingyear = :year
        AND b.billingmonth = :month
        AND cn.customerid = :customer_id AND cn.connectionid = :connection_id
        AND b.billissuedate BETWEEN t.startdate and t.enddate
        AND :AHOC BETWEEN t.thresholdlow_perhour and t.thresholdhigh_perhour 
        """

        cursor.execute(query_offpeak, {
            "customer_id": customer_id,
            "connection_id": connection_id,
            "connection_type": customer[4],
            "AHOC": AHOC,
            "year": int(year),
            "month": int(month),
            "importoffpeakunits": importoffpeakunits,
            "exportoffpeakunits": exportoffpeakunits,
            "billingdays": billingdays
        })

        # Process Off-Peak Hour Tariffs
        offpeak_tariffs = [
            {"name": row[0], "units": row[1], "rate": row[2], "amount": row[3]} for row in cursor
        ]
        logger.info(AHOC)
        logger.info(offpeak_tariffs)

        # Combine Results into a Single List
        combined_tariffs = peak_tariffs + offpeak_tariffs

        # Aggregate Tariffs
        tariff_aggregation = {}
        for tariff in combined_tariffs:
            name = tariff["name"]
            if name in tariff_aggregation:
                tariff_aggregation[name]["units"] += tariff["units"]
                tariff_aggregation[name]["rate"] += tariff["rate"]
                tariff_aggregation[name]["amount"] += tariff["amount"]
            else:
                tariff_aggregation[name] = {
                    "units": tariff["units"],
                    "rate": tariff["rate"],
                    "amount": tariff["amount"]
                }

        # Convert Aggregated Results to a List of Dictionaries
        tariffs = [
            {"name": name, "units": details["units"], "rate": details["rate"], "amount": details["amount"]}
            for name, details in tariff_aggregation.items()
        ]

        # GET OFFPEAKAMOUNT
        query = """
        SELECT fun_compute_OffPeakAmount(:connection_id, :billing_month, :billing_year, :billissuedate)
        FROM DUAL
        """
        cursor.execute(query, {"connection_id":connection_id, "billing_month":month,
        "billing_year":year, "billissuedate": bill[0]})
        
        offpeakamount_results = cursor.fetchone()
        if offpeakamount_results is None:
            logger.warning("No import peakunits found")
            offpeakamount = None
        else:
            offpeakamount = offpeakamount_results[0]

        
        # GET PEAKAMOUNT
        query = """
        SELECT fun_compute_PeakAmount(:connection_id, :billing_month, :billing_year, :billissuedate)
        FROM DUAL
        """
        cursor.execute(query, {"connection_id":connection_id, "billing_month":month,
        "billing_year":year, "billissuedate": bill[0]})
        
        peakamount_results = cursor.fetchone()
        if peakamount_results is None:
            logger.warning("No import peakunits found")
            peakamount = None
        else:
            peakamount = offpeakamount_results[0]
        
        # GET TAXES
        query = """
        SELECT tr.taxtype, 
            ((:peakamount + :offpeakamount) * tr.rate) AS amount
            FROM taxrates tr 
            JOIN connections cn on cn.connectiontypecode = tr.connectiontypecode
            JOIN Bill b on b.connectionid = cn.connectionid
            WHERE cn.connectiontypecode = :connection_type
            AND cn.connectionid = :connection_id
            AND cn.customerid = :customer_id
            AND b.billingmonth = :month
            AND b.billingyear = :year
            AND :issuedate BETWEEN startdate AND enddate
        """
        # Query applicable taxes
        cursor.execute(query, {
            "connection_type": customer[4],
            "connection_id": connection_id,
            "customer_id": customer_id,
            "year": year,
            "month": month,
            "peakamount": peakamount,
            "offpeakamount": offpeakamount,
            "issuedate": bill[0]
        })

        # Aggregate taxes
        taxes_aggregation = {}
        for row in cursor:
            name, amount = row
            if name in taxes_aggregation:
                taxes_aggregation[name]["amount"] += amount
            else:
                taxes_aggregation[name] = {"amount": amount}

        # Convert aggregated results to a list
        taxes = [{"name": name, "amount": details["amount"]} for name, details in taxes_aggregation.items()]
        total_taxes = sum(details["amount"] for details in taxes_aggregation.values())

        query = """
            SELECT s.subsidydescription, p.providername,s.rateperunit
            FROM subsidy s
            JOIN subsidyprovider p on p.providerid = s.providerid
            JOIN connections cn on cn.connectiontypecode = s.connectiontypecode
            JOIN bill b on b.connectionid = cn.connectionid
            WHERE s.connectiontypecode = :connection_type AND cn.connectionid = :connection_id AND cn.customerid = :customer_id
            AND b.billingmonth = :month
            AND b.billingyear = :year
            AND b.billissuedate BETWEEN s.startdate AND s.enddate
        """
        # Query applicable subsidies
        cursor.execute(query, {
            "connection_type": customer[4],
            "connection_id": connection_id,
            "customer_id": customer_id,
            "year": year,
            "month": month
        })
        
        subsidies = [{"name": row[0], "provider_name": row[1], "rate_per_unit": row[2]} for row in cursor]

        # Query applicable fixed fees
        query = """
            SELECT f.fixedchargetype, f.fixedfee
            FROM fixedcharges f
            JOIN connections cn on cn.connectiontypecode = f.connectiontypecode
            WHERE cn.connectiontypecode = :connection_type
            AND cn.connectionid = :connection_id
            AND cn.customerid = :customer_id
            AND :issue_date BETWEEN startdate AND enddate
        """
        cursor.execute(query, {"connection_type": customer[4], "customer_id":customer_id,
        "connection_id": connection_id, "issue_date":bill[0]})

        fixed_fees = [{"name": row[0], "amount": row[1]} for row in cursor]
        total_fixedfees = sum(details["amount"] for details in fixed_fees)

        # Query previous bills
        query = """
            SELECT 
            EXTRACT(MONTH FROM b.billissuedate) || '-' || EXTRACT(YEAR FROM b.billissuedate) AS month_year,
            b.totalamount_beforeduedate,
            b.duedate,
            NVL(p.paymentstatus, 'No Payment') AS paymentstatus
        FROM 
            bill b
        LEFT JOIN 
            paymentdetails p ON p.billid = b.billid
        JOIN 
            connections cn ON cn.connectionid = b.connectionid
        WHERE 
            cn.customerid = :customer_id
            AND cn.connectionid = :connection_id
        ORDER BY 
            b.billissuedate DESC
        FETCH FIRST 10 ROWS ONLY

        """
        cursor.execute(query, {"customer_id":customer_id, "connection_id":connection_id})
        previous_bills = [{"month": row[0], "amount": row[1], "due_date": row[2], "status": row[3]} for row in cursor]


        bill_details = {
        "customer_id": customer_id,
        "connection_id": connection_id,
        "customer_name": customer[0],
        "customer_address": customer[1],
        "customer_phone": customer[2],
        "customer_email": customer[3],
        "connection_type": customer[4],
        "division": customer[5],
        "subdivision": customer[6],
        "installation_date": customer[7],
        "meter_type": customer[8],
        "issue_date": bill[0],
        "net_peak_units": bill[1],
        "net_off_peak_units": bill[2],
        "bill_amount": bill[3],
        "due_date": bill[4],
        "amount_after_due_date": bill[5],
        "month": month,
        "arrears_amount": bill[6],
        "fixed_fee_amount": total_fixedfees,
        "tax_amount": total_taxes,
        "tariffs": tariffs,
        "taxes": taxes,
        "subsidies": subsidies,
        "fixed_fee": fixed_fees,
        "bills_prev": previous_bills
    }
    

        return templates.TemplateResponse("bill_details.html", {"request": request, "bill_details": bill_details})

    except oracledb.Error as e:
        logger.error(f"Database error: {e}")
        return JSONResponse({"error": "An error occurred while retrieving bill details"}, status_code=500)
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

# Code for handling adjustments goes here
@app.post("/bill-adjustment", response_class=HTMLResponse)
async def post_bill_adjustments(
    request: Request,
    bill_id: int = Form(...),
    officer_name: str = Form(...),
    officer_designation: str = Form(...),
    original_bill_amount: float = Form(...),
    adjustment_amount: float = Form(...),
    adjustment_reason: str = Form(...),
):
   

    connection = oracledb.connect(
        user=user_name,
        password = user_pswd,
        dsn = db_alias
    )
    try:
        cursor = connection.cursor()
        current_date = datetime.datetime.now()
        adjustment_id = int(f"{bill_id}{current_date.year:04d}{current_date.month:02d}{current_date.day:02d}{current_date.hour:02d}{current_date.minute:02d}")

        query = """
        SELECT totalamount_beforeduedate FROM BILL where billid = :bill_id
        """
        cursor.execute(query, {"bill_id":bill_id})
        original_check = cursor.fetchone()

        if original_bill_amount != original_check[0]:
            return JSONResponse({"error":"Entered Bill does not match with the original bill from the bill ID"})

        if original_bill_amount <= 0 or adjustment_amount <= 0:
            return JSONResponse({"error": "Incorrect original bill amount or adjustment amount"})

        result = cursor.callfunc(
                "fun_adjust_bill",
                int,
                [adjustment_id, bill_id, current_date, officer_name, 
                officer_designation, original_bill_amount, adjustment_amount,
                adjustment_reason]
            )
        if result != 1:
            return JSONResponse({"error": "Adjustment Failed."}, status_code=500)
        

        bill_adjustment = {
            "adjustment_id": adjustment_id,
            "bill_id": bill_id,
            "officer_name": officer_name,
            "officer_designation": officer_designation,
            "original_bill_amount": original_bill_amount,
            "adjustment_amount": adjustment_amount,
            "adjustment_reason": adjustment_reason,
            "adjustment_date": datetime.datetime.now()
        }
        
        return templates.TemplateResponse("adjustment_reciept.html", {"request": request, "bill_adjustment": bill_adjustment})
    
    except oracledb.Error as e:
        logger.error(f"Database error: {e}")
        return JSONResponse({"error": "An error occurred while retrieving bill details"}, status_code=500)
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

    

if __name__ == "__main__":
    uvicorn.run(app, host='0.0.0.0', port=8000)