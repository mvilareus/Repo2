################################################ DRIVERS CODE #################################################
### CONNECT TO SNOWFLAKE WITH COMPETITIVENESS CREDENTIALS
import snowflake.connector as sf
import config 
conn = sf.connect(user=config.user, password=config.password, account=config.account)
# Chose the time (month o year) to be analyzed. If it's year indicated 2021-01-01
time = "month"
data = "2021-08-01" #!!!!!!!!!!!!!!!!!

def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()

############################################# 1. SUPPLIERS DRIVER #############################################
# Goal: calculate the % of P by destination when we have all the suppliers ('None') or when we exclude a supplier
try:
    sql = 'use {}'.format(config.database)
    execute_query(conn,sql)

    sql = 'use warehouse {}'.format(config.wharehouse)
    execute_query(conn, sql)

# Intro table: select the min price by GMH6 supplier from the FTP:
    sql = """CREATE OR REPLACE VIEW "HBG_COMPETITIVE_INTELLIGENCE"."SANDBOX_SOURCE"."FTP_DRIVERS_SUPPLIERS_TEST_PYTHON" AS
                SELECT C.*,
                    P."Type of P Destination" AS TYPE_OF_P,
                    (CASE WHEN PRICE_EM IS NULL OR PRICE_HBD IS NULL THEN 'Exclude' ELSE 'Include' END) AS FILTER_AVL
                ---
                FROM (SELECT DISTINCT 
                        A."DestinationCode" AS DESTINATIONCODE,
                        A."Hotel Code" AS HOTEL_CODE,
                        A."SourceMarket" AS SOURCEMARKET,
                        A."Timestamp" AS UPLOAD_DAY, 
                        A."CheckinDate" AS CHECKINDATE, 
                        A."LOS",
                        A."Pax" AS PAX,
                        'B2B VS B2B MULTI' AS "SCOPE",
                        A."Currency" AS CURRENCY,--9
                        MIN(A."Price") AS PRICE_HBD,
                        B."Supplier" AS SUPPLIER,--11
                        B."Price EM" AS PRICE_EM--12
                    --
                    FROM HBG_COMPETITIVE_INTELLIGENCE.SANDBOX_SOURCE.CPT_FTPDAILY A
                    --
                    -- Union of min price for every supplier in EM:
                    ---
                    LEFT JOIN 
                        (SELECT DISTINCT 
                            "DestinationCode",
                            "Hotel Code",
                            "SourceMarket",
                            "Timestamp", 
                            "CheckinDate", 
                            "Supplier",
                            "LOS",
                            "Pax",
                            "Currency",--9
                            MIN("Price") AS "Price EM"
                            --
                            FROM HBG_COMPETITIVE_INTELLIGENCE.SANDBOX_SOURCE.CPT_FTPDAILY
                            --
                            WHERE "Hotel Code" IS NOT NULL
                            AND DATE_TRUNC(%s,"Timestamp") = %s
                            AND "Company" = 'Easymarket'
                            AND "Lead Time Actual" NOT IN ('Ad hoc','Ad hoc special')--,'Free Event')
                            AND NOT ("Lead Time Actual"='Immediate Arrivals' AND NOT (UPPER("Region")='CHINA'))
                            --AND NOT ("Supplier"='Priceline' AND ((UPPER("Region")='CHINA') OR (UPPER("Region")='Middle East- Africa & India')))
                            GROUP BY 1,2,3,4,5,6,7,8,9) B
                        ON A."Hotel Code" = B."Hotel Code" AND A."SourceMarket" = B."SourceMarket" AND A."Pax" = B."Pax"
                        AND A."Timestamp" = B."Timestamp" AND A."CheckinDate" = B."CheckinDate" AND A."LOS"= B."LOS"
                    ---
                    ---Main conditions filters table A
                    --
                    WHERE A."Hotel Code" IS NOT NULL
                    AND DATE_TRUNC(%s,A."Timestamp") = %s
                    AND A."Company" = 'Hotelbeds'
                    AND A."SourceMarket" = 'IT'
                    AND A."Lead Time Actual" NOT IN ('Ad hoc','Ad hoc special')--,'Free Event')
                    AND NOT (A."Lead Time Actual"='Immediate Arrivals' AND NOT (UPPER(A."Region")='CHINA'))
                    GROUP BY 1,2,3,4,5,6,7,8,9,11,12) C
                --ADD TPS HOTEL
                LEFT OUTER JOIN (SELECT DISTINCT "Hotel ID","TPS Hotel"
                    FROM (SELECT DISTINCT "Hotel ID", (CASE WHEN "TPS Hotel" IS NULL THEN 'NO TPS' ELSE "TPS Hotel" END) AS "TPS Hotel"
                                ,ROW_NUMBER() OVER (PARTITION BY "Hotel ID" ORDER BY "Uploaded On" DESC) AS RN
                            FROM "HBG_COMPETITIVE_INTELLIGENCE"."COMMUNITY_SOURCE"."CPT_OVERVIEW_SRM_12M"
                            WHERE "TPS Hotel" IS NOT NULL
                            AND UPPER("Competitor") NOT IN ('WASABI','HOTELS.COM')
                            AND DATE_TRUNC(%s,TO_DATE("Uploaded On",'YYYY-MM-DD'))= %s
                            )
                    WHERE RN=1) TP
                ON C.HOTEL_CODE=TP."Hotel ID"
                --ADD TYPE OF P DESTINATION
                LEFT OUTER JOIN (SELECT DISTINCT "Destination Code","Type of P Destination"
                            FROM "HBG_COMPETITIVE_INTELLIGENCE"."COMMUNITY_SOURCE"."CPT_OVERVIEW_SRM_12M"
                            WHERE  DATE_TRUNC(%s,TO_DATE("Uploaded On",'YYYY-MM-DD'))= %s) P
                ON C.DESTINATIONCODE= P."Destination Code"
                ---- Remove TPS hotels and comparisons with No Availability:
                WHERE TP."TPS Hotel"<> 'TPS'
                AND (SUPPLIER = 'Hotelbeds' OR FILTER_AVL = 'Include');"""
    time_use = (time,data,time,data,time,data,time,data)
    cursor = conn.cursor()
    cursor.execute(sql,time_use)
    
    sql = """CREATE OR REPLACE TABLE "HBG_COMPETITIVE_INTELLIGENCE"."SANDBOX_SOURCE"."FTP_DRIVERS_SUPPLIERS_T_TEST_PYTHON" AS
            SELECT *
            FROM "HBG_COMPETITIVE_INTELLIGENCE"."SANDBOX_SOURCE"."FTP_DRIVERS_SUPPLIERS_TEST_PYTHON" """
    cursor = conn.cursor()
    cursor.execute(sql)

    # List table: create the list of suppliers to be included in the loop for the exclusion
    sql = """CREATE OR REPLACE TABLE "HBG_COMPETITIVE_INTELLIGENCE"."SANDBOX_SOURCE"."DRIVER_SUPPLIERS_LIST_TEST_PYTHON" AS
            SELECT DISTINCT "Supplier" AS SUPPLIER
            FROM HBG_COMPETITIVE_INTELLIGENCE.SANDBOX_SOURCE.CPT_FTPDAILY
                WHERE "Hotel Code" IS NOT NULL
                AND DATE_TRUNC(%s,"Timestamp") = %s
                AND "Company" = 'Easymarket'
                AND "SourceMarket" = 'IT'
                AND "Hotel Code" IS NOT NULL
                AND "Lead Time Actual" NOT IN ('Ad hoc','Ad hoc special')--,'Free Event')
                AND NOT ("Lead Time Actual"='Immediate Arrivals' AND NOT (UPPER("Region")='CHINA'))"""
    time_use = (time,data)
    cursor = conn.cursor()
    cursor.execute(sql,time_use)

    # Add the 'None' supplier that indicated the % of P without any exclusion:
    sql = """INSERT INTO "HBG_COMPETITIVE_INTELLIGENCE"."SANDBOX_SOURCE"."DRIVER_SUPPLIERS_LIST_TEST_PYTHON" SELECT 'None' AS SUPPLIER """
    cursor = conn.cursor()
    cursor.execute(sql)

    # Drivers table: create the table where the drivers will be add:
    sql = """CREATE OR REPLACE TABLE "HBG_COMPETITIVE_INTELLIGENCE"."SANDBOX_SOURCE"."DRIVER_AGREGATED_TABLE_TEST_PYTHON" 
            ( Driver TEXT, Competitor TEXT, Scope TEXT, DC TEXT, Ratemix TEXT, Supplier TEXT,  AVL_SOURCE TEXT, Eclerx_holiday_exclusion TEXT, 
            PAX_TYPE TEXT, GD TEXT,P decimal(20,6), Records number)"""
    cursor = conn.cursor()
    cursor.execute(sql)

    # Supplier Loop: calculate the % of P excluding the list of suppliers
    sql = """CALL "HBG_COMPETITIVE_INTELLIGENCE"."SANDBOX_SOURCE".DRIVER_LOOP_SUPPLIER_TEST_PYTHON()"""
    cursor = conn.cursor()
    cursor.execute(sql)
    
    # Output: see the output table for NYC
    sql = """ SELECT * FROM "HBG_COMPETITIVE_INTELLIGENCE"."SANDBOX_SOURCE"."DRIVER_AGREGATED_TABLE_TEST_PYTHON"
                WHERE DC = 'NYC' AND COMPETITOR <> 'Expedia' AND SCOPE <> 'BOL BB VS B2C' """
    cursor = conn.cursor()
    cursor.execute(sql)
    for C in cursor:
            print(C)

except Exception as e:
    print(e)

############################################# 2. SUPPLIERS DRIVER #############################################
# Goal: calculate the % of P by destination when we have all the suppliers ('None') or when we exclude a supplier

############################################# OUTPUT #############################################
