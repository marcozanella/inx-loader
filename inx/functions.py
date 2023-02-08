from configparser import ConfigParser
import pyodbc, random
import glob
import pandas as pd
import numpy as np
import datetime
import time

# Connects to the db (connection string is passed)
# Returns True is succesfully connected, and the connection object, and the cursor
def connect_db(the_socket, conn_string):
    
    the_socket.send("Connection String:")
    the_socket.send(conn_string)
    try:
        conx = pyodbc.connect(conn_string)
        the_socket.send("Connection established successfully")
        # cursor = conx.curs()
        # the_socket.send(conx)
        # HURRAY!
        conx.autocommit = False
        curs = conx.cursor()
        return (True, conx, curs)
    except pyodbc.ConnectionError as ex:
        the_socket.send("Connection Error")
        return False
    except pyodbc.OperationalError as err:
        the_socket.send("DB connection could not be established")
        return False
    except pyodbc.ProgrammingError as ex:
        the_socket.send (ex.args[1])
        return False
    except Exception as ex:
        the_socket.send("Database connection terminated with unhandled eception")
        the_socket.send(ex)
        return False

def prep_file():
    files_dict = {
        "ke30": "",
        "ke24": "",
        "zaq": "",
        "oo": "",
        "oh": "",
        "oit": "",
        "arr": "",
        "prl": "",
    }
    files = glob.glob('./upload/inxd/*.*')
    pass

def run_process(ws, conx, curs, files, stored_proc=False):
    files_dict = {}
    duties ={}
    for file in files:
        if "ke30" in file:
            duties["ke30"] = file
        if "ke24" in file:
            duties["ke24"] = file
        if "zaq" in file:
            duties["zaq"] = file
        if "oo" in file:
            duties["oo"] = file
        if "oh" in file:
            duties["oh"] = file
        if "oit" in file:
            duties["oit"] = file
        if "arr" in file:
            duties["arr"] = file
        if "prl" in file:
            duties["prl"] = file
    ws.send("duties: " + str(duties))
    ws.send("Processing, it may take some time ..., don't refresh or leave the page ")
    for duty_key, duty in duties.items():
        ws.send("duty_key: " + duty_key + ", duty: " + duty)
        oggi = datetime.datetime.now()
        oggi = oggi.strftime("%Y%m%d-%H%M%S") # 20201120-203456
        match duty_key:
            case "ke30":
                #Preparing dictionaries
                converters_dict = {
                    'Customer': str,
                    'Customer.1': str,
                    'Product': str,
                    'Product.1': str,
                    'Profit Center': str,
                    'Profit Center.1': str,
                    'Period/Year': str,
                    'Period/Year.1': str,
                    'Material Group': str,
                    'Material Group.1': str,
                    'Product Hierarchy': str,
                    'Prod.hierarachy':str,
                    'Cust.Acct.Assg.Group': str,
                    'AccAssmtGrpCust':str,
                    'Fiscal Year': str,
                    'Fiscal Year.1': str,
                    'Sales employee': str,
                    'Sales employee.1': str,
                    'CustomerHierarchy01': str,
                    'CustomerHier01': str,
                    'Sales district': str,
                    'Sales district.1': str,
                    'Period': str,
                    'Period.1': str,
                    'Material pricing grp': str,
                    'Mat.pricing grp': str,
                    'Price group': str,
                    'Price group.1': str,
                    'Division': str,
                    'Division.1': str,
                    'Color': str,
                    'Color.1': str,
                    'Color Group': str,
                    'Color Group.1': str,
                    'Product Line': str,
                    'Product Line.1': str,
                    'Market Segment': str,
                    'Mareket Segment.1': str,
                    'Major Label': str,
                    'Major Label.1': str,
                    'Industry': str,
                    'Industry.1': str,
                    'Brand Name': str,
                    'Brand Name.1': str,
                    'Country': str,
                    'Country.1': str,
                    'Currency': str,
                    'Unit of Measure': str,
                    'Ship-to party': str,
                    'Ship-to party.1': str,
                    'National Account Mgr':str,
                    'National Account Mgr.1':str,
                    'Field Sales Mgr': str,
                    'Field Sales Mgr.1': str,
                    'Unit Sales quantity':str,
                    'VP sales': str
                }
                rename_dict = {
                    "Customer.1":"Customer1",
                    "Product.1":"Product1",
                    "N.Sales/unit Actual": "N#Sales/unit Actual",
                    "Material Group.1":"Material Group1",
                    "Prod.hierarchy": "Prod#hierarchy",
                    "Color.1":"Color1",
                    "Product Line.1":"Product Line1",
                    "Cust.Acct.Assg.Group": "Cust#Acct#Assg#Group",
                    "AccAssmtGrpCust": "CustAcctAssgGrp",
                    "Profit Center.1":"Profit Center1",
                    "Market Segment.1":"Market Segment1",
                    "Major Label.1":"Major Label1",
                    "National Account Mgr.1": "National Account Mgr1",
                    "C.Margin % Actual": "C#Margin % Actual",
                    "Fiscal Year.1": "Fiscal Year1",
                    "Country.1":"Country1",
                    "Sales employee.1": "Sales employee1",
                    "Sales district.1": "Sales district1",
                    "Color Group.1":"Color Group1",
                    "Mat.pricing grp": "Mat#pricing grp", 
                    "Price group.1": "Price group1", 
                    "Industry.1": "Industry1", 
                    "Brand Name.1":"Brand Name1",
                    "Period.1":"Period1",
                    "Division.1": "Division1",
                    "Field Sales Mgr.1":"Field Sales Mgr1",
                    "Period/Year.1":"Period/year1",
                    "Ship-to party.1":"Ship-to party1"
                }
                table_name = "KE30_import"
                work_on_the_file(ws, duty_key, duty, converters_dict, rename_dict, table_name, conx, curs)

            case "ke24":
                converters_dict = {
                    "Currency": str,
                    "CurrencyType": str,
                    "RecordType": str,
                    "YearMonth": str,
                    "DocumentNumber": str,
                    "ItemNumber": str,

                    "CreateOn": datetime,
                    
                    "ReferenceDocument": str,
                    "ReferenceItemNo": str,
                    "CreateBy": str,
                    "CompanyCode": str,
                    "SenderCostCenter": str,
                    "CostElement": str,
                    "CurrencyKey": str,

                    "SalesQuantity": float,
                    
                    "CostElement": str,
                    "CurrencyKey": str,
                    "SalesQuantity": str,
                    "UnitSalesQuantity": str,
                    "YearWeek": str,
                    "Product": str,
                    "IndustryCode1": str,
                    "Industry": str,

                    "PostingDate": datetime,
                    
                    "SalesDistrict": str,
                    "ReferenceOrgUnit": str,
                    "LogSystemSource": str,
                    "ReferenceTransaction": str,
                    "PointOfValuation": str,
                    
                    "Revenue": float,
                    
                    "InvoiceDate": datetime,
                    
                    "BillingType": str,
                    
                    "Year": int,
                    
                    "BusinessArea": str,
                    "CustomerHierarchy01": str,
                    "CustomerHierarchy02": str,
                    "CustomerHierarchy03": str,
                    "CustomerHierarchy04": str,
                    "CustomerHierarchy05": str,
                    "Origin": str,
                    
                    "HierarchyAssignment": int,
                    
                    "AnnualRebates": float,
                    
                    "SalesOrder": str,
                    "CustomerGroup": str,
                    
                    "SalesOrderItem": int,
                    
                    "Customer": str,
                    "ControllingArea": str,
                    "PriceGroup": str,
                    "MaterialPricingGroup": str,
                    "CostObject": str,
                    "CustomerAccountAssignmentGroup": str,
                    "ShiToParty": str,
                    
                    "ExchangeRate": float,
                    
                    "Country": str,
                    "Client": str,
                    "MaterialGroup": str,
                    
                    "QuantityDiscount": float,
                    
                    "MarketSegment": str,
                    "Color": str,
                    "MajorLabel": str,
                    "BrandName": str,
                    "ColorGroup": str,
                    "ProfitabilitySegmentNo": str,
                    "PartnerProfSegment": str,
                    "PartSubNumber": str,
                    "SubNumber": str,
                    
                    "Period": int,
                    "PlanActIndicator": int,
                    
                    "PartnerProfitCenter": str,
                    "DyeInk": str,
                    "ProfitCenter": str,
                    "ProductHierarchy": str, 
                    "SenderBusinessProcess": str,
                    "WBSElement": str,
                    "CurrencyOfRecord": str,
                    "Order": str, 
                    "UpdateStataus": str,
                    "Division": str,
                    "CanceledDocument": str, 
                    "CanceledDocumentItem": str,
                    "TimeCreated": str, 
                    
                    "Date": datetime,
                    
                    "Time": datetime,
                    
                    "Version": str,
                    "SalesOrg": str,
                    "SalesEmployee": str,
                    "DistributionChannel": str,
                    
                    "CostOfSales": float,
                    "Inplant_Depreciation": float,
                    "FreightCharges": float,
                    "MTS_InputVar": float,
                    "MTS_InputPriveVar": float,
                    "MTS_LotsizeVar": float,
                    "MTO_FixFreightCost": float,
                    "MTO_FixMaterialCost": float,
                    "MTO_VariableMaterialCost": float,
                    "MTO_FixOverheadCost": float,
                    "MTO_VariableOverheadCost": float,
                    "MTO_FixProductionCost": float,
                    "MTS_OutputPriceVar": float,
                    "MTO_VariableProductionCost": float,
                    "Inplant_OtherExpenses": float,
                    "Inplant_Payroll": float,
                    "MTS_QuantityVar": float,
                    "MTS_RemainingVar": float,
                    "MTS_ResUsageVar": float,
                    "MTS_FixFreightCost": float,
                    "MTS_FixMaterialCost": float,
                    "MTS_VariableMaterialCost": float,
                    "MTS_FixOverheadCost": float,
                    "MTS_VarialbleOverheadCost": float,
                    "MTS_FixProductionCost": float,
                    "MTS_VariableProductionCost": float,
                    
                    "GoodsIssueDate": datetime,
                    
                    "Plant": str,
                    "NationalAccountManager": str,
                    "ProductLine": str,
                    "VPSales": str,
                    "ProductLineSalesManager": str,
                    "FieldSalesManager": str
                }
                rename_dict = {
                    'Currency type':'CurrencyType',
                    'Record Type':'RecordType',
                    'Period/Year':'YearMonth',
                    'Document number':'DocumentNumber',
                    'Item number':'ItemNumber',
                    'Created On':'CreatedOn',
                    'Reference document':'ReferenceDocument',
                    'Reference item no.':'ReferenceItemNo',
                    'Created By':'CreatedBy',
                    'Company Code':'CompanyCode',
                    'Sender cost center':'senderCostCenter',
                    'Cost Element':'CostElement',
                    'Currency key':'CurrencyKey',
                    'Sales quantity':'SalesQuantity',
                    'Unit Sales quantity':'UnitSalesQuantity',
                    'Week/year':'YearWeek',
                    'Industry Code 1': 'IndustryCode1',
                    'Posting date': 'PostingDate',
                    'Sales district': 'SalesDistrict',
                    'Reference Org Unit': 'ReferenceOrgUnit',
                    'Log. system source': 'LogsystemSource',
                    'Reference Transact.': 'ReferenceTransaction',
                    'Point of valuation': 'PointOfValuation',
                    'Invoice date': 'InvoiceDate',
                    'Billing Type': 'BillingType',
                    'Fiscal Year': 'Year',
                    'Business Area': 'BusinessArea',
                    'CustomerHierarchy01': 'CustomerHierarchy01',
                    'CustomerHierarchy02': 'CustomerHierarchy02',
                    'CustomerHierarchy03': 'CustomerHierarchy03',
                    'CustomerHierarchy04': 'CustomerHierarchy04',
                    'CustomerHierarchy05': 'CustomerHierarchy05',
                    'Origin': 'Origin',
                    'Hierarchy Assignment': 'HierarchyAssignment',
                    'Annual rebates': 'AnnualRebates',
                    'Sales Order': 'SalesOrder',
                    'Customer group': 'CustomerGroup',
                    'Sales Order Item': 'SalesOrderItem',
                    'Customer': 'Customer',
                    'Controlling Area': 'ControllingArea',
                    'Price group': 'PriceGroup',
                    'Material pricing grp': 'MaterialPricingGroup',
                    'Cost Object': 'CostObject',
                    'Cust.Acct.Assg.Group': 'CustomerAccountAssignmentGroup',
                    'Ship-to party': 'ShiToParty',
                    'Exchange rate': 'ExchangeRate',
                    'Material Group': 'MaterialGroup',
                    'Quantity discount': 'QuantityDiscount',
                    'Market Segment': 'MarketSegment',
                    'Major Label': 'MajorLabel',
                    'Brand Name': 'BrandName',
                    'Color Group': 'ColorGroup',
                    'Profitab. Segmt No.': 'ProfitabilitySegmentNo',
                    'Partner prof.segment': 'PartnerProfSegment',
                    'Partner subnumber': 'PartSubNumber',
                    'Subnumber': 'SubNumber',
                    'Plan/Act. Indicator': 'PlanActIndicator',
                    'Partner Profit Ctr': 'PartnerProfitCenter',
                    'Dye Ink': 'DyeInk',
                    'Profit Center': 'ProfitCenter',
                    'Product hierarchy': 'ProductHierarchy',
                    'Sender bus. process': 'SenderBusinessProcess',
                    'WBS Element': 'WBSElement',
                    'Currency of record': 'CurrencyOfRecord',
                    'Update status': 'UpdateStataus',
                    'Canceled document': 'CanceledDocument',
                    'Canceled doc. item': 'CanceledDocumentItem',
                    'Time created': 'TimeCreated',
                    'Sales Organization': 'SalesOrg',
                    'Sales employee': 'SalesEmployee',
                    'Distribution Channel': 'DistributionChannel',
                    'Cost of Sales - SD': 'CostOfSales',
                    'INPLANT - depreciat.': 'Inplant_Depreciation',
                    'Freight Charges': 'FreightCharges',
                    'MTS -Input var.': 'MTS_InputVar',
                    'MTS -Inp. prive var.': 'MTS_InputPriveVar',
                    'MTS -Lotsize var.': 'MTS_LotsizeVar',
                    'MTO -Fix.Freight Cst': 'MTO_FixFreightCost',
                    'MTO -Fix.Mater. Cst': 'MTO_FixMaterialCost',
                    'MTO -Vbl.Mater. Cst': 'MTO_VariableMaterialCost',
                    'MTO -Fix.Overh. Cst': 'MTO_FixOverheadCost',
                    'MTO -Vbl.Overh. Cst': 'MTO_VariableOverheadCost',
                    'MTO -Fix.Prod. Cst': 'MTO_FixProductionCost',
                    'MTS -Outp. price var': 'MTS_OutputPriceVar',
                    'MTO -Vbl.Prod. Cst': 'MTO_VariableProductionCost',
                    'INPLANT - other exp.': 'Inplant_OtherExpenses',
                    'INPLANT - payroll': 'Inplant_Payroll',
                    'MTS -Quantity var.': 'MTS_QuantityVar',
                    'MTS -Remaining var.': 'MTS_RemainingVar',
                    'MTS -Res. usage var.': 'MTS_ResUsageVar',
                    'MTS -Fix.Freight Cst': 'MTS_FixFreightCost',
                    'MTS - Fix. mat. cost': 'MTS_FixMaterialCost',
                    'MTS - Vbl. mat. cost': 'MTS_VariableMaterialCost',
                    'MTS -Fix.Overh. Cst': 'MTS_FixOverheadCost',
                    'MTS -Vbl.Overh. Cst': 'MTS_VarialbleOverheadCost',
                    'MTS -Fix.Prod. Cst': 'MTS_FixProductionCost',
                    'MTS -Vbl.Prod. Cst': 'MTS_VariableProductionCost',
                    'Goods Issue Date': 'GoodsIssueDate',
                    'National Account Mgr': 'NationalAccountManager',
                    'Product Line': 'ProductLine',
                    'VP Sales': 'VPSales',
                    'Prod Line Sls Mgr': 'ProductLineSalesManager',
                    'Field Sales Mgr': 'FieldSalesManager'
                }
                table_name = "KE24_import"
                work_on_the_file(ws, duty_key, duty, converters_dict, rename_dict, table_name, conx, curs)
                pass
            case "zaq":
                converters_dict = {
                    'Material': str,
                    'Description': str,
                    'Sold to': int,
                    'Name': str,
                    'BillingDoc': str,
                    'Invoice Qty': str,
                    'UoM': str,
                    'Unit Price': str,
                    'Invoice Sales': str,
                    'Curr.': str,
                    'Batch': str,
                    'GM (%)': str,
                    'Prof': str,
                    'PTrm': str,
                    'Curr..1': str,
                    'Cost':str,
                    'Can': str,
                    'Bill':str,
                    'Item': str,
                    'Tax amount': str,
                    'Curr..2': str,
                    'Dv': str,
                    'ShPt': str,
                    'Sales doc.': str,
                    'ImportDate': str
                }
                rename_dict = {
                    "Sales doc.":"SalesDoc"
                }
                table_name = "ZAQCODMI9_import"
                work_on_the_file(ws, duty_key, duty, converters_dict, rename_dict, table_name, conx, curs)
            case "oo":
                pass
            case "oh":
                pass
            case "oit":
                pass
            case "arr":
                pass
            case "prl":
                pass
    # Here store procedure are evaluated, and run if necessary
    if stored_proc:
        if len(duties) > 0:
            ws.send("Execution of store procedures ...")
        for duty in duties:
            if duties[duty] != "":
                match duty:
                    case "ke30":
                        sp_name = "spDoKE30Import"
                        curs.execute(sp_name)
                        ws.send(sp_name + " ...done")
                    case "ke24":
                        pass
                    case "zaq":
                        pass
                    case "oo":
                        pass
                    case "oh":
                        pass
                    case "oit":
                        pass
                    case "arr":
                        pass
                    case "prl":
                        pass

       
def grind_the_file(ws, duty_key, tab_name, cursor, sql_statement, dataframe):
    ws.send("-------GRIND THE FILE -----")
    #Get setting of stored procedures

    start_time = time.time()
    df_length = len(dataframe)
    if df_length > 19999:
        chunk_size = 4000
    elif df_length > 14999:
        chunk_size = 3000
    elif df_length > 9999:
        chunk_size = 2000
    elif df_length > 7499:
        chunk_size = 1500
    elif df_length > 4999:
        chunk_size = 1000
    else:
        chunk_size = 500
    
    # Determination of how many rounds
    iterations = df_length / chunk_size
    if ( iterations - int(iterations) ) != 0:
        iterations = int(iterations) + 1
    else:
        iterations = int(iterations)
        ws.send("executing " + duty_key + " in " + str(iterations) + " rounds...")

    # Iterate over chunks
    for iteration in range(iterations):
        lower_limit = iteration * chunk_size
        upper_limit = (iteration * chunk_size) + (chunk_size - 1)
        if upper_limit >= df_length - 1: upper_limit = df_length - 1
        ws.send("from " + str(lower_limit) + " to " + str(upper_limit) )
        chunk_df = dataframe.iloc[lower_limit:upper_limit]
        cursor.fast_executemany = True
        start_time = time.time()

        # Must implement the one_field_at_a_time insert on the chunc to see if
        config = ConfigParser()
        config.read('config.ini')
        one_column_per_time = config["DEFAULT"]["one_column_at_a_time"]
        

        ########################################################################
        # This section is adding one column at a time to check if one field is
        # giving too long insertion time
        ########################################################################
        if one_column_per_time == "True":
            ws.send("start one_column_per_time process")
            # Don't pollute the chunck df
            chunk_df_copy = chunk_df     
            fields_and_time_dict = {}
            # Create a temporary disposable dataframe 
            disposable_df=pd.DataFrame()
            # Cycle through columns 
            for column in range(len(chunk_df_copy.columns)):
                cursor.execute("DELETE FROM " + tab_name)
                chunk_df_copy = chunk_df.iloc[:,column:column+1]
                col_name = chunk_df_copy.columns[0]
                sql_statement_per_column = SQL_statement_fabricator(tab_name, chunk_df_copy.columns.to_list())
                # ws.send("SQL Statement per column: " + sql_statement_per_column)

                # sql_statement_per_rows = SQL_statment_maker(table_name, df_temporary, "--> SQL stmt done 4 col :" + str(col+1) + " " + col_name, one_liner = True)
                # if df_temporary[col_name].dtype == '<M8[ns]':
                #     df_temporary[col_name] = df_temporary[col_name].apply(lambda x: x.date())

                time_at_start = time.time()
                try:
                    cursor.executemany(sql_statement_per_column, chunk_df_copy.values.tolist())
                except pyodbc.Error as err:
                    ws.send(err)
                time_at_finish = time.time()
                time_lapsed = time_at_finish - time_at_start
                fields_and_time_dict[col_name] = time_lapsed
                ws.send(col_name + "\t\ttime lasped: {:0.2f}".format(time_lapsed) + " sec.")
            ws.send("end of the one_column_per_time process")
        ##########################################################################


        # one filed is slower than others - this is a debug strategy
        try:
            cursor.executemany(sql_statement, chunk_df.values.tolist())
        except pyodbc.ProgrammingError as err:
            ws.send("*******************************************")
            ws.send(str(err))

        end_time = time.time()
        duration = end_time - start_time
        ws.send("      done in " + str(round(duration, 2)) + " sec        @ " + str(round(chunk_size / duration, 2)) + " rec/sec")

    pass

# read_the_file
# Reads the Excel file, creates a dataframe, and build the SQL statement
# It returns both the dataframe and the SQL statement
def read_the_file(ws, duty_key, duty, converters_dict, rename_dict, tablename):
    oggi = datetime.datetime.now()
    oggi = oggi.strftime("%Y%m%d-%H%M%S") # 20201120-203456
    ws.send(" ---- read_the_file " + duty_key + " " + duty + " ")
    ws.send(" ---- we read the file and adjust data types and filed names")
    df = pd.read_excel(duty, thousands='.', decimal=',', converters=converters_dict)
    # ws.send(df.columns.to_list())
    ws.send("Renaming fields ...")
    df.rename(columns=rename_dict, inplace=True)
    # ws.send(df.columns.to_list())
    ws.send(duty_key + ": " + str(len(df)) + "records ")
    df = df.replace(np.nan, '')
    ws.send(duty_key + " dataframe created")
    match duty_key:
        case "ke30":
            df['Importtimestamp'] = oggi
            df["YearMonth"] = (df["Fiscal Year"].astype(str) + df["Period"].astype(str).str[-2:]).astype(int)

            # df['YearMonth'] = str(int(df['Fiscal Year'])*100) + str(int(df['Period'])).zfill(2)
            df = df.replace(np.nan, '')
        case "ke24":
            pass

    ws.send("there are " + str(len(df.columns)) + " columns")
    sql_full = SQL_statement_fabricator(tablename, df.columns.to_list())
    return df, sql_full

def truncate_table(ws, conx, curs, tablename):
    ws.send("Truncating ...")
    sqlstatement = "TRUNCATE TABLE " + tablename
    curs.execute(sqlstatement)
    ws.send("Truncated table " + tablename)

def SQL_statement_fabricator(table_name, list_of_columns):
    sql_insert = "INSERT INTO " + table_name
    sql_fields = "([" + "], [".join(list_of_columns) + "])"
    sql_value = "VALUES"
    sql_questionmarks = "(" + ("?, " * len(list_of_columns))[:-2] + ")"
    sql_statement = sql_insert + sql_fields + sql_value + sql_questionmarks
    return sql_statement

def work_on_the_file(ws, duty_key, duty, converters_dict, rename_dict, table_name, conx, curs):
    # Read file into dataframe and make SQL statement
    df, sqlstatement = read_the_file(ws, duty_key, duty, converters_dict, rename_dict, table_name)
    # Export - if needing to export an excel file for verification - with timestamp
    # df.to_excel("./output_dataframe" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + ".xlsx")

    # Cleaning table
    truncate_table(ws, conx, curs, table_name)

    # Function to execute database work
    # need the duty_key to decide what to do
    # need the SQL statement
    # need the dataframe
    # need to know if store procedure must be run or not (boolean)
    grind_the_file(ws, duty_key, table_name, curs, sqlstatement, df)