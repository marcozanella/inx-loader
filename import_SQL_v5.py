from decimal import localcontext
from genericpath import isfile
from operator import concat
from re import X
import sys
import readline
import getpass
from os import path
import argparse
from threading import local
from unicodedata import decimal
from zlib import DEF_BUF_SIZE
import pandas as pd
import numpy as np
from time import sleep
import time
from datetime import datetime
import pyodbc
import configparser
import glob
from uuid import getnode as get_mac

#from import_SQL_v1 import truncate_table

user_name = getpass.getuser()
mac_address = get_mac()

separator = "-" * 80
tab = " "

main_menu_data = [
    {'choice': 1, 'request_text': 'select ke30 file ...', 'feedback_text': 'selected file: ', 'selected_text': ''},
    {'choice': 2, 'request_text': 'select ke24 file ...', 'feedback_text': 'selected file: ', 'selected_text': ''},
    {'choice': "d", 'request_text': 'select db ...', 'feedback_text': 'selected database: ', 'selected_text': ''},
    {'choice': "p", 'request_text': 'run stored proc (y/n)', 'feedback_text': 'stored procedures ->  ','selected_text': ''},
    {'choice': "s", 'request_text': 'select SQL driver version (17/18) ...', 'feedback_text': 'selected MSSQL driver version: ', 'selected_text': ''},
    {'choice': "r", 'request_text': 'Run import process with selected values', 'feedback_text': 'no_text', 'selected_text': ''},
    {'choice': "x", 'request_text': 'Exit', 'feedback_text': 'no_text', 'selected_text': ''}
]

# List used to store files found
files_list = []

# -------------------------------------------------
# |0| KE30 file                                   |
# |1| KE24 file                                   |
# |2| database name                               |
# |3| run store proc                              |
# |4| SQL driver version                          |
# |5| run process                                 |
# |6| Exit                                        |
# -------------------------------------------------

def main():
    # get the argument value
    parser = argparse.ArgumentParser(description='Database configuration')
    parser.add_argument('--db', action='store', help='Database to be used [inxeu] - [inxd] - [local]')
    args = parser.parse_args()

    # read configuration file
    config = configparser.ConfigParser()
    config.read('settings.ini')

    # set database details


    choice = 0
    global main_menu_df
    main_menu_df = pd.DataFrame(main_menu_data)


    # debug
    main_menu_df['selected_text'][0] = "./ke30_EU/KE30_EU_8700_9200_202*.XLSX"
    check_file(main_menu_df['selected_text'][0], "ke30")

    main_menu_df['selected_text'][1] = "./ke24_EU/ke24_eu_2021Q*.XLSX"
    check_file(main_menu_df['selected_text'][1], "ke24")

    main_menu_df['selected_text'][2] = "inxeu"
    check_file(main_menu_df['selected_text'][2], "db")

    main_menu_df['selected_text'][3] = "y"
    main_menu_df['selected_text'][4] = "18"
    # debug end

    while choice != "x":
        print()
        print(separator)
        print("Selection Menu", "- user:", user_name, "- ID:", mac_address)
        print(separator)
        for index in range(len(main_menu_df)):
            menu_choice = main_menu_df['choice'][index]
            if main_menu_df['selected_text'][index] == '':
                menu_text = main_menu_df['request_text'][index]
            else:
                menu_text = main_menu_df['feedback_text'][index] + main_menu_df['selected_text'][index]
            print(menu_choice, ')', menu_text)

        choice = input("Your choice--->>> ")

        if choice == "1":
            print()
            input_text = input("enter the full path to ke30 file: ")
            if check_file(input_text, "ke30") == "OK":
                main_menu_df['selected_text'][0] = input_text
            else:
                main_menu_df['selected_text'][0] = ""
        elif choice == "2":
            print()
            input_text = input("enter the full path to ke24 file: ")
            if check_file(input_text, "ke24")== "OK":
                main_menu_df['selected_text'][1] = input_text
            else:
                main_menu_df['selected_text'][1] = ""
        elif choice == "d":
            input_text = input("enter db name (inxeu/inxd/local): ")
            if check_file(input_text, "db") == "OK":
                main_menu_df['selected_text'][2] = input_text
            else:
                main_menu_df['selected_text'][2] = ""
        elif choice == "p":
            input_text = input("run stored procedure (y/n): ")
            if check_file(input_text, "sp") == "OK":
                main_menu_df['selected_text'][3] = input_text
            else:
                main_menu_df['selected_text'][3] = ""
        elif choice == "s":
            input_text = input("enter MSSQL driver version (17/18): ")
            if check_file(input_text, "sql") == "OK":
                main_menu_df['selected_text'][4] = input_text
            else:
                main_menu_df['selected_text'][4] = ""
        elif choice == "r":
            print('Running process ...')
            if main_menu_df['selected_text'][2] == "" or main_menu_df['selected_text'][4] == "":
                print("")
                print("WARNING: Setting SQL driver version and target DB is mandatory")
                # time.sleep(1)
            else:
                run_process(zot=False)
                main_menu_df['selected_text'][0] == ""
                main_menu_df['selected_text'][1] == ""
        elif choice == "zot":
            print("Zotting the DB")
            sys.stdout.flush()
            run_process(zot=True)

        else:
            print("")

def run_process(**kwargs):
    # before anything else let's connect to database
    conn, curs = SQL_connect_cursor(main_menu_df['selected_text'][4], db_server, db_name, db_uid, db_pwd)
    if not conn == False:       # only goes here if connection is established
        if kwargs.get("zot", True):
            #zotta
            print("zotting...")
            sys.stdout.flush()
            tables_to_truncate = (
                'Sales.KE30',
                'KE30_Import',
                'Sales.KE24',
                'KE24_Import',
                'Products',
                'Customers',
                'CustomerHierarchies',
                'Industries',
                'ProductHierarchies',
                'Brands',
                'ProceGroups',
                'MaterialPricingGroups',
                'ProductLines',
                'MaterialGroups',
                'MajorLabels',
                'MarketSegments',
                'Divisions',
                'PriceGroups'
            )
            for table_to_truncate in tables_to_truncate:
                print("zotting", table_to_truncate, "...", end="")
                sys.stdout.flush()
                curs.execute("sp_truncate_non_empty_table @TableToTruncate = ?", table_to_truncate)
                print("done")
                sys.stdout.flush()
            return
        list_of_duties ={}
        if main_menu_df['selected_text'][0] != "":
            list_of_duties["ke30"] = main_menu_df['selected_text'][0]
        if main_menu_df['selected_text'][1] != "":
            list_of_duties["ke24"] = main_menu_df['selected_text'][1]
        if len(list_of_duties) < 1:
            print ("Nothing to do")
            return False
        for duty_key, duty in list_of_duties.items():
            if "*" in duty:
                # it's a wildcard pattern
                #for loop
                #fill the list of files
                files_list = make_list_of_files(duty)
                for file in files_list:
                    grind_the_file(file, duty_key, curs, conn, main_menu_df['selected_text'][3])
            else:
                grind_the_file(duty, duty_key, curs, conn, main_menu_df['selected_text'][3])
    else:
        print("Connection with DB cannot be established")

def import_df(file_path, df_type, output_excel):
    if not file_path == '':
        # dataframe specific ops
        print("Importing dataframe(s)...", file_path)
        match df_type:
            case 'ke30':
                # read the Excel file
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
                    'Ship-to party': str,
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
                df = read_the_file(file_path, converters_dict)

                # Check currency in KE30
                if df['Currency'][0] == 'USD':
                    sys.exit('Currency of KE30 is USD; it cannot be imported')
                # Preparing Pandas dataframe
                df = df.replace(np.nan, '')
                df['Profit Center'] = df['Profit Center'].str[-4:]
                # df['Period/Year.1'] = '00' + df['Period/Year.1'].apply(str)
                df['Period/Year.1'] = '00' + df['Period/Year.1']
                df['Period/Year.1'] = df['Period/Year.1'].str[-6:]
                df['ImportTimestamp'] = datetime.now().strftime("%Y%m%d-%H%M%S")  # 20201120-203456
                df['ImportUser'] = user_name
                df['Import_mac'] = mac_address
                df['YearMonth'] = df['Fiscal Year'] + df['Period'].apply(lambda x: x.zfill(2))

                if output_excel:
                    df.to_excel(df_type + '_dataframe.xlsx')
            case 'ke24':
                converters_dict = {
                    'Currency': str,
                    'Currency type': str,
                    'Record Type': str,
                    'Period/Year': str,
                    'Document number': str,
                    'Item number': str,
                    'Reference document': str,
                    'Reference item no.': str,
                    'Created By': str,
                    'Company Code': str,
                    'Sender cost center': str,
                    'Cost Element': str,
                    'Currency key': str,
                    'Unit Sales quantity': str,
                    'Week/Year': str,
                    'Product': str,
                    'Industry Code 1': str,
                    'Industry': str,
                    'Sales district': str,
                    'Reference Org Unit': str,
                    'Log. system source': str,
                    'Reference Transact.': str,
                    'Point of valuation': str,
                    'Billing Type': str,
                    'Business Area': str,
                    'CustomerHierarchy01': str,
                    'CustomerHierarchy02': str,
                    'CustomerHierarchy03': str,
                    'CustomerHierarchy04': str,
                    'CustomerHierarchy05': str,
                    'Origin': str,
                    'Sales Order': str,
                    'Customer group': str,
                    'Customer': str,
                    'Controlling Area': str,
                    'Price group': str,
                    'Material Ppricing grp': str,
                    'Cost Object': str,
                    'Cust.Acct.Assg.Group': str,
                    'Shi-to party': str,
                    'Country': str,
                    'Client': str,
                    'Material Group': str,
                    'Market Segment': str,
                    'Color': str,
                    'Major Label': str,
                    'Brand Name': str,
                    'Color Group': str,
                    'Profitab.Segmt No.': str,
                    'Partner prof.segment': str,
                    'Partner subnumber': str,
                    'Subnumber': str,
                    'Partner Profit Ctr': str,
                    'Dye Ink': str,
                    'Profit Center': str,
                    'Product hierarchy': str,
                    'Sender bus.process': str,
                    'WBS Element': str,
                    'Currency of record': str,
                    'Order': str,
                    'Update status': str,
                    'Division': str,
                    'Canceled document': str,
                    'Canceled doc. item': str,
                    'Time created': str,
                    'Version': str,
                    'Sales Organization': str,
                    'Sales employee': str,
                    'Distribution Channel': str,
                    'Plant': str,
                    'National Account Mgr': str,
                    'Product Line': str,
                    'VP Sales': str,
                    'Prod Line Sls Mgr': str,
                    'Field Sales Mgr': str,

                    ' Fiscal Year': int,
                    'Hierarchy Assignment': int,
                    'Sales Order Item': int,
                    'Period': int,
                    'Plan/Act. Indicator': int,

                    # 'Created On': datetime,
                    # 'Posting date': date,
                    # 'Invoice date': date,
                    # 'Date': date,
                    # 'Goods Issue Date': date,

                    # 'Time': time,

                    'SalesQuantity': float,
                    'Revenue': float,
                    'AnnualRebates': float,
                    'ExchangeRate': float,
                    'QuantityDiscount': float,
                    'CostOfSales': float,
                    'Inplant_Depreciation': float,
                    'FreightCharges': float,
                    'MTS_InputVar': float,
                    'MTS_InputPriveVar': float,
                    'MTS_LotsizeVar': float,
                    'MTO_FixFreightCost': float,
                    'MTO_FixMaterialCost': float,
                    'MTO_VariableMaterialCost': float,
                    'MTO_FixOverheadCost': float,
                    'MTO_VariableOverheadCost': float,
                    'MTO_FixProductionCost': float,
                    'MTS_OutputPriceVar': float,
                    'MTO_VariableProductionCost': float,
                    'Inplant_OtherExpenses': float,
                    'Inplant_Payroll': float,
                    'MTS_QuantityVar': float,
                    'MTS_RemainingVar': float,
                    'MTS_ResUsageVar': float,
                    'MTS_FixFreightCost': float,
                    'MTS_FixMaterialCost': float,
                    'MTS_VariableMaterialCost': float,
                    'MTS_FixOverheadCost': float,
                    'MTS_VarialbleOverheadCost': float,
                    'MTS_FixProductionCost': float,
                    'MTS_VariableProductionCost': float
                }
                df = read_the_file(file_path, converters_dict)
                ke24_fields_dict = {
                    'Currency type': 'CurrencyType',
                    'Record Type': 'RecordType',
                    'Period/Year': 'YearMonth',
                    'Document number': 'DocumentNumber',
                    'Item number': 'ItemNumber',
                    'Created On': 'CreatedOn',
                    'Reference document': 'ReferenceDocument',
                    'Reference item no.': 'ReferenceItemNo',
                    'Created By': 'CreatedBy',
                    'Company Code': 'CompanyCode',
                    'Sender cost center': 'senderCostCenter',
                    'Cost Element': 'CostElement',
                    'Currency key': 'CurrencyKey',
                    'Sales quantity': 'SalesQuantity',
                    'Unit Sales quantity': 'UnitSalesQuantity',
                    'Week/year': 'YearWeek',
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
                    'Update status': 'UpdateStatus',
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
                df.rename(columns=ke24_fields_dict, inplace=True)
                df = df.replace(np.nan, None)
                df['CreatedOn'] = df['CreatedOn'].apply(lambda x: x.date())
                df['PostingDate'] = df['PostingDate'].apply(lambda x: x.date())
                df['Date'] = df['Date'].apply(lambda x: x.date())
                df['YearMonth'] = df['YearMonth'].apply(lambda x: x[-4:] + x[:-4].zfill(2))
                df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
                df['GoodsIssueDate'] = pd.to_datetime(df['GoodsIssueDate'])
                if output_excel == True:
                    print("output file", df_type)
                    df.to_excel(df_type + '_dataframe.xlsx')
            case 'zaq':
                df = df.iloc[:-6]
                df.rename(columns={"Sales doc.": "SalesDoc"}, inplace=True)
                df['Billing date'] = pd.to_datetime(df['Billing date'])
                df['ImportDate'] = datetime.now().strftime("%Y%m%d-%H%M%S")
                df = df.replace(np.nan, '')
                if output_excel:
                    print("output file", df_type)
                    df.to_excel(df_type + '_dataframe.xlsx')
            case 'oo':
                converters_dict = {}
                df = read_the_file(file_path, converters_dict)
                oo_fields_dict = {
                    'Sold-to': 'CustomerNumber',
                    'Ship-to': 'Ship_to',
                    'Customer Name': 'CustomerName',
                    'Cty': 'Country',
                    'Sales Doc#': 'SalesOrderNumber',
                    'SOStLoc': 'StoreLocation',
                    'Item': 'ItemLineNumber',
                    'Sa Ty': 'OrderType',
                    'Order Date': 'SalesOrderDate',
                    'Req. dt': 'RequestedDate',
                    'PL. GI Dt': 'PartialShipmentDate',
                    'Days late': 'DaysLate',
                    'Material': 'ProductNumber',
                    'Material Description': 'ProductName',
                    'Ordered Qty': 'QtyOrdered',
                    'Unit': 'QtyOrdered_unit',
                    'Open Order Qty': 'QtyOpen',
                    'Unit.1': 'QtyOpen_unit',
                    'GI Qty': 'QtyPartialShipped',
                    'Unit.3': 'QtyPartialShipped_unit',
                    'Cust PO #': 'CustomerPONumber',
                    'Lead time': 'LeadTime'
                }
                df.rename(columns=oo_fields_dict, inplace=True)
                df.drop({'Transit Time', 'Open Del Qty', 'Unit.2', 'In Stock', 'Equipment'}, axis='columns',
                        inplace=True)
                df.drop(df.tail(4).index, inplace=True)
                df['SalesOrderDate'] = pd.to_datetime(df['SalesOrderDate'])
                df['RequestedDate'] = pd.to_datetime(df['RequestedDate'])
                df['PartialShipmentDate'] = pd.to_datetime(df['PartialShipmentDate'])
                df['ImportDate'] = datetime.now()
                df['Plant'] = df['Plant'].astype(int).astype(str)
                df['SalesOrderNumber'] = df['SalesOrderNumber'].astype(int).astype(str)
                df['LineType'] = 'OO'
                df = df.replace(np.nan, '')
                df['CustomerNumber'] = np.where(df['CustomerNumber'] == '', df['Ship_to'], df['CustomerNumber'])
                if output_excel == True: df.to_excel(df_type + '_dataframe.xlsx')
            case 'oh':
                converters_dict = {}
                df = read_the_file(file_path, converters_dict)
                oh_fields_dict = {
                    'Cust #': 'CustomerNumber',
                    'Customer Name': 'CustomerName',
                    'Material #': 'ProductNumber',
                    'Material Description': 'ProductName',
                    'Order #': 'SalesOrderNumber',
                    'Sales Document Item': 'ItemLineNumber',
                    'Order Date': 'SalesOrderDate',
                    'Order Qty': 'QtyOrdered',
                    'UoM': 'QtyOrdered_unit',
                    'Delivery #': 'DeliveryNumber',
                    'Post GI Date': 'DeliveryDate',
                    'Invoice #': 'InvoiceNumber',
                    'Invoice Date': 'InvoiceDate',
                    'Document Currency': 'DocumentCurrency',
                    'Inv Qty': 'QtyInvoiced',
                    'UoM.1': 'QtyInvoiced_unit',
                    'Net value': 'ValueInvoiced'
                }
                df.rename(columns=oh_fields_dict, inplace=True)
                df['SalesOrderDate'] = pd.to_datetime(df['SalesOrderDate'])
                df['DeliveryDate'] = pd.to_datetime(df['SalesOrderDate'])
                df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
                df['ImportDate'] = datetime.now()
                df['SalesOrderNumber'] = df['SalesOrderNumber'].astype(int).astype(str)
                df['LineType'] = 'OH'
                df = df.replace(np.nan, '')
                if output_excel == True: df.to_excel(df_type + '_dataframe.xlsx')
            case 'oit':
                converters_dict = {}
                df = read_the_file(file_path, converters_dict)
                oit_fields_dict = {
                    'Document Date': 'DocumentDate',
                    'Net due date': 'NetDueDate',
                    'Arrears after net due date': 'Arrears',
                    'Amount in doc. curr.': 'AmountInDocCurr',
                    'Document currency': 'DocCurr',
                    'Document Type': 'DocType',
                    'Account': 'CustomerNumber',
                    'Document Number': 'DocNumber',
                    'Billing Document': 'InvoiceNumber',
                    'Terms of Payment': 'PaymentTerms',
                    'Invoice reference': 'InvoiceRef',
                    'Payment date': 'PaymentDate',
                    'Debit/Credit ind': 'DebCred'
                }
                df.rename(columns=oit_fields_dict, inplace=True)
                df.drop({'Net due date symbol', 'Arrears for discount 1', 'Baseline Payment Dte', 'Payment Block',
                         'Due net'}, axis='columns', inplace=True)
                df.replace(np.nan, '')
                rows_to_retract = df['DocCurr'].nunique()
                df = df.iloc[:-rows_to_retract]
                df.astype({'AmountInDocCurr': 'int64'})
                df.astype({'CustomerNumber': 'int64'})
                df.astype({'DocNumber': 'int64'})
                df['PaymentTerms'] = df['PaymentTerms'].astype(str)
                df['InvoiceRef'] = df['InvoiceRef'].astype(str)
                df['DebCred'] = df['DebCred'].astype(str)
                df['InvoiceNumber'] = df['InvoiceNumber'].astype(str)
                if output_excel == True: df.to_excel(df_type + '_dataframe.xlsx')
            case 'arr':
                converters_dict = {}
                df = read_the_file(file_path, converters_dict)
                arr_fields_dict = {
                    'Document Date': 'DocumentDate',
                    'Net due date': 'NetDueDate',
                    'Arrears after net due date': 'Arrears',
                    'Amount in doc. curr.': 'AmountInDocCurr',
                    'Document currency': 'DocCurr',
                    'Document Type': 'DocType',
                    'Account': 'CustomerNumber',
                    'Document Number': 'DocNumber',
                    'Billing Document': 'InvoiceNumber',
                    'Terms of Payment': 'PaymentTerms',
                    'Invoice reference': 'InvoiceRef',
                    'Payment date': 'PaymentDate',
                    'Debit/Credit ind': 'DebCred'
                }
                df.rename(columns=arr_fields_dict, inplace=True)
                df.drop({'Net due date symbol', 'Arrears for discount 1', 'Baseline Payment Dte', 'Payment Block', 'Due net'}, axis='columns', inplace=True)
                df.replace(np.nan, '')
                rows_to_retract = df['DocCurr'].nunique()
                df = df.iloc[:-rows_to_retract]
                df.astype({'AmountInDocCurr': 'int64'})
                df.astype({'CustomerNumber': 'int64'})
                df.astype({'DocNumber': 'int64'})
                df['PaymentTerms'] = df['PaymentTerms'].astype(str)
                df['InvoiceRef'] = df['InvoiceRef'].astype(str)
                df['DebCred'] = df['DebCred'].astype(str)
                df['InvoiceNumber'] = df['InvoiceNumber'].astype(str)
                if output_excel == True: df.to_excel(df_type + '_dataframe.xlsx')
            case 'prl':
                converters_dict = {}
                df = read_the_file(file_path, converters_dict)
                prl_fields_dict = {
                    'Customer': 'CustomerNumber',
                    'Customer Name': 'CustomerName',
                    'Material': 'ProductNumber',
                    'Material Description': 'ProductName',
                    'Scale Qty From': 'Volume_From',
                    'Scale Qty To': 'Volume_To',
                    'Curr': 'Currency',
                    'Start Date': 'Valid_From',
                    'End Date': 'Valid_To'
                }
                df.rename(columns=prl_fields_dict, inplace=True)
                df.drop({'SOrg', 'Dv', 'CTyp'}, axis='columns', inplace=True)
                df['Valid_From'] = pd.to_datetime(df['Valid_From'])
                df['Valid_To'] = df['Valid_To'].astype(str)
                df['Valid_To'] = df['Valid_To'].str[0:10]
                df['ImportDate'] = datetime.now()
                df = df.replace(np.nan, '')
                if output_excel == True: df.to_excel(df_type + '_dataframe.xlsx')
        return df

def SQL_statment_maker(table_name, dataframe, final_statement, **kwarg):
    insert_part = "INSERT INTO " + table_name + " ("
    list_of_fields_from_df = ['[' + s + ']' for s in dataframe.columns.to_list()]
    number_of_fields = len(list_of_fields_from_df)
    fields = ', '.join(list_of_fields_from_df)
    list_of_question_marks = '?' * number_of_fields
    question_marks = ', '.join(list_of_question_marks)
    sql_full_statement = insert_part + fields + ') VALUES (' + question_marks + ')'
    if kwarg.get('one_liner'):
        print(final_statement, end="")
    else:
        print(final_statement)
    return sql_full_statement

def check_file(file_path, type):
    # special cases: database and sql drivers
    global db_server
    global db_name
    global db_uid
    global db_pwd

    if type == "db":
        if file_path == "inxd":
            db_server = "inx-eugwc-inxdigital-svr.database.windows.net"
            db_name = "INXD_Database"
            db_uid = "INXD_Database_admin"
            db_pwd = "NX{Pbv2AF;"
            return "OK"
        elif file_path == "inxeu":
            db_server = "inxeu.database.windows.net"
            db_name = "inxeu_db"
            db_uid = "inxeu_admin"
            db_pwd = "2zs$SgD*D8aNPtr@"
            return "OK"
        elif file_path == "local":
            db_server = 'localhost'
            db_name = 'inxeu_db_local'
            db_uid = 'sa'
            db_pwd = 'dellaBiella2!'
            return "OK"
        else:
            return "NOK"
    if type == "sql":
        if file_path == "17" or file_path == "18":
            return "OK"
        else:
            return "NOK"

    if type == "sp":
        if file_path == "y" or file_path == "n":
            return "OK"
        else:
            return "NOK"

    if ("*" in file_path):
        # It's a pattern with wildcards - get the list of files
        files_list = glob.glob(file_path)
        if len(files_list) > 0:
            print ("found", len(files_list), "files")
            #check and retain only good Excel files
            for file in files_list:
                if (".XLSX" not in file.upper()): files_list.remove(file)
            print (len(files_list), "Excel files")
            if len(files_list) == 0: return "NOK"
        else:
            print("no files found")
            return"NOK"
        for file in files_list:
            print(file)
        # print (files_list)
        return "OK"
    elif path.isfile(file_path) and ".XLSX" in file_path.upper():
        print("file", file_path, "...OK")
        return "OK"
    else:
        return "NOK"

def SQL_connect_cursor(sql_drv_ver, srv, db, user, password):
    try:
        print('\nConnecting to SQL database server ', srv, end = "...")
        conn_string = 'DRIVER={ODBC Driver ' + str(
            sql_drv_ver) + ' for SQL Server};SERVER=' + srv + ';DATABASE=' + db + ';UID=' + user + ';PWD=' + password + ';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
        if srv == "localhost":
            conn_string = 'DRIVER={ODBC Driver ' + str(
                sql_drv_ver) + ' for SQL Server};SERVER=' + srv + ';DATABASE=' + db + ';UID=' + user + ';PWD=' + password + ';TrustServerCertificate=yes;Connection Timeout=30;'
        conx = pyodbc.connect(conn_string)
    except pyodbc.OperationalError as err:
        print("DB connection could not be established")
        conx = False
        curs = False
        # sys.exit(err)
    except pyodbc.ProgrammingError as ex:
        print (ex.args[1])
        conx = False
        curs = False
        # sys.exit(ex)
    except Exception as ex:
        sys.exit(ex)

    # define connection cursor
    if not conx == False:       # When it's False, connection is not established
        # Setting autocommit to false, then requires manual commit
        conx.autocommit = False
        curs = conx.cursor()
    print("done")
    return conx, curs

def truncate_table(table_name, db_cursor, **kwarg):
    line_type = kwarg.get("line_type", None)
    column_name_for_deletion = kwarg.get("col_name_4_deletion", None)
    if line_type == "oo" or line_type == "oh":
        print()
        sql_line = "DELETE FROM " + table_name + " WHERE [" + column_name_for_deletion + "] = '" + line_type + "'"
        db_cursor.execute(sql_line)
        print(tab * 2 + "Removed ", line_type, " from table ", table_name)
    else:
        print()
        delete_statement = "TRUNCATE TABLE " + table_name
        db_cursor.execute(delete_statement)
        print(tab * 2 + 'Truncated table ', table_name, "...")

def write_SQL_table(connection, cursor, sql_statement, dataframe, table, **kwarg):
    table_name = kwarg.get("table_name", None)
    time_start_of_operation = time.time()
    try:
        length = len(dataframe)
        if not length == 0:
            if length > 20000:
                chunk_size = 5000
            elif length > 15000:
                chunk_size = 3500
            elif length > 9999:
                chunk_size = 2000
            elif length > 7499:
                chunk_size = 1500
            elif length > 4999:
                chunk_size = 999
            else:
                chunk_size = 500

            iterations = int(len(dataframe) / chunk_size) + 1
            print(tab * 1, "executing", table, "in", iterations + 1, "rounds...")
            sys.stdout.flush()
            for iteration in range(iterations):
                print(tab * 1, "round", iteration, "-", end="")
                upper_limit = chunk_size * iteration
                lower_limit = upper_limit + chunk_size - 1
                if iteration == iterations - 1:
                    lower_limit = len(dataframe) - 1
                print("", upper_limit, "-", lower_limit, end="...")
                sys.stdout.flush()
                chunk_df = dataframe.iloc[upper_limit:lower_limit]
                # Exporting is time intensive, avoid if not for debugging
                # chunk_df.to_excel("ke30_chunk_" + str(upper_limit) + "_to_" + str(lower_limit) + ".xlsx")
                cursor.fast_executemany = True
                start_time = time.time()

                ########################################################################
                # Inserts fields one at the time to check if there's one that is
                # slower than others - check keyword argument passed and if true, it
                # process one field at the time
                ########################################################################
                if kwarg.get("one_liner", False):
                    df_temporary = chunk_df
                    fields_and_time_dict = {}
                    ugodf=pd.DataFrame()
                    for col in range(len(df_temporary.columns)):
                        cursor.execute("DELETE FROM " + table_name)
                        df_temporary = chunk_df.iloc[:,col:col+1]
                        col_name = df_temporary.columns[0]
                        sql_statement_per_rows = SQL_statment_maker(table_name, df_temporary, "--> SQL stmt done 4 col :" + str(col+1) + " " + col_name, one_liner = True)
                        if df_temporary[col_name].dtype == '<M8[ns]':
                            df_temporary[col_name] = df_temporary[col_name].apply(lambda x: x.date())

                        time_at_start = time.time()
                        cursor.executemany(sql_statement_per_rows, df_temporary.values.tolist())
                        time_at_finish = time.time()
                        time_lapsed = time_at_finish - time_at_start
                        fields_and_time_dict[col_name] = time_lapsed
                        print(4*tab, "time lasped: {:0.2f}".format(time_lapsed), "sec.")
                    ugodf.append(fields_and_time_dict, ignore_index=True)
                    # ugodf.join(fields_and_time_dict)

                ##########################################################################
                else:
                    cursor.executemany(sql_statement, chunk_df.values.tolist())
                end_time = time.time()
                duration = end_time - start_time
                records = lower_limit - upper_limit
                print(tab, "done in", round(duration, 2), "sec", round(records / duration, 2), "rec/sec")
            if kwarg.get("one_liner", False):
                ugodf.to_excel(table + "_fields_and_time.xlsx")
                print ("files with time is published")
        time_end_of_operation = time.time()
        duration_of_operation_str = str(round(time_end_of_operation - time_start_of_operation, 2))
        throughput_of_operation = str(round(length / (round(time_end_of_operation - time_start_of_operation, 2)), 2))
    except pyodbc.DatabaseError as db_err:
        print("db error\n", db_err)
        sys.exit("Script interrupted")
    else:
        connection.commit()
        sys.stdout.flush()
        final_message = tab + table + ' committed to DB - '
        if table_name != '' or table_name != None:
            final_message = final_message + ' pushed in SQL table ' + table_name + "\n"
            final_message += tab + "total time: " + duration_of_operation_str + " sec - " + throughput_of_operation + " records per sec.\n"
        print(final_message)
        sys.stdout.flush()

def read_the_file(file_path, dict_of_converters):
    df_local = pd.read_excel(str(file_path), thousands='.', decimal=',', converters=dict_of_converters)
    df_size = len(df_local)
    print(file_path, tab, df_size, 'records have been read')
    return df_local

def make_list_of_files(pattern):
    files_list = glob.glob(pattern)
    files_list = sorted(files_list,reverse=True, key=str.casefold)
    if len(files_list) > 0:
        #check and retain only good Excel files
        for file in files_list:
            if (".XLSX" not in file.upper()):
                files_list.remove(file)
        if len(files_list) == 0:
            return False
    else:
        return False
    return files_list

def grind_the_file(this_file, file_type, cursor, connection, enable_stored_proc):

    the_dataframe = import_df(this_file, file_type, False)
    the_sql_statement = SQL_statment_maker(file_type + "_import", the_dataframe, file_type + " - sql statement built")
    try:
        truncate_table(file_type.upper() + "_Import", cursor)
    except pyodbc.DatabaseError as err:
        print(err)
        connection.rollback()
    else:
        connection.commit()
    write_SQL_table(connection, cursor, the_sql_statement, the_dataframe, file_type.upper(), table_name= file_type.upper() + "_import", one_liner = False)

    if enable_stored_proc == "y":
    # if store procedure were requested
        print("Running stored procedures")
        sp_names = []
        if (main_menu_df['selected_text'][0] != "") and file_type.upper() == "KE30":
            sp_names.append("spDoKE30Import")
            sp_names.append("spCustomers_01_CustHier")
            sp_names.append("spCustomers_02_Industries")
            sp_names.append("spCustomers_03_SalesEmployees")
            sp_names.append("spCustomers_04_AddCustomers")
            sp_names.append("spProducts_01_AddMajorLabels")
            sp_names.append("spProducts_02_AddBrands")
            sp_names.append("spProducts_03_AddProducthierarchies")
            sp_names.append("spProducts_04_AddMarketSegments")
            sp_names.append("spProducts_05_AddDivisions")
            sp_names.append("spProducts_06_AddProductLines")
            sp_names.append("spProducts_07_AddMaterialGroups")
            sp_names.append("spProducts_08_AddProducts")
        if main_menu_df['selected_text'][1] != "" and file_type.upper() == "KE24":
            sp_names.append("spDoKE24Import")
        #Execution of store procedure listed
        for sp in sp_names:
            print(tab*1, "->", sp, "...", end = "")
            sys.stdout.flush()
            cursor.execute(sp)
            sys.stdout.flush()
            cursor.commit()
            print("done")
        print()

if __name__ == '__main__':
    main()
