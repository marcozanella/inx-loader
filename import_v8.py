from decimal import localcontext
import sys
import os
import getpass
from os import path
import argparse
import pandas as pd
import numpy as np
from time import sleep
import time
import datetime
import pyodbc
import socket

user_name = getpass.getuser()
print('Username: ', user_name)
local_ip = socket.gethostbyname(socket.gethostname())
print('Local IP address: ', local_ip)

 
parser = argparse.ArgumentParser(description='Importing SAP data in SQL database')
parser.add_argument('--sql', action='store', type = int, default = 17, help='SQL Server driver version - can be "17" or "18" - default 17')
parser.add_argument('--ke3',action='store' ,help='KE3 is the Excel file exported from SAP KE30 - Sales Basic')
parser.add_argument('--ke2', action='store', help='KE24 is the Excel file exported from SAP KE24')
parser.add_argument('--zaq',action='store', help='ZAQ is the Excel file exported from SAP ZAQCODMI9')
parser.add_argument('--oo', action='store', help='OO is the Excel file exported from SAP ZSDORDER Open Orders')
parser.add_argument('--oh', action='store', help='OH is the Excel file exported from SAP ZSDORDHIST Open Orders')
parser.add_argument('--oit', action='store', help='OIT is the Excel file exported from SAP FBL5N Open Items')
parser.add_argument('--arr', action='store', help='ARR is the Excel file exported from SAP FBL5N All Items')
parser.add_argument('--prl', action='store', help='PRL is the Excel file exported from SAP XXXXX Customer Prices')
parser.add_argument('--inxeu', action=argparse.BooleanOptionalAction, help='use if you want to target inxeu database, defaults to --no-inxeu')
parser.add_argument('--sp', action=argparse.BooleanOptionalAction, help = 'use if you want store procedures to update live data in Azure SQL; defaults --no-sp')
args = parser.parse_args()
# print(args)

print ('')
if args.ke3 == '' or args.ke3 is None:
    print('ke30 \t\tNOT passed in arguments')
    xl_filename_ke30 = ''
else:
    if 'xlsx' in args.ke3.lower():
        print ('ke30 \t\tpassed in arguments', args.ke3)
        xl_filename_ke30 = args.ke3
    else:
        print ('ke30 \t\tpassed, but not an Excel file')
        xl_filename_ke30 = ''

if args.ke2 == '' or args.ke2 is None:
    print('ke24 \t\tNOT passed in arguments')
    xl_filename_ke24 = ''
else:
   if ('xlsx' in args.ke2.lower()):
      print ('ke24 \t\tpassed in arguments', args.ke2)
      xl_filename_ke24 = args.ke2
   else:
      print ('ke24 \t\tpassed, but not an Excel file')
      xl_filename_ke24 = ''

if args.zaq == '' or args.zaq is None:
    print('zaqcodmi9 \tNOT passed in arguments')
    xl_filename_zaq = ''
else:
    if 'xlsx' in args.zaq.lower():
        print ('zaqcodmi9 \tpassed in arguments', args.zaq)
        xl_filename_zaq = args.zaq
    else:
        print ('zaqcodmi9 \tpassed, but is not an Excel file')
        xl_filename_zaq = ''

if args.oo == '' or args.oo is None:
    print('ZSDORDER \tNOT passed in arguments')
    xl_filename_oo = ''
else:
    if 'xlsx' in args.oo.lower():
        print ('ZSDORDER \tpassed in arguments', args.oo)
        xl_filename_oo = args.oo
    else:
        print ('ZSDORDER \tpassed, but is not an Excel file')
        xl_filename_oo = ''

if args.oh == '' or args.oh is None:
    print('ZSDORDHIST \tNOT passed in arguments')
    xl_filename_oh = ''
else:
    if 'xlsx' in args.oh.lower():
        print ('ZSDORDHIST \tpassed in arguments', args.oh)
        xl_filename_oh = args.oh
    else:
        print ('ZSDORDHIST \tpassed, but is not an Excel file')
        xl_filename_oh = ''

if args.oit == '' or args.oit is None:
    print('Oitems \t\tNOT passed in arguments')
    xl_filename_open = ''
else:
    if 'xlsx' in args.oit.lower():
        print ('Oitems \t\tpassed in arguments', args.oit)
        xl_filename_open = args.oit
    else:
        print ('Oitems \t\tpassed, but is not an Excel file')
        xl_filename_open = ''

if args.arr == '' or args.arr is None:
    print('ARR \t\tNOT passed in arguments')
    xl_filename_arr = ''
else:
    if 'xlsx' in args.arr.lower():
        print('ARR \t\tpassed in arguments', args.arr)
        xl_filename_arr = args.arr
    else:
        print ('ARR \t\tpassed, but is not an Excel file')
        xl_filename_arr = ''

if args.prl == '' or args.prl is None:
    print('PRL \t\tNOT passed in arguments')
    xl_filename_prl = ''
else:
    if 'xlsx' in args.prl.lower():
        print ('PRL \t\tpassed in arguments', args.prl)
        xl_filename_prl = args.prl
    else:
        print ('PRL \t\tpassed, but is not an Excel file')
        xl_filename_prl = ''

if (xl_filename_zaq == '') and (xl_filename_ke24 == '') and (xl_filename_ke30 == '') and (xl_filename_oo == '') and (xl_filename_oh == '') and (xl_filename_open == '') and (xl_filename_arr == '') and (xl_filename_prl == '') and (args.sp == None):
    print('')
    sys.exit('No arguments passed; aborting script\n')

try:
    xl_path_ke30 = str(os.path.dirname(xl_filename_ke30))
    xl_basename_ke30 = str(os.path.basename(xl_filename_ke30))
    xl_path_ke24 = str(os.path.dirname(xl_filename_ke24))
    xl_basename_ke24 = str(os.path.basename(xl_filename_ke24))
    xl_path_zaqcodmi9 = str(os.path.dirname(xl_filename_zaq))
    xl_basename_zaqcodmi9 = str(os.path.basename(xl_filename_zaq))
    xl_path_oo = str(os.path.dirname(xl_filename_oo))
    xl_basename_oo = str(os.path.basename(xl_filename_oo))
    xl_path_oh = str(os.path.dirname(xl_filename_oh))
    xl_basename_oh = str(os.path.basename(xl_filename_oh))
    xl_path_open = str(os.path.dirname(xl_filename_open))
    xl_basename_open = str(os.path.basename(xl_filename_open))
    xl_path_arr = str(os.path.dirname(xl_filename_arr))
    xl_basename_arr = str(os.path.basename(xl_filename_arr))
    xl_path_prl = str(os.path.dirname(xl_filename_prl))
    xl_basename_prl = str(os.path.basename(xl_filename_prl))
    if xl_path_ke30 == '': xl_path_ke30 = './'
    if xl_path_zaqcodmi9 == '': xl_path_zaqcodmi9 = './'
    if xl_path_oo == '': xl_path_oo = './'
    if xl_path_oh == '': xl_path_oh = './'
    if xl_path_open == '': xl_path_open = './'
    if xl_path_arr == '': xl_path_arr = './'
    if xl_path_prl == '': xl_path_prl = './'
    if xl_filename_ke30 != '':
        print ('Importing KE30 file:\t\t', xl_path_ke30, xl_basename_ke30, '\tsize: ', round(os.path.getsize(xl_path_ke30 + xl_basename_ke30)/1000000, 2), 'Mb')
    if xl_filename_ke24 !='':
        print ('Importing KE24 file: \t', xl_path_ke24, xl_basename_ke24, '\tsize: ', round(os.path.getsize(xl_path_ke24 + xl_basename_ke24)/1000000, 2), 'Mb')
    if xl_filename_zaq != '':
        print ('Importing ZACODMI9 file:\t', xl_path_zaqcodmi9, xl_basename_zaqcodmi9)
    if xl_filename_oo != '':
        print ('Importing ZSDORDER file:\t', xl_path_oo, xl_basename_oo)
    if xl_filename_oh != '':
        print ('Importing ZSDORDHIST file:\t', xl_path_oh, xl_basename_oh)
    if xl_filename_open != '':
        print ('Importing FBL5N open item file:\t', xl_path_open, xl_basename_open)
    if xl_filename_arr != '':
        print ('Importing FBL5N arrear file:\t', xl_path_arr, xl_basename_arr)
    if xl_filename_prl != '':
        print ('Importing ZSDCSTPR file:\t', xl_path_prl, xl_basename_prl)
except IndexError as err:
    sys.exit('Something has gone wrong with parameters, aborting script\n')

sizeof_df_KE30_excel = 0
sizeof_df_KE24_excel = 0
sizeof_df_ZAQ_excel = 0
sizeof_df_OO_excel = 0
sizeof_df_OH_excel = 0
sizeof_df_open_excel = 0
sizeof_df_arr_excel = 0
sizeof_df_prl_excel = 0
time_before_cleaning = 0
oggi = datetime.datetime.now()
oggi = oggi.strftime("%Y%m%d-%H%M%S") # 20201120-203456
separator = '-' * 80
print(separator)

if not xl_filename_ke30 == '':
    df_KE30_excel = pd.read_excel(str(xl_filename_ke30))
    sizeof_df_KE30_excel = len(df_KE30_excel)
    print (xl_filename_ke30, ' have been read ', sizeof_df_KE30_excel, ' records')
    # Check currency in KE30 
    if df_KE30_excel['Currency'][0] == 'USD':
        sys.exit('Currency of KE30 is USD; it cannot be imported')
    # Preparing Pandas dataframe
    df_KE30_excel['Period/Year.1'] = '00' + df_KE30_excel['Period/Year.1'].apply(str)
    df_KE30_excel['Period/Year.1'] = df_KE30_excel['Period/Year.1'].str[-7:]
    df_KE30_excel['ImportTimestamp'] = oggi
    df_KE30_excel['ImportUser'] = user_name
    df_KE30_excel['ImportIP'] = local_ip
    df_KE30_excel['YearMonth'] = (df_KE30_excel['Fiscal Year'])*100 + (df_KE30_excel['Period'])
    df_KE30_excel = df_KE30_excel.replace(np.nan, '')
    # df_KE30_excel = df_KE30_excel.drop(df_KE30_excel.columns.difference(['Period/Year', 'Customer', 'Customer.1']), axis=1)
    # df_KE30_excel.to_excel('KE30_dataframe.xlsx')
    
if not xl_filename_ke24=='':
    df_KE24_excel=pd.read_excel(str(xl_filename_ke24))
    sizeof_df_KE24_excel = len(df_KE24_excel)
    print (xl_filename_ke24, ' have been read ', sizeof_df_KE24_excel, ' records')
        
    KE24_fields_dict = {
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
    df_KE24_excel.rename(columns = KE24_fields_dict, inplace=True)
    list_KE24_columns_name = df_KE24_excel.columns.values.tolist()
    split_index = 117
    columns_to_keep = list_KE24_columns_name[:split_index]
    columns_to_drop = list_KE24_columns_name[split_index:]
    # Need to cut dataframe
    if split_index < 117: df_KE24_excel = df_KE24_excel.iloc[:,:len(columns_to_drop)*(-1)]
    df_KE24_excel = df_KE24_excel.replace(np.nan, None)
    df_KE24_excel['YearMonth'] = df_KE24_excel['YearMonth'].astype(str)
    df_KE24_excel['InvoiceDate'] = pd.to_datetime(df_KE24_excel['InvoiceDate'])
    df_KE24_excel['GoodsIssueDate'] = pd.to_datetime(df_KE24_excel['GoodsIssueDate'])
    df_KE24_excel = df_KE24_excel.astype({'Revenue':'float'})
    comma_position = df_KE24_excel['ExchangeRate'][0].find(',')
    df_KE24_excel['ExchangeRate'] = df_KE24_excel['ExchangeRate'][0][0:comma_position]+'.'+df_KE24_excel['ExchangeRate'][0][comma_position+1:]
    # df_KE24_excel.to_excel('KE24_dataframe.xlsx')

if not xl_filename_zaq=='':
    df_ZAQ_excel=pd.read_excel(str(xl_filename_zaq), parse_dates = True)
    df_ZAQ_excel=df_ZAQ_excel.iloc[:-6]
    sizeof_df_ZAQ_excel = len(df_ZAQ_excel)
    df_ZAQ_excel['Billing date'] = pd.to_datetime(df_ZAQ_excel['Billing date'])
    df_ZAQ_excel['ImportDate'] = oggi
    print (xl_filename_zaq, ' have been read , ', sizeof_df_ZAQ_excel, ' records')
    if time_before_cleaning==0: time_before_cleaning = datetime.datetime.now()
    df_ZAQ_excel = df_ZAQ_excel.replace(np.nan, '')
    
if not xl_filename_oo=='':
    # Changing column names
    df_OO_excel=pd.read_excel(str(xl_filename_oo), parse_dates = True)
    df_OO_excel.rename(columns={'Sold-to':'CustomerNumber', 'Ship-to':'Ship_to', 'Customer Name':'CustomerName', 'Cty':'Country', 'Sales Doc#':'SalesOrderNumber',
       'SOStLoc':'StoreLocation', 'Item':'ItemLineNumber', 'Sa Ty':'OrderType', 'Order Date':'SalesOrderDate', 'Req. dt':'RequestedDate',
       'PL. GI Dt':'PartialShipmentDate', 'Days late':'DaysLate', 'Material':'ProductNumber', 'Material Description':'ProductName',
       'Ordered Qty':'QtyOrdered', 'Unit':'QtyOrdered_unit', 'Open Order Qty':'QtyOpen', 'Unit.1':'QtyOpen_unit',
       'GI Qty':'QtyPartialShipped', 'Unit.3':'QtyPartialShipped_unit', 'Cust PO #':'CustomerPONumber', 'Lead time':'LeadTime'}, inplace=True)
    df_OO_excel.drop({'Transit Time', 'Open Del Qty', 'Unit.2', 'In Stock', 'Equipment'}, axis='columns', inplace=True)
    df_OO_excel = df_OO_excel.iloc[:-4]
    df_OO_excel['SalesOrderDate'] = pd.to_datetime(df_OO_excel['SalesOrderDate'])
    df_OO_excel['RequestedDate'] = pd.to_datetime(df_OO_excel['RequestedDate'])
    df_OO_excel['PartialShipmentDate'] = pd.to_datetime(df_OO_excel['PartialShipmentDate'])
    df_OO_excel['ImportDate'] = datetime.datetime.now()
    df_OO_excel['ImportDate'] = '2022-01-28'
    df_OO_excel['Plant'] = df_OO_excel['Plant'].astype(int).astype(str)
    df_OO_excel['SalesOrderNumber'] = df_OO_excel['SalesOrderNumber'].astype(int).astype(str)
    df_OO_excel['LineType'] = 'OO'
    df_OO_excel = df_OO_excel.replace(np.nan, '')
    sizeof_df_OO_excel = len(df_OO_excel)
    df_OO_excel['CustomerNumber'] = df_OO_excel['CustomerNumber'].fillna(df_OO_excel['Ship_to'])
    df_OO_excel['CustomerNumber'] = np.where(df_OO_excel['CustomerNumber'] == '', df_OO_excel['Ship_to'], df_OO_excel['CustomerNumber'])
    df_OO_excel.to_excel('ZSDORDER_dataframe.xlsx')

if not xl_filename_oh=='':
    # Changing column names
    df_OH_excel=pd.read_excel(str(xl_filename_oh), parse_dates = True)
    df_OH_excel.rename(columns={
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
       }, inplace=True)
    df_OH_excel['SalesOrderDate'] = pd.to_datetime(df_OH_excel['SalesOrderDate'])
    df_OH_excel['DeliveryDate'] = pd.to_datetime(df_OH_excel['SalesOrderDate'])
    df_OH_excel['InvoiceDate'] = pd.to_datetime(df_OH_excel['InvoiceDate'])
    df_OH_excel['ImportDate'] = datetime.datetime.now()
    df_OH_excel['SalesOrderNumber'] = df_OH_excel['SalesOrderNumber'].astype(int).astype(str)
    df_OH_excel['LineType'] = 'OH'
    df_OH_excel = df_OH_excel.replace(np.nan, '')
    sizeof_df_OH_excel = len(df_OH_excel)
    df_OH_excel.to_excel('ZSDORDHIST_dataframe.xlsx')

if not xl_filename_open=='':
    df_open_excel=pd.read_excel(str(xl_filename_open), parse_dates = True)
    df_open_excel.rename(columns={'Document Date':'DocumentDate', 'Net due date':'NetDueDate',
       'Arrears after net due date':'Arrears', 'Amount in doc. curr.':'AmountInDocCurr',
       'Document currency':'DocCurr', 'Document Type':'DocType', 'Account':'CustomerNumber', 'Document Number':'DocNumber',
       'Billing Document':'InvoiceNumber', 'Terms of Payment':'PaymentTerms', 'Invoice reference':'InvoiceRef',
       'Payment date':'PaymentDate', 'Debit/Credit ind':'DebCred'}, inplace = True)
    df_open_excel.drop ({'Net due date symbol', 'Arrears for discount 1', 'Baseline Payment Dte', 'Payment Block', 'Due net'}, axis='columns', inplace=True)
    df_open_excel.replace(np.nan, '')
    rows_to_retract = df_open_excel['DocCurr'].nunique()
    df_open_excel=df_open_excel.iloc[:-rows_to_retract]
    sizeof_df_open_excel = len(df_open_excel)
    df_open_excel.astype({'AmountInDocCurr': 'int64'})
    df_open_excel.astype({'CustomerNumber': 'int64'})
    df_open_excel.astype({'DocNumber': 'int64'})
    df_open_excel['PaymentTerms'] = df_open_excel['PaymentTerms'].astype(str)
    df_open_excel['InvoiceRef'] = df_open_excel['InvoiceRef'].astype(str)
    df_open_excel['DebCred'] = df_open_excel['DebCred'].astype(str)
    df_open_excel['InvoiceNumber'] = df_open_excel['InvoiceNumber'].astype(str)

if not xl_filename_arr=='':
    df_arr_excel = pd.read_excel(str(xl_filename_arr), parse_dates=True)
    df_arr_excel.rename (columns={'Document Date':'DocumentDate', 'Net due date':'NetDueDate', 'Arrears after net due date':'Arrears',
        'Amount in doc. curr.':'AmountInDocCurr','Document currency':'DocCurr', 'Document Type':'DocType', 'Account':'CustomerNumber',
        'Document Number':'DocNumber', 'Billing Document':'InvoiceNumber', 'Terms of Payment':'PaymentTerms', 'Invoice reference':'InvoiceRef',
        'Payment date':'PaymentDate', 'Debit/Credit ind':'DebCred'}, inplace=True)
    df_arr_excel.drop ({'Net due date symbol', 'Arrears for discount 1', 'Baseline Payment Dte', 'Payment Block', 'Due net'}, axis='columns', inplace=True)
    df_arr_excel.replace(np.nan, '')
    rows_to_retract = df_arr_excel['DocCurr'].nunique()
    df_arr_excel=df_arr_excel.iloc[:-rows_to_retract]
    sizeof_df_arr_excel = len(df_arr_excel)
    df_arr_excel.astype({'AmountInDocCurr': 'int64'})
    df_arr_excel.astype({'CustomerNumber': 'int64'})
    df_arr_excel.astype({'DocNumber': 'int64'})
    df_arr_excel['PaymentTerms'] = df_arr_excel['PaymentTerms'].astype(str)
    df_arr_excel['InvoiceRef'] = df_arr_excel['InvoiceRef'].astype(str)
    df_arr_excel['DebCred'] = df_arr_excel['DebCred'].astype(str)
    df_arr_excel['InvoiceNumber'] = df_arr_excel['InvoiceNumber'].astype(str)

if not xl_filename_prl=='':
    df_prl_excel=pd.read_excel(str(xl_filename_prl), parse_dates = True)
    df_prl_excel.rename(columns={
        'Customer':'CustomerNumber',
        'Customer Name':'CustomerName',
        'Material':'ProductNumber',
        'Material Description':'ProductName',
        'Scale Qty From': 'Volume_From',
        'Scale Qty To':'Volume_To',
        'Curr':'Currency',
        'Start Date':'Valid_From',
        'End Date':'Valid_To'
       }, inplace=True)
    df_prl_excel.drop({'SOrg', 'Dv', 'CTyp'}, axis='columns', inplace=True)
    df_prl_excel['Valid_From'] = pd.to_datetime(df_prl_excel['Valid_From'])
    df_prl_excel['Valid_To'] = df_prl_excel['Valid_To'].astype(str)
    df_prl_excel['Valid_To'] = df_prl_excel['Valid_To'].str[0:10]
    df_prl_excel['ImportDate'] = datetime.datetime.now()
    df_prl_excel = df_prl_excel.replace(np.nan, '')
    sizeof_df_prl_excel = len(df_prl_excel)
    df_prl_excel.to_excel('ZSDCSTPR_dataframe.xlsx')

print('DATAFRAMES CREATION COMPLETED')
print(separator)

# building SQL statements
if not xl_filename_ke30 == '':
    list_of_fields = ['Period/Year', 'Customer', 'Sales Qty Actual', 'Field Sales Mgr', 'Customer.1', 'Product', 'Product.1', 'Unit Sales quantity', 'Net Sales Actual', 'Rebates Actual', 'Gross Sales Actual', 'RMC Actual', 'Conversion Actual', 'Other Cost Actual', 'Total Costs Actual', 'Gross Margin Actual', 'Margin % Actual', 'Contribution Margin Actual', 'N.Sales/unit Actual', 'Cost/Unit Actual', 'CustomerHierarchy01', 'CustomerHier01', 'Disc/Claims/Adj Actual', 'Material Group', 'Material Group.1', 'Product hierarchy', 'Prod.hierarchy', 'Color', 'Color.1', 'Product Line', 'Product Line.1', 'VP Sales', 'Cust.Acct.Assg.Group', 'AccAssmtGrpCust', 'Profit Center', 'Currency', 'Profit Center.1', 'Unit of Measure', 'Market Segment', 'Market Segment.1', 'Major Label', 'Major Label.1', 'National Account Mgr', 'National Account Mgr.1', 'C.Margin % Actual', 'Fiscal Year', 'Fiscal Year.1', 'Country', 'Country.1', 'Sales employee', 'Sales employee.1', 'Sales district', 'Sales district.1', 'Color Group', 'Color Group.1', 'Material pricing grp', 'Mat.pricing grp', 'Price group', 'Price group.1', 'Industry', 'Industry.1', 'Brand Name', 'Brand Name.1', 'Period', 'Period.1', 'Division',  'Division.1', 'Field Sales Mgr.1', 'Period/Year.1', 'Ship-to party', 'Ship-to party.1', 'ImportTimestamp', 'ImportUser', 'ImportIP', 'YearMonth']
    
    # Workl to insert incrementally fields to check if there is any slowing down the code
    # print(len(list_of_fields))
    marker = 0
    # print(list_of_fields[marker])

    ke30_sql_insert_query = "INSERT INTO KE30_import ("
    ke30_list_of_fields = ['[' + s + ']' for s in df_KE30_excel.columns.to_list()]
    ke30_number_of_fields = len(ke30_list_of_fields)
    ke30_fields = ', '.join(ke30_list_of_fields)
    ke30_list_of_question_marks = '?' * ke30_number_of_fields
    ke30_question_marks = ', '.join(ke30_list_of_question_marks)
    ke30_sql_full_statement = ke30_sql_insert_query + ke30_fields + ') VALUES (' + ke30_question_marks +')'
    
    # print (ke30_sql_full_statement)
    print ('KE30 - SQL statement is built')

if not xl_filename_ke24 =='':
    # columns_to_keep is defined when the dataframe is created
    ke24_sql_full_statement = 'INSERT INTO KE24_import ([' + '], ['.join(columns_to_keep) +']) VALUES (' + '?, ' * len(columns_to_keep)
    ke24_sql_full_statement = ke24_sql_full_statement[0:len(ke24_sql_full_statement) - 2] + ')'
    # print(ke24_sql_full_statement)
    print ('KE24 - SQL statement is built')

if not xl_filename_zaq =='':
    zaq_sql_insert_query = "INSERT INTO ZAQCODMI9_import ("
    zaq_sql_list_of_fields = "[Billing date], [Material], [Description], [Sold to], [Name], [BillingDoc], [Invoice Qty], [UoM], [Unit Price], [Invoice Sales], [Curr.], [Batch], [GM (%)], [Prof], [PTrm], [Curr..1], [Cost], [Can], [Bill], [Item], [Tax amount], [Curr..2], [Dv], [ShPt], [SalesDoc], [ImportDate]"
    zaq_question_marks = "?, " * len(df_ZAQ_excel.columns)
    zaq_question_marks = zaq_question_marks[:len(zaq_question_marks)-2]
    zaq_sql_values_part = ")VALUES(" + zaq_question_marks + ")"
    zaq_sql_full_statement = zaq_sql_insert_query + zaq_sql_list_of_fields + zaq_sql_values_part
    print ('ZAQCODMI9 - SQL statement is built')

if not xl_filename_oo=='':
    OO_sql_insert_query = "INSERT INTO Orders ("
    OO_sql_list_of_fields = "[CustomerNumber], [Ship_to], [CustomerName], [Country], [Plant], [SalesOrderNumber], [StoreLocation], [ItemLineNumber], [OrderType], [SalesOrderDate], [RequestedDate], [PartialShipmentDate],[DaysLate], [ProductNumber], [ProductName], [QtyOrdered], [QtyOrdered_unit], [QtyOpen], [QtyOpen_unit], [QtyPartialShipped], [QtyPartialShipped_unit], [CustomerPONumber], [LeadTime], [ImportDate], [LineType]"
    OO_question_marks = "?, " * len(df_OO_excel.columns)
    OO_question_marks = OO_question_marks[:len(OO_question_marks)-2]
    OO_sql_values_part = ")VALUES(" + OO_question_marks + ")"
    OO_sql_full_statement = OO_sql_insert_query + OO_sql_list_of_fields + OO_sql_values_part
    print ('OO - SQL statement is built')

if not xl_filename_oh=='':
    OH_sql_insert_query = "INSERT INTO Orders ("
    OH_sql_list_of_fields = "[CustomerName], [CustomerNumber], [ProductNumber], [ProductName], [QtyOrdered], [QtyOrdered_unit], [SalesOrderNumber], [ItemLineNumber], [SalesOrderDate], [DeliveryNumber], [DeliveryDate], [InvoiceNumber], [InvoiceDate], [DocumentCurrency], [QtyInvoiced], [QtyInvoiced_unit], [ValueInvoiced], [ImportDate], [LineType]"
    OH_question_marks = "?, " * len(df_OH_excel.columns)
    OH_question_marks = OH_question_marks[:len(OH_question_marks)-2]
    OH_sql_values_part = ")VALUES(" + OH_question_marks + ")"
    OH_sql_full_statement = OH_sql_insert_query + OH_sql_list_of_fields + OH_sql_values_part
    print ('OH - SQL statement is built')

if not xl_filename_open=='':
    open_sql_insert_query = "INSERT INTO FBL5N_open_import ("
    open_sql_list_of_fields = "[DocumentDate], [NetDueDate], [Arrears], [AmountInDocCurr], [DocCurr], [DocType], [CustomerNumber], [DocNumber], [InvoiceNumber], [PaymentTerms], [InvoiceRef], [PaymentDate], [DebCred]"
    open_question_marks = "?, " * len(df_open_excel.columns)
    open_question_marks = open_question_marks[:len(open_question_marks)-2]
    open_sql_values_part = ")VALUES(" + open_question_marks + ")"
    open_sql_full_statement = open_sql_insert_query + open_sql_list_of_fields + open_sql_values_part
    print ('FBL5N_open - SQL statement is built')

if not xl_filename_arr=='':
    arr_sql_insert_query = "INSERT INTO FBL5N_arr_import ("
    arr_sql_list_of_fields = "[DocumentDate], [NetDueDate], [Arrears], [AmountInDocCurr], [DocCurr], [DocType], [CustomerNumber], [DocNumber], [InvoiceNumber], [PaymentTerms], [InvoiceRef], [PaymentDate], [DebCred]"
    arr_question_marks = "?, " * len(df_arr_excel.columns)
    arr_question_marks = arr_question_marks[:len(arr_question_marks)-2]
    arr_sql_values_part = ")VALUES(" + arr_question_marks + ")"
    arr_sql_full_statement = arr_sql_insert_query + arr_sql_list_of_fields + arr_sql_values_part
    print ('FBL5N_arr - SQL statement is built')

if not xl_filename_prl=='':
    prl_sql_insert_query = "INSERT INTO Prices ("
    prl_sql_list_of_fields = "[CustomerNumber], [CustomerName], [ProductNumber], [ProductName], [Volume_From], [Volume_To], [Price], [Currency], [Per], [UoM], [Valid_From], [Valid_To], [ImportDate]"
    prl_question_marks = "?, " * len(df_prl_excel.columns)
    prl_question_marks = prl_question_marks[:len(prl_question_marks)-2]
    prl_sql_values_part = ")VALUES(" + prl_question_marks + ")"
    prl_sql_full_statement = prl_sql_insert_query + prl_sql_list_of_fields + prl_sql_values_part
    print ('ZSDCSTPR - SQL statement is built')


# ***** DATABASE WORK *******
# define DB connection

server = 'inx-eugwc-inxdigital-svr.database.windows.net'
database = 'INXD_Database'
uid = 'INXD_Database_admin'
pwd = 'NX{Pbv2AF;'
driver_version = str(args.sql)

if args.inxeu:
    database = 'INX EU'
    server = 'inxeu.database.windows.net'
    database = 'inxeu_db'
    uid = 'inxeu_admin'
    pwd = '2zs$SgD*D8aNPtr@'

conn_string = 'DRIVER={ODBC Driver ' + driver_version + ' for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + uid + ';PWD=' + pwd + ';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

print(separator)
print(conn_string)
print('Operation wil be performed on database: ', database)
confirmation = input('Do you confirm? (Y to confirm, any other key to cancel) ')
print('\n' + separator)
if confirmation != 'Y':
    sys.exit('Script aborted by user')    

try:
    print('\nConnecting to SQL database server ', server)
    conx = pyodbc.connect(conn_string)
except pyodbc.OperationalError as err:
    print("DB connection could not be established")
    sys.exit('')
# Setting autocommit to false, then requires manual commit
conx.autocommit = False
# define connection cursor
curs = conx.cursor()
print('\tDB connected; cursor created ...')

# Cleaning KE30_import and ZAQCODMI9_import tables
dbtabs = '\t\t\t'
try:
    if not xl_filename_ke30=='':
        # delete_statement = "DELETE FROM KE30_import WHERE ImportUser ='" + user_name +"'"
        delete_statement = "TRUNCATE TABLE KE30_import"
        curs.execute(delete_statement)
        print(dbtabs + 'Truncated table KE30_import')
    if not xl_filename_ke24=='':
        curs.execute('TRUNCATE TABLE KE24_import')
        print(dbtabs + 'Truncated table KE24_import')
    if not xl_filename_zaq=='':
        curs.execute('TRUNCATE TABLE ZAQCODMI9_import')
        print(dbtabs + 'Truncated table ZAQCODMI9_import')
    if not xl_filename_oo=='':
        sql_line = "DELETE FROM Orders WHERE [LineType] = 'OO'"
        curs.execute(sql_line)
        print (dbtabs + 'Removed Open Orders from table Orders')
    if not xl_filename_oh=='':
        sql_line = "DELETE FROM Orders WHERE [LineType] = 'OH'"
        curs.execute(sql_line)
        print (dbtabs + 'Removed Order History from table Orders')
    if not xl_filename_open=='':
        curs.execute('TRUNCATE TABLE FBL5N_open_import')
        print(dbtabs + 'Truncated table FBL5N_open_import')
    if not xl_filename_arr=='':
        curs.execute('TRUNCATE TABLE FBL5N_arr_import')
        print(dbtabs + 'Truncated table FBL5N_arr_import')
    if not xl_filename_prl == '':
        curs.execute('TRUNCATE TABLE Prices')
        print('Trucated table Prices')
except pyodbc.DatabaseError as err:
    print ('\t', err)
    conx.rollback()
else:
    conx.commit()
    print (dbtabs + 'All  import tables have been cleaned in AzureSQL DB\n')

try:
    #curs.fast_executemany = False
    curs.fast_executemany = True
    if not sizeof_df_KE30_excel==0:
        # start = timeit.timeit()
        print ('\texecuting KE30...', end='')
        start_time = time.time()
        curs.executemany(ke30_sql_full_statement, df_KE30_excel.values.tolist())
        end_time=time.time()
        # end = timeit.timeit()
        print('...done in ', end_time - start_time,  end='\n')
    if not sizeof_df_KE24_excel==0:
        print ('\texecuting KE24...', end='')
        curs.executemany(ke24_sql_full_statement, df_KE24_excel.values.tolist())
        print('...done', end='\n')
    if not sizeof_df_ZAQ_excel==0:
        print ('\texecuting ZAQCODMI9...', end='')
        curs.executemany(zaq_sql_full_statement, df_ZAQ_excel.values.tolist())
        print('...done', end='\n')
    if not sizeof_df_open_excel == 0:
        print ('\texecuting OPEN...', end='')
        curs.executemany(open_sql_full_statement, df_open_excel.values.tolist())
        print('...done', end='\n')
    if not sizeof_df_arr_excel == 0:
        print ('\texecuting FBL5N...', end='')
        curs.executemany(arr_sql_full_statement, df_arr_excel.values.tolist())
        print('...done', end='\n')
    if not sizeof_df_OO_excel == 0:
        print('\texecuting OO...', end='')
        curs.executemany (OO_sql_full_statement, df_OO_excel.values.tolist())
        print('...done', end='\n')
    if not sizeof_df_OH_excel == 0:
        print('\texecuting OH...', end='')
        curs.executemany (OH_sql_full_statement, df_OH_excel.values.tolist())
        print('...done', end='\n')
    if not sizeof_df_prl_excel == 0:
        print('\texecuting PRL...', end='')
        curs.executemany (prl_sql_full_statement, df_prl_excel.values.tolist())
        print('...done', end='\n')

except pyodbc.DatabaseError as err:
    print ('\n\n',err)
    conx.rollback()
else:
    conx.commit()
    print ('\n\tCommitted to database')

# run sp, if arg is True
sp_execution_counter = 0
if args.sp:
    print('')
    print('Running Stored Procedures ...')
    if not xl_filename_ke30=='':
        print("Executing spDoKE30Import...", end=" ")
        stored_proc='EXEC spDoKE30Import'
        curs.execute(stored_proc)
        print("done")
        sp_execution_counter += 1
    if not xl_filename_zaq=='':
        print("Executing spDoZAQCODMI9Import...", end=" ")
        curs.execute("spDoZAQCODMI9Import")
        print("done")
        sp_execution_counter += 1
        curs.commit()
        print("Executing spBudForAlignment ... ", end=" ")
        curs.execute("spBudForAlignment")
        print(" done")
        curs.commit()
        print("Executing spBudForDetails_FillSales", end =" ")
        curs.execute("spBudForDetails_FillSales")
        print(" done")
        curs.commit()
    conx.commit()
    if sp_execution_counter == 0:
        print ("no stored procedures executed and committed")
    if sp_execution_counter == 1:
        print ("stored procedures partially executed and committed")
    if sp_execution_counter == 2:
        print ("stored procedures fully executed and committed")
else:
    print(separator)
    print("Store procedures weren't ran, live data have NOT been updated")
    print("Use the --sp optional argument in command line ")
    print("to run store procedures and update live data in AzureSQL")
    print(separator)
