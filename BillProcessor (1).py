#!/usr/bin/env python
# coding: utf-8

import re
import os
import argparse
import tabula
import json
import math
import requests
import pandas as pd
import warnings
from tabulate import tabulate
import warnings
import pdfplumber
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
warnings.filterwarnings("ignore")
BILL_ATTRIBUTE_NUMBER_MAP = {'internet': 7,
                             'sms': 6,
                             'subscription': 4}

class BillProcessor(object):
    def __init__(self, file):
        self.file_path = file
        # Read pdf into list of DataFrame
        self.pdf_pages = tabula.read_pdf(self.file_path, pages="all", stream=True, silent=True)
        self.pdf_pages = self.pdf_pages[4:]
        self.usage_pages = []
        self.all_usage = []
        page = self.pdf_pages[2]
        for page in self.pdf_pages:
            self.usage_pages.append(self.process_page(page))
        for page in self.usage_pages:
            for line in page.values.tolist():
                self.all_usage.append(line)
        self.name, self.invoice_period, self.mobile_no, self.address =\
            self.get_user_info()

    # process pages/tables based on multiple scenarios
    def process_page(self, page):
        page_type = self.get_page_type(page)
        if page_type == 'subscription':
            page = self.process_subscription_table(page)
        if page_type == 'roaming':
            page = page[page.columns.drop(self.columns_with_regex(page, 'Unnamed'))]
        if page_type == 'sms':
            page = page.iloc[1: , :]
        # drops NaN entries picked up by tabula as extra columns under Unnamed

        # processing tables/pages for roaming entries
        if page_type == 'roaming':
            if self.columns_with_regex(page, 'time.*number'):
                left_table, right_table = self.resolve_time_number(page)
        # processing tables/pages for local entries
        else:
            left_table = page.iloc[: , :BILL_ATTRIBUTE_NUMBER_MAP[page_type]]
            right_table = page.iloc[: , BILL_ATTRIBUTE_NUMBER_MAP[page_type]:]
            left_table = left_table.dropna()
            right_table = right_table.dropna()

        self.rename_columns_to_bill_entry(left_table, page_type)
        self.rename_columns_to_bill_entry(right_table, page_type)
        return pd.concat([left_table, right_table], axis=0)

    def get_user_info(self):
        # read pdf using pdfplumber
        with pdfplumber.open(self.file_path) as pdf:
            page= pdf.pages[0]
            text=page.extract_text()
        # get invoice period using the standard text
        for row in text.split('\n'):
            if row.startswith("YOUR TAX INVOICE"):
                invoice_period = row.split()[3:]
                invoice_period[1] = invoice_period[1][2:]
                invoice_period = ' to '.join(invoice_period)
        # get mobile number using standard text
        for row in text.split('\n'):
            if row.startswith("Mobile No"):
                num = row.split()[-1]
                mobile_no = ''.join(num)

        # find the standard text before name and get the next line
        found_name = False
        for row in text.split('\n'):
            # if string before name is not found found_name is set to false,
            # the next line is picked up as name
            if not found_name:
                if row.lower().find('your tax invoice') >= 0:
                    found_name = True
            else:
                name_on_bill = row
                break
        # find the standard text till address and after address(mobile number)
        # process the lines in between to get the address
        found_address = False
        mobile_no_found = False
        address_str_list = []
        for row in text.split('\n'):
            # if the line before address is not found this is set to false
            if not found_address:
                if re.findall(r'\b(\s*)total(\s*)due(\s*)date(\s*)', row, re.IGNORECASE):
                    found_address = True
            # if the line is found rest of the lines till mobile number are appended
            # the mobile number is using the same logic
            else:
                if re.findall(r'\b(\s*)mobile no.(\s*)', row, re.IGNORECASE):
                    mobile_no_found = True
                if not mobile_no_found:
                    address_str_list.append(row)
                else:
                    break
        address = ", ".join(address_str_list[:-1])
        return name_on_bill, invoice_period, mobile_no, address

    def process_subscription_table(self, page):
        # for i in page['Subscription Service']:
        page = page[page['Subscription Service'].notna()]
        page["sr_correction"] = page.apply(lambda x: ' '.join(x['Subscription Service'].split()[:1]), axis=1)
        page["period_correction"] = page.apply(lambda x: ' '.join(x['Subscription Service'].split()[1:]), axis=1)
        page["Service_correction"] = page.apply(lambda x: 'Dailer Tones', axis=1)
        page = pd.concat([page.iloc[1:, 8:11], page.iloc[1:, 2:8]],axis=1)
        page = page.dropna(axis=1, how='all')
        page = page.dropna()
        return page

# utility methods
    def rename_columns_to_bill_entry(self, input_dataframe, service_type):
        if(service_type == "sms"):
            input_dataframe.columns = ['Sr. No.', 'Date', 'Time hh:mm:ss', 'Destination Number', 'Volume', 'SMS Charges']
        if(service_type == "subscription"):
            input_dataframe.columns = ['Sr. No.', 'Period', 'Service Name', 'Charges']
        if(service_type == "internet"):
            input_dataframe.columns = ['Sr. No.', 'Date', 'Time hh:mm:ss', 'APN', 'Rating Group', 'Total Vol/Dur', 'Total Amt.']
        if(service_type == "roaming"):
            input_dataframe.columns = ['Sr. No.', 'Date', 'Destination', 'Time hh:mm:ss', 'Number', 'Type', 'Duration', 'Total']
        return 0

    def add_time_number_columns(self, table):
        for column in self.columns_with_regex(table, 'Time Number'):
            table['Time'] = table.apply(lambda x: x[column].split()[0], axis=1)
            table['Number'] = table.apply(lambda x:  x[column].split()[1], axis=1)
            table = table.drop(column, 1)
            # this is to re-arrange the columns of the table 
            # to facilitate renaming
            table = pd.concat([table.iloc[:, :3], table.iloc[:, 6:8], table.iloc[:, 3:6]],axis=1)
        return table

    def resolve_time_number(self, table):
        # getting the different schemas if time number entry is 
        # confused by tabula
        if len(self.columns_with_regex(table, 'Time Number')) == 1:
            left_table = table.iloc[:, :8]
            right_table = table.iloc[:, 8:]
        if len(self.columns_with_regex(table, 'Time Number')) == 2:
            left_table = table.iloc[:, :7]
            right_table = table.iloc[:, 7:]
        for column in self.columns_with_regex(left_table, 'Time Number'):
            left_table = left_table.dropna()
            left_table = left_table.astype(str)
            left_table['Time'] = left_table.apply(lambda x: x[column].split()[0], axis=1)
            left_table['Number'] = left_table.apply(lambda x:  x[column].split()[1], axis=1)
        for column in self.columns_with_regex(right_table, 'Time Number'):
            right_table = right_table.dropna()
            right_table = right_table.astype(str)
            right_table['Time'] = right_table.apply(lambda x: x[column].split()[0], axis=1)
            right_table['Number'] = right_table.apply(lambda x:  x[column].split()[1], axis=1)

        left_table = left_table.dropna()
        right_table = right_table.dropna()
        left_table = self.add_time_number_columns(left_table)
        right_table = self.add_time_number_columns(right_table)
        # get the time and number entry for the tables having 
        # above mentioned anomaly
        return left_table, right_table

    def get_page_type(self, page):
        col_name_to_type = {'sms': 'sms',
                            'apn': 'internet',
                            'subscription': 'subscription',
                            'number': 'roaming'}
        for i in ['sms', 'apn', 'subscription', 'number']:
            if self.columns_with_regex(page, i):
                return col_name_to_type[i]


    def columns_with_regex(self, df, r_exp):
        return [i for i in list(df.columns) if re.search(f'.*{r_exp}.*', i, re.IGNORECASE)]

    def get_output_json(self):
        out_json = {"Name Of person :" : self.name,
        "Invoice Period : ": self.invoice_period,
        "Mobile No : ": self.mobile_no,
        "Address : ": self.address,
        "Usage": self.all_usage}

        return out_json

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Helper module to '
                                                 'process pdf bills')
    parser.add_argument('-p','--path', help='path to file or '
        'directory containing bill files')
    parser.add_argument('-o','--output-path', help='output path '
        'this should be a directory if the -p/--path is a directory')
    args = parser.parse_args()


    if os.path.isdir(args.path):
        for directory,_,files in os.walk(args.path):
            for file in files:
                in_file = (os.path.join(directory, file))
                file_output = BillProcessor(in_file).get_output_json()
                if args.output_path:
                    file = os.path.join(args.output_path, file)
                else:
                    file = in_file
                with open(f'{file.rstrip(".pdf")}.json', 'w') as outfile:
                    json.dump(file_output, outfile, indent=3)

    elif os.path.isfile(args.path):
        with open(f'{(args.output_path if args.output_path else args.path).rstrip(".pdf")}.json', 'w') as outfile:
            file_output = BillProcessor(args.path).get_output_json()
            json.dump(file_output, outfile, indent=3)
