from rich import print
import json
import scrapy
import pkgutil  
from io import BytesIO
import pandas as pd
# from ..utils import gauth, check_clear, add_rows_1
import requests, asyncio
import gspread
import pkgutil, json
from io import BytesIO
from rich import print
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.service_account import Credentials
import aiohttp
import asyncio
import gspread_asyncio
from scrapy.crawler import CrawlerProcess

def gauth(creds):
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds,SCOPES)
    client = gspread.authorize(credentials)
    return client


def check_clear(acc, client):
    sheet_id = "1Gi-bTqVdQWG1B5bUZvvz2_798a8HHdh6UZ-nujZ2L0I"
    spreadsheet = client.open_by_key(sheet_id)
    try:
        worksheet = spreadsheet.worksheet(acc)
        worksheet.clear()
        print(f"Sheet '{acc}' already exists. and cleared")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=acc, rows="10000", cols="20")
        print(f"Sheet '{acc}' did not exist, so a new one was created.")
    
    headers = ["Membro", "Teléfono", "Suscripción_total"]
    worksheet.insert_row(headers, index=1)  
    # worksheet.format("A1:C1", {"textFormat": {"bold": True}}) 
    worksheet.freeze(rows=1)
    print('Headers are created')
    return spreadsheet


def add_rows(row_dict, worksheet, client, acc):
    headers = ["Membro", "Teléfono", "Suscripción_total"]
    new_row = [''] * 3
    for key, value in row_dict.items():
        if key in headers:
            col_index =  headers.index(key)  # Find the correct column index
            new_row[col_index] = value
    print(new_row)
    accsht = client.open_by_key("1Gi-bTqVdQWG1B5bUZvvz2_798a8HHdh6UZ-nujZ2L0I").worksheet(acc)
    print(accsht)
    accsht.append_row(new_row)
    print(f'{new_row} is added to {worksheet}')
    return None



class MembsSpider(scrapy.Spider):
    name = "membs"
    data = pkgutil.get_data("dgbt","resources/creds.json")
    creds = json.loads(data.decode('utf-8'))
    client = gauth(creds)

    def start_requests(self):
        
        data = pkgutil.get_data("dgbt","resources/accounts.csv")
        df = pd.read_csv(BytesIO((data)))
        
            
        for index, row in df.iterrows():
            account = str(row['account'].strip())
            password = str(row['password'].strip())

            print(index, account, password)
    
            headers = {
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9,ar;q=0.8',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'Origin': 'https://ai.dgpt.club',
                'Referer': 'https://ai.dgpt.club/',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
                'X-Requested-With': 'XMLHttpRequest',
                'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
            }


            
            data_body = {
                'device_id': 'f3037a20-46f6-49fc-be3a-e5f9431b1301',
                'account': account,
                'password': password,
                'isRemember': 'true',
            }
            
            yield scrapy.FormRequest(
                url= 'https://api.dgpt.club/api/login/dologin',
                formdata = data_body,
                headers= headers,
                dont_filter=True,
                meta = {'account':account}
                
            )
            
            
            
    def parse(self, response):
    
            res_body = json.loads(response.text)
            print(res_body)
            account = response.meta['account']
            token = res_body.get('data').get('token')
            worksheet = check_clear(account, self.client)
            membs_url = 'https://api.dgpt.club/api/myteam/mylist?lang=en&v=1.3.5&page=1&grade=&isvalid=&dateline=all&level='
            headers = {
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-Language': 'en-US,en;q=0.9,ar;q=0.8',
                    'Authorization': token,
                    'DNT': '1',
                    'Origin': 'https://ai.dgpt.club',
                    'Referer': 'https://ai.dgpt.club/',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
                    'X-Requested-With': 'XMLHttpRequest',
                        }
            
            
            yield scrapy.Request(
                    url = membs_url,
                    headers= headers,
                    callback=self.parse_list,
                    dont_filter=True,
                    meta = {'headers': headers,'account':account, 'page':1, 'worksheet':worksheet}
                )
                

        
    def parse_list(self, response):
        
        headers = response.meta['headers']
        account = response.meta['account']
        worksheet = response.meta['worksheet']

        page = response.meta['page']
        page = page + 1 
        
        
        jsbdy = json.loads(response.text)
        data = jsbdy.get('data').get('memlist')
        pages =  data.get('last_page')
        print('pages:', pages)
        
        mids = data.get('data')
        for mid in mids:
            mem_id = mid.get('mem_id')
            mem_url = f'https://api.dgpt.club/api/myteam/meminfo?lang=en&v=1.3.5&mem_id={mem_id}'
            yield scrapy.Request (mem_url, callback=self.mem_details, headers= headers, dont_filter=True, meta={'account':account, 'worksheet':worksheet})
            

        if pages > 1 and page <= pages:
            membs_url = f'https://api.dgpt.club/api/myteam/mylist?lang=en&v=1.3.5&page={page}&grade=&isvalid=&dateline=all&level='
            yield scrapy.Request(
                url = membs_url,
                headers= headers,
                dont_filter=True,
                meta = {'headers': headers, 'page': page, 'account':account, 'worksheet':worksheet},
                callback= self.parse_list
            )
            
    
    
    def mem_details(self, response):
        data = json.loads(response.text).get('data')
        name = data.get('account')
        mobile = data.get('mobile')
        total_buy = data.get('total_buy')
        account = response.meta['account']
        worksheet = response.meta['worksheet']

        row_dict={
                'Membro': name,
                'Teléfono': mobile, 
                'Suscripción_total': total_buy,
            }
        print(worksheet)
        add_rows(row_dict, worksheet, self.client, account)

        yield{
            'Membro': name,
            'Teléfono': mobile, 
            'Suscripción_total': total_buy,
            'account': account
        }   
        
        
          
    def parse_sync_wrapper(self, response):
        return  self.loop.run_until_complete(self.mem_details(response))



       
          
process = CrawlerProcess({
    'RETRY_TIMES' : 10,  

    'RETRY_HTTP_CODES' : [500, 502, 503, 504, 522, 524, 408, 400, 401, 402, 403, 404, 405, 406, 429],

    # Crawl  by identifying yourself (and your website) on the user-agent
    'USER_AGENT' : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",

    'ROBOTSTXT_OBEY': False,

    'CONCURRENT_REQUESTS' : 5,
    # CONCURRENT_REQUESTS_PER_DOMAIN : 1,
    # CONCURRENT_REQUESTS_PER_IP : 1,
    
    'AUTOTHROTTLE_ENABLED': True,
    'AUTOTHROTTLE_START_DELAY' : 5,
    'AUTOTHROTTLE_MAX_DELAY' : 20,
    'DOWNLOAD_DELAY' : 3,

    'LOG_LEVEL' : 'DEBUG',
})

process.crawl(MembsSpider)
process.start()