from rich import print
import scrapy
from io import BytesIO
import pandas as pd
import gspread
import pkgutil, json
from oauth2client.service_account import ServiceAccountCredentials
from scrapy.crawler import CrawlerProcess

def gauth(creds):
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds,SCOPES)
    client = gspread.authorize(credentials)
    return client


def check_clear(acc, client):
    sheet_id = "1Gi-bTqVdQWG1B5bUZvvz2_798a8HHdh6UZ-nujZ2L0I"
    spreadsheet = client.open_by_key(sheet_id)
    print(spreadsheet)
    try:
        worksheet = spreadsheet.worksheet(acc)
        worksheet.clear()
        print(f"Sheet '{acc}' already exists. and cleared")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=acc, rows="10000", cols="20")
        print(f"Sheet '{acc}' did not exist, so a new one was created.")
    
    headers = ["Membro", "Teléfono", "Suscripción_total", 'UID', 'Hora_de_registro', 'Número_total_de_equipos',  'Superior', 'Ingresos_totales', 'grade', 'email']
    worksheet.insert_row(headers, index=1)  
    # worksheet.format("A1:C1", {"textFormat": {"bold": True}}) 
    worksheet.freeze(rows=1)
    print('Headers are created')
    return spreadsheet


def add_rows(row_dict, worksheet, client, acc):
    headers =  ["Membro", "Teléfono", "Suscripción_total", 'UID', 'Hora_de_registro', 'Número_total_de_equipos',  'Superior', 'Ingresos_totales', 'grade', 'email']
    new_row = [''] * len(headers)
    for key, value in row_dict.items():
        if key in headers:
            col_index =  headers.index(key)  # Find the correct column index
            new_row[col_index] = value
    print(new_row)
    accsht = client.open_by_key("1Gi-bTqVdQWG1B5bUZvvz2_798a8HHdh6UZ-nujZ2L0I").worksheet(acc)
    accsht.append_row(new_row)
    print(f'{new_row} is added to {worksheet}')
    return None



class MembsSpider(scrapy.Spider):
    name = "membs"
    # data = pkgutil.get_data("dgbt_main","dgbt/resources/creds.json")
    # creds = json.loads(data.decode('utf-8'))
    creds = {
        "type": "service_account",
        "project_id": "accountssheets-438006",
        "private_key_id": "43368ee81614933c3d37099c15c8c6c641054381",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDU/i+tBFgr6a8h\nvZeBZLwdRoGUTqOmE2SvUZv3wkgOv++JcTSx7ofZX0w+tFu9MYnlL6AghOBFqnn6\nv+Z5V1aliYkli4YpqceiWltetSFUH5WXbn4kJrfKh2aXkvpU2C3qUQtkFfsQWEsV\niPr26dHR03BBv5mjLEc5GigjZFafK1kb2hm6T6GxyoiySQjVnCwX0QLnb+LR2Cqr\n9LBxQrfaBNVzjx5n3Dx5u1cpYLe1mqF2MElWJA+20mpq/yeH82A/CI3tRMYgg46Z\nQ6gu99d6VoABFZVP/ZR9hBu0nGMlsFIN+1ziTjMSW9Gluu13jpsF4uKRjmeXy08P\nTwu2PsVDAgMBAAECggEAUv/Mj1USkOYq33CUshERtyzLAQKWBfknsqQQLb+xOcI0\ns3AAc1f660uWGBdLapH75OomsZVmGe/BQeP3CZDtzGsonQ9eVCz7hPpGAcV+u9vk\n/NJLaIYH9+3Enkthrp9hYR583F8ua2Okursa5Q4fu+7zn0NtOOdfx9I4EvaeRL62\n52D38X6Ql6vGVnLxsbbl64mH6Cqd+nfs0/k5eOYPNgkt6kDBewrrqAHgMW4KSwBF\n3y4CHvv+h2paD4JQOaDUroxAaUFPK5pF3zbpWVqQAwCAntOq4uQvwf9KNUQxGnK8\ndh5zgYc3j6WzuSEkO5N0zUHOyb0Q6OyX5wGKbs6gdQKBgQD2NkCaQgmcUMoQXlCh\n06KpQ91lT72hO4ifVnm/eXgdZoLYQlz2K/b+CUprvEgyMoJqhbtJnrd01tQTzqT9\nsKd8BA+0Sy7i/0TJcn611ZOnGsf6K7w5VTcwe+yrSxAxrGBwi46vbGfyNHZO9GkJ\n8Hh+pHiqu9PnVRMuoVPjCGJXnwKBgQDddduA1qZxGGdctFQ4tbxOw3UeBOS2AYd9\nnzoUvNWfqvO6TqKLeXG/3GFM07wq3SkJSkOTwJCmxPrUg71E6V/TiVYDrJA/NkLh\nWCPdM2LNTfv/42jzGh/iV8czqSlVAgHMmcbIZTy3F5euCdlSDQ+zobpzTWHm1QaT\nrjS5CZc/3QKBgQDTyMT3K7jCmgfF3qrzGG2BtduaCuZt6xzRGnxtRJoaiBQi26rF\nIfo6eFlopLNhYmsmH9SDNiBOQ4B3bTgk9DCND51Gk6lIHxXXRkJSPN3eTr4Xpkko\n8/EeJmrkyROr9r+Z76GTqecbwx7FOZ2krBGpteYJzrku8tImOSVEU/DrtwKBgQC2\n1wNvD8lDPn64DzopyAG8laswKaIakpCmqrttO6qztJSdkSaqOI4tdWnv4DBOw2GK\nBgJdDnNe/OqKYmn1ZyhyocSeK+68AbSeEAMsMay1DFmuHrcbXspMOWSBxnwVbx7F\nKYDxGoRSNexJCGCgWaBJEpG5eH4H8oEHlKB61OxHEQKBgBvEFLQqaN2Whisc4O4T\nSL2YjPMzWvdBqVaVozQ5zXR6lWrgDNg3tPdX8g95OGwOpyDkk9Pa1+WG4AnNiirl\nzZ39epkAKmhQK18mT8zv2UpUynY12itV9CEgctxTGck6FkYcsyVg789RQMn+XN/W\npPFpzZDxNCzu5A6nNWqtD5Zi\n-----END PRIVATE KEY-----\n",
        "client_email": "gsheets-updates@accountssheets-438006.iam.gserviceaccount.com",
        "client_id": "105941998225468511415",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/gsheets-updates%40accountssheets-438006.iam.gserviceaccount.com",
        "universe_domain": "googleapis.com"
        }


    CLIENT = gauth(creds)
    print(CLIENT)
    
    def start_requests(self):
        
        data = pkgutil.get_data("dgbt","resources/accounts.csv")
        df = pd.read_csv(BytesIO((data)))
        # df = pd.read_csv(r'D:\Coding\Scrapping\Scrapy_projects\Freelancing\28-Oscar-dgpt\dgbt_main\dgbt\resources\accounts.csv')
          
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
            worksheet = check_clear(account, self.CLIENT)
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
        account = response.meta['account']
        worksheet = response.meta['worksheet']
        data = json.loads(response.text).get('data')
        
        name = data.get('account')
        mobile = data.get('mobile').replace('-','').strip()
        total_buy = data.get('total_buy')
        grade = data.get('grade')
        hist = data.get('created_at')
        email = data.get('email')
        mem_id = data.get('mem_id')
        sponsor = data.get('sponsor')
        team_num = data.get('team_num')
        total_profit= data.get('total_profit')
       
       

        row_dict={
                'Membro': name,
                'Teléfono': mobile, 
                'Suscripción_total': total_buy,
                'UID': mem_id,
                'Hora_de_registro': hist,
                'Número_total_de_equipos': team_num,
                'Superior': sponsor,
                'Ingresos_totales': total_profit,
                'grade':grade,
                'email':  email 
            }
        
        print(worksheet)
        add_rows(row_dict, worksheet, self.CLIENT, account)

        yield{
            'Membro': name,
            'Teléfono': mobile, 
            'Suscripción_total': total_buy,
            'account': account
        }   
        
        
        
process = CrawlerProcess({
    'RETRY_TIMES' : 5,  

    'RETRY_HTTP_CODES' : [500, 502, 503, 504, 522, 524, 408, 400, 401, 402, 403, 404, 405, 406, 429],

    # Crawl  by identifying yourself (and your website) on the user-agent
    'USER_AGENT' : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",

    'ROBOTSTXT_OBEY': False,

    'CONCURRENT_REQUESTS' : 10,
    # CONCURRENT_REQUESTS_PER_DOMAIN : 1,
    # CONCURRENT_REQUESTS_PER_IP : 1,
    
    'AUTOTHROTTLE_ENABLED': True,
    'AUTOTHROTTLE_START_DELAY' : 5,
    'AUTOTHROTTLE_MAX_DELAY' : 15,
    'DOWNLOAD_DELAY' : 3,

    'LOG_LEVEL' : 'DEBUG',
})

process.crawl(MembsSpider)
process.start()
