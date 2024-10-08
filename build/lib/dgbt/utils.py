import gspread
import pkgutil, json
from io import BytesIO
from rich import print
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.service_account import Credentials
import aiohttp
import asyncio
import gspread_asyncio


def gauth(creds):
    # data = pkgutil.get_data("dgbt","resources/creds.json")
    # creds = json.loads(data.decode('utf-8'))
    # print(creds)
    # # SERVICE_ACCOUNT_FILE = 'dgbt\resources\creds.json'
    # SERVICE_ACCOUNT_FILE = creds
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']
    # credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds,SCOPES)
    client = gspread.authorize(credentials)
    return client



async def check_clear(acc, client):
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
    worksheet.format("A1:C1", {"textFormat": {"bold": True}}) 
    worksheet.freeze(rows=1)
    # await asyncio.wait(10)
    print('Headers are created')
    
    return spreadsheet


async def membs_req(mem_url, headers):
    async with aiohttp.ClientSession() as session:
        async with session.get(mem_url, headers=headers) as response:
            res =  await response.json()
            await asyncio.sleep(5)
            return res
        
        

def add_rows_1(row_dict, worksheet, client):
    headers = ["Membro", "Teléfono", "Suscripción_total"]
    new_row = [''] * 3
    for key, value in row_dict.items():
        if key in headers:
            col_index =  headers.index(key)  # Find the correct column index
            new_row[col_index] = value
    print(new_row)
    accsht = client.open_by_key("1Gi-bTqVdQWG1B5bUZvvz2_798a8HHdh6UZ-nujZ2L0I").worksheet("sheet_name")
    print(accsht)
    accsht.append_row(new_row)
    print(f'{new_row} is added to {worksheet}')
    return None



async def add_rows(mem_url, headers, client, acc):
    
    res_body = await membs_req(mem_url, headers)
    
    sheet_id = "1Gi-bTqVdQWG1B5bUZvvz2_798a8HHdh6UZ-nujZ2L0I"
    worksht =  client.open_by_key(sheet_id)
    accsht =  worksht.worksheet(acc)
    
    data = res_body.get('data')
    name = data.get('account')
    mobile = data.get('mobile')
    total_buy = data.get('total_buy')
    row_dict={
            'Membro': name,
            'Teléfono': mobile, 
            'Suscripción_total': total_buy,
        }


    add_rows_1(row_dict, accsht)

    return row_dict





        
# async def members_details_1(mem_id, headers, spreadsheet, sheet_name):
#     mem_url = f'https://api.dgpt.club/api/myteam/meminfo?lang=en&v=1.3.5&mem_id={mem_id}'
#     res = await requests.get(mem_url,  headers= headers)
    
#     data = await res.json().get('data')
#     name = data.get('account')
#     mobile = data.get('mobile')
#     total_buy = data.get('total_buy')
#     row_dict={
#             'Membro': name,
#             'Teléfono': mobile, 
#             'Suscripción_total': total_buy,
#         }
   
#     await (add_rows(row_dict, spreadsheet, sheet_name))

#     return row_dict
        

# if __name__ =='__main__':
#     client = gauth()
#     accounts = ['Pichy29', 'tsunami24', 'jasibu@hotmail.com']
#     for acc in accounts:
#         worksheet = check_clear(acc, client)
#         new_row_dict = {
#             "Membro": "Jyunior",
#             "Teléfono": '+53-59231549',
#             "Suscripción_total": 332
#         }
        
#         add_rows(new_row_dict, worksheet)





