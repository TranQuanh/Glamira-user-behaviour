import os
import IP2Location
import json
import collection.connect_mongodb as connect_mongodb
database = IP2Location.IP2Location(r"D:\project_glamira\IP2Location-Python-master\data\need\IP2LOCATION-LITE-DB11.BIN")
import time
from pymongo import errors
from IPython.display import clear_output

def update_json(file_path, new_data):
    # Kiểm tra xem tệp có tồn tại không
    if os.path.exists(file_path):
        # Đọc dữ liệu hiện có từ tệp
        with open(file_path, 'r', encoding='utf-8') as file:
            try:
                existing_data = json.load(file)
            except json.JSONDecodeError:
                existing_data = {}
    else:
        existing_data = {}

    # Kiểm tra nếu dữ liệu hiện có không phải là dict, biến nó thành dict
    if not isinstance(existing_data, dict):
        existing_data = {}

    existing_data.update(new_data)    
    

    # Ghi dữ liệu đã cập nhật trở lại tệp
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(existing_data, file, ensure_ascii=False, indent=4)
        
def take_info(initial_doc):
    global ip_list
    global ip_list_error
    global ip_list_unknown
    

    ip = initial_doc.get("ip")
    if ip in ip_list:
        return
    rec = database.get_all(ip)
    document = {}
    document['ip'] = ip
    document['country_code'] = rec.country_short
    document['contry_name'] = rec.country_long
    document['region_name'] = rec.region
    document['city_name'] = rec.city
    document['latitude'] = rec.latitude
    document['longtitude'] = rec.longitude
    document['ZIP_code'] = rec.zipcode
    document['time_zone'] = rec.timezone
    
    ip_list[ip] = document
    
    update_json('ip_list.json',ip_list)



def main_code():
    collection =connect_mongodb.connect_db()
    documents = collection.find({},{"ip":1,"_id":0})
    doc =0
    for document in documents:
        doc+=1
        take_info(document)
        print(f'doc:{doc}')
        if(doc%2000==0):
            print(f"list_ip:{len(ip_list)}")
            print("Sleep 10s")
            time.sleep(10)
            clear_output()
    print(doc)
          
# Initialize dictionaries
ip_list = {}
# ip_list_error = {}
# ip_list_unknown = {}

# Đường dẫn tới file JSON
file_path = 'ip_list.json'

# Đường dẫn tới file JSON
file_path = 'ip_list.json'

# Đọc nội dung của file JSON
with open(file_path, 'r', encoding='utf-8') as file:
    try:
        ip_list = json.load(file)
    except:
        ip_list = {}
print(len(ip_list))

while True:
    try:
        main_code()
    except errors.CursorNotFound as e:
        print(f"Cursor not found:{e}")
        time.sleep(10)
        continue
    except Exception as e:
        print(f"An expected error occurred:{e}")
        time.sleep(5)
        continue
    break

