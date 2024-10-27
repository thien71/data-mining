import requests
import csv

# Biến toàn cục chứa API Key và URL cơ bản
API_KEY = 'p7CVKPTamXQJchOn1d3C6mksnpNvFommnZnLHwRx'
BASE_URL = 'https://api.eia.gov/v2/crude-oil-imports/data/'

# Hàm tạo URL truy vấn
def create_url(start_date, end_date, offset=0, length=5000):
    url = (f"{BASE_URL}?api_key={API_KEY}"
           f"&frequency=monthly"
           f"&data[0]=quantity"
           f"&start={start_date}&end={end_date}"
           f"&sort[0][column]=period&sort[0][direction]=desc"
           f"&offset={offset}&length={length}")
    return url

# Hàm cào dữ liệu từ API
def fetch_data_from_api(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return None

# Hàm xử lý dữ liệu JSON và lưu vào file CSV
def save_to_csv(json_data, csv_filename, mode='a'):
    with open(csv_filename, mode=mode, newline='') as file:
        writer = csv.writer(file)
        # Chỉ ghi tiêu đề khi mở file ở chế độ ghi ('w')
        if mode == 'w':
            writer.writerow(['Period', 'Origin Country', 'Origin ID', 'Origin Type', 'Origin Type Name',
                             'Destination ID', 'Destination Name', 'Destination Type', 'Destination Type Name',
                             'Grade ID', 'Grade Name', 'Quantity', 'Quantity Units'])
        
        row_count = 0
        # Kiểm tra nếu 'response' có trong dữ liệu JSON và có dữ liệu 'data'
        if json_data and 'response' in json_data and 'data' in json_data['response']:
            for entry in json_data['response']['data']:
                writer.writerow([
                    entry['period'],
                    entry['originName'],
                    entry['originId'],
                    entry['originType'],
                    entry['originTypeName'],
                    entry['destinationId'],
                    entry['destinationName'],
                    entry['destinationType'],
                    entry['destinationTypeName'],
                    entry['gradeId'],
                    entry['gradeName'],
                    entry['quantity'],
                    entry['quantity-units']
                ])
                row_count += 1
        else:
            print("No data found in the response or response structure has changed.")
    
    return row_count

# Hàm chính để chạy chương trình
def main():
    start_date = "2020-01"
    end_date = "2024-07"
    offset = 0
    length = 5000
    csv_filename = 'crude_oil_imports.csv'
    
    # Xóa file cũ nếu tồn tại và ghi dòng tiêu đề
    save_to_csv({}, csv_filename, mode='w')

    # Biến đếm tổng số hàng đã tải
    total_rows = 0
    max_rows = 100000  # Giới hạn tổng số hàng
    
    while total_rows < max_rows:
        url = create_url(start_date, end_date, offset, length)
        print("Requesting URL:", url)  # In URL để kiểm tra

        # Gửi yêu cầu và lấy dữ liệu từ API
        json_data = fetch_data_from_api(url)

        # Kiểm tra nếu không có dữ liệu để tải thêm
        if not json_data or 'response' not in json_data or 'data' not in json_data['response']:
            print("No more data to fetch or error in response structure.")
            break

        # Lưu dữ liệu và đếm số hàng mới thêm
        rows_added = save_to_csv(json_data, csv_filename, mode='a')
        total_rows += rows_added

        # Tăng offset để lấy trang dữ liệu tiếp theo
        offset += length

        # Kiểm tra nếu đã đạt đến giới hạn hàng
        if total_rows >= max_rows:
            print(f"Reached row limit of {max_rows}. Stopping download.")
            break

    print(f"Total rows saved: {total_rows}")

if __name__ == "__main__":
    main()
