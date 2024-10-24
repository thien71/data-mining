import requests
import csv

# Biến toàn cục chứa API Key và URL cơ bản
API_KEY = 'p7CVKPTamXQJchOn1d3C6mksnpNvFommnZnLHwRx'
BASE_URL = 'https://api.eia.gov/v2/crude-oil-imports/data/'

# Hàm tạo URL truy vấn
def create_url(start_date, end_date, offset=0, length=10):
    """
    Hàm tạo URL truy vấn với các tham số cần thiết.
    :param start_date: Ngày bắt đầu truy vấn (YYYY-MM).
    :param end_date: Ngày kết thúc truy vấn (YYYY-MM).
    :param offset: Độ lệch (phân trang dữ liệu).
    :param length: Số lượng hàng dữ liệu cần lấy.
    :return: URL hoàn chỉnh để gọi API.
    """
    url = (f"{BASE_URL}?api_key={API_KEY}"
           f"&frequency=monthly"
           f"&data[0]=quantity"
           f"&start={start_date}&end={end_date}"
           f"&sort[0][column]=period&sort[0][direction]=desc"
           f"&offset={offset}&length={length}")
    return url

# Hàm cào dữ liệu từ API
def fetch_data_from_api(url):
    """
    Gửi yêu cầu GET tới API và trả về dữ liệu JSON.
    :param url: URL API để gửi yêu cầu.
    :return: JSON chứa dữ liệu hoặc None nếu thất bại.
    """
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()  # Trả về dữ liệu JSON
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return None

# Hàm xử lý dữ liệu JSON và lưu vào file CSV
def save_to_csv(json_data, csv_filename):
    """
    Xử lý dữ liệu JSON và lưu vào file CSV.
    :param json_data: Dữ liệu JSON lấy từ API.
    :param csv_filename: Tên file CSV để lưu dữ liệu.
    """
    # Mở file CSV để ghi
    with open(csv_filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        
        # Ghi dòng tiêu đề (tất cả các cột)
        writer.writerow(['Period', 'Origin Country', 'Origin ID', 'Origin Type', 'Origin Type Name',
                         'Destination ID', 'Destination Name', 'Destination Type', 'Destination Type Name',
                         'Grade ID', 'Grade Name', 'Quantity', 'Quantity Units'])
        
        # Ghi từng dòng dữ liệu
        if 'data' in json_data['response']:
            for entry in json_data['response']['data']:
                writer.writerow([
                    entry['period'],                # Thời gian nhập khẩu (YYYY-MM)
                    entry['originName'],            # Tên quốc gia xuất khẩu dầu thô (ví dụ: Algeria)
                    entry['originId'],              # Mã định danh của quốc gia xuất khẩu (ví dụ: CTY_AG)
                    entry['originType'],            # Loại nguồn xuất khẩu, ví dụ: CTY (Country - Quốc gia)
                    entry['originTypeName'],        # Tên đầy đủ của loại nguồn xuất khẩu, ví dụ: Country (Quốc gia)
                    entry['destinationId'],         # Mã định danh của điểm đến (cảng hoặc khu vực PADD), ví dụ: PP_1
                    entry['destinationName'],       # Tên cảng hoặc khu vực nhận dầu thô, ví dụ: PADD1 (East Coast)
                    entry['destinationType'],       # Loại điểm đến, ví dụ: PP (Port PADD - Cảng)
                    entry['destinationTypeName'],   # Tên đầy đủ của loại điểm đến, ví dụ: Port PADD (Cảng thuộc khu vực PADD)
                    entry['gradeId'],               # Mã định danh của loại dầu thô, ví dụ: LSW (Light Sweet)
                    entry['gradeName'],             # Tên loại dầu thô, ví dụ: Light Sweet (Dầu nhẹ, ít lưu huỳnh)
                    entry['quantity'],              # Số lượng dầu thô nhập khẩu, ví dụ: 412 (thousand barrels)
                    entry['quantity-units']          # Đơn vị đo lường, ví dụ: thousand barrels (nghìn thùng dầu thô)
                ])
        else:
            print("No data found in the response.")

    print(f"Data saved to {csv_filename}")

# Hàm chính để chạy chương trình
def main():
    # Thiết lập các tham số cho API
    start_date = "2023-12"
    end_date = "2024-07"
    offset = 0
    length = 5000  # Giới hạn 10 hàng dữ liệu

    # Tạo URL truy vấn
    url = create_url(start_date, end_date, offset, length)

    # Gửi yêu cầu và lấy dữ liệu từ API
    json_data = fetch_data_from_api(url)

    # Nếu có dữ liệu, xử lý và lưu vào file CSV
    if json_data:
        csv_filename = 'crude_oil_imports.csv'  # Tên file CSV
        save_to_csv(json_data, csv_filename)

if __name__ == "__main__":
    main()
