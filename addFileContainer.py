from azure.storage.blob import BlobServiceClient

storage_account_key = "ZDlcXuY0PRswWX+bBwrLkMeEO9Kq38xNIB//lY/2Zk/DVHlhK0liV0VpcB0QJ132/v5azvygI4/8+ASt7Hi9Gw=="
storage_account_name ="lab1ced"
connection_string = "DefaultEndpointsProtocol=https;AccountName=lab1ced;AccountKey=ZDlcXuY0PRswWX+bBwrLkMeEO9Kq38xNIB//lY/2Zk/DVHlhK0liV0VpcB0QJ132/v5azvygI4/8+ASt7Hi9Gw==;EndpointSuffix=core.windows.net"
container_name = input("Enter container name : ")

def uploadToBlobStorage(file_path, file_name):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
    
    with open(file_path, "rb") as data: 
        blob_client.upload_blob(data)
    print("Uploaded {file_name}.")


uploadToBlobStorage("C:\\Users\\HatimMano\\Desktop\\Datalake\\drive-download-20221114T155957Z-001\\adult.test", "adult.test")
uploadToBlobStorage("C:\\Users\\HatimMano\\Desktop\\Datalake\\drive-download-20221114T155957Z-001\\base_stations.parquet", "base_stations.parquet")
uploadToBlobStorage("C:\\Users\\HatimMano\\Desktop\\Datalake\\drive-download-20221114T155957Z-001\\devices.csv", "devices.csv")
uploadToBlobStorage("C:\\Users\\HatimMano\\Desktop\\Datalake\\drive-download-20221114T155957Z-001\\devices.json", "devices.json")
uploadToBlobStorage("C:\\Users\\HatimMano\\Desktop\\Datalake\\drive-download-20221114T155957Z-001\\Index.txt", "Index.txt")
uploadToBlobStorage("C:\\Users\\HatimMano\\Desktop\\Datalake\\drive-download-20221114T155957Z-001\\stocks_data.zip", "stocks_data.zip")
uploadToBlobStorage("C:\\Users\\HatimMano\\Desktop\\Datalake\\drive-download-20221114T155957Z-001\\accountdevice\\part-00000-f3b62dad-1054-4b2e-81fd-26e54c2ae76a.csv", "part-00000-f3b62dad-1054-4b2e-81fd-26e54c2ae76a.csv")