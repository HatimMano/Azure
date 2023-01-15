import os
import random

from azure.storage.filedatalake import (
    DataLakeServiceClient,
)
SOURCE_FILE = 'adult.names'

def upload_download_sample(filesystem_client):
    #creating file
    file_name = "testfile"
    print("Creating a file named '{}'.".format(file_name))
    #Starting 
    file_client = filesystem_client.get_file_client(file_name)
    file_client.create_file()
    #Ending 

    #Preparing 
    file_content = get_random_bytes(4*1024)

    #Appending file content
    print("Uploading data to '{}'.".format(file_name))
    file_client.append_data(data=file_content[0:1024], offset=0, length=1024)
    file_client.append_data(data=file_content[1024:2048], offset=1024, length=1024)
    #Start
    file_client.append_data(data=file_content[2048:3072], offset=2048, length=1024)
    #End
    file_client.append_data(data=file_content[3072:4096], offset=3072, length=1024)

    file_client.flush_data(len(file_content))

    #Get properties
    #Start
    properties = file_client.get_file_properties()
    #End

    #Read data
    print("Downloading data from '{}'.".format(file_name))
    #Start
    download = file_client.download_file()
    downloaded_bytes = download.readall()
    #End

    #Verifying 
    if file_content == downloaded_bytes:
        print("The downloaded data is equal to the data uploaded.")
    else:
        print("Something went wrong.")

    #Renaming properties
    #Start
    new_client = file_client.rename_file(file_client.file_system_name + '/' + 'newname')
    #End

    #Downloading to a file
    with open(SOURCE_FILE, 'wb') as stream:
        download = new_client.download_file()
        download.readinto(stream)

    #Start
    new_client.delete_file()
    #End

#Help function to generate random bytes
def get_random_bytes(size):
    rand = random.Random()
    result = bytearray(size)
    for i in range(size):
        result[i] = int(rand.random()*255)  
    return bytes(result)


def run():
    account_name = 'lab1cedric'
    account_key = 'PEsu33Xxmymlg5uLhBRL5Vj439+2miff9i9MBJ+RSDbtiHgvrSnFhscWQPI8V+UgtDsGRnDKB14G+ASth1LkPw=='

    #Set up client connection to Data Lake Storage account 
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https",
        account_name
    ), credential=account_key)

    #Generate a random name for the filesystem
    fs_name = "testfs{}".format(random.randint(1, 1000))
    print("Generating a test filesystem named '{}'.".format(fs_name))

    #Create the filesystem
    filesystem_client = service_client.create_file_system(file_system=fs_name)

    #Invoke the sample function
    try:
        upload_download_sample(filesystem_client)
    finally:
        #Clean up the filesystem and close the client connection
        filesystem_client.delete_file_system()


if __name__ == '__main__':
    run()