import os
import boto3
from botocore.config import Config
from sqlalchemy import create_engine, Column, Integer, String, Boolean
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import requests

Base = declarative_base()

class BatchNitroflare(Base):
    __tablename__ = 'nitroflare'  # ปรับเปลี่ยนชื่อตารางเป็น 'nitroflare'

    id = Column(Integer, primary_key=True)
    file_id = Column(String(255))  # ปรับเปลี่ยนชื่อคอลัมน์เป็น 'file_id'
    status = Column(Integer)
    s3_link = Column(String(255))

def delete_from_vultr(file_name, bucket_name):
    ACCESS_KEY_ID = ''
    SECRET_ACCESS_KEY = ''
    ENDPOINT_URL = 'https://sgp1.vultrobjects.com'

    s3 = boto3.client('s3',
                      endpoint_url=ENDPOINT_URL,
                      aws_access_key_id=ACCESS_KEY_ID,
                      aws_secret_access_key=SECRET_ACCESS_KEY,
                      config=Config(signature_version='s3v4'),
                      region_name='sgp1')

    s3.delete_object(Bucket=bucket_name, Key=file_name)

def download_nitroflare_file(url, path):
    response = requests.get(url, stream=True)
    file_name = os.path.join(path, url.split("/")[-1])

    with open(file_name, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    return file_name

def get_nitroflare_download_link(user, premiumKey, file_id):
    url = f"https://nitroflare.com/api/v2/getDownloadLink?user={user}&premiumKey={premiumKey}&file={file_id}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data.get("type") == "success":
            return data["result"]["url"]
    return None


def upload_to_vultr(file_path, bucket_name):
    ACCESS_KEY_ID = ''
    SECRET_ACCESS_KEY = ''
    ENDPOINT_URL = 'https://sgp1.vultrobjects.com'

    s3 = boto3.resource('s3',
                        endpoint_url=ENDPOINT_URL,
                        aws_access_key_id=ACCESS_KEY_ID,
                        aws_secret_access_key=SECRET_ACCESS_KEY,
                        config=Config(signature_version='s3v4'),
                        region_name='sgp1')

    FILE_NAME = file_path.split("/")[-1]
    data = open(file_path, 'rb')
    s3.Bucket(bucket_name).put_object(Key=FILE_NAME, Body=data, ACL='public-read')

    return f"https://sgp1.vultrobjects.com/{bucket_name}/{FILE_NAME}"


def main():
    user = "email@email.com" # ใส่ email ของคุณ
    premiumKey = "password # ใส่รหัสผ่านของคุณ
    path = "/home//batch/m1n.app/get/tmp/nitroflare/"
    engine = create_engine('mysql+pymysql://db_user:password@localhost:3306/db_name')
    Session = sessionmaker(bind=engine)
    session = Session()

    rows = session.query(BatchNitroflare).filter(BatchNitroflare.status == 1).all()

    for row in rows:
        download_link = get_nitroflare_download_link(user, premiumKey, row.file_id)
        if download_link:
            file_path = download_nitroflare_file(download_link, path)
            s3_url = upload_to_vultr(file_path, 'nitroflare')

            row.s3_link = s3_url
            row.status = 2

            # Remove the file after upload
            os.remove(file_path)

    # Add this block to delete files with status = 4
    rows_to_delete = session.query(BatchNitroflare).filter(BatchNitroflare.status == 4).all()
    for row in rows_to_delete:
        if row.s3_link:
            file_name = row.s3_link.split("/")[-1]
            delete_from_vultr(file_name, 'nitroflare')
        session.delete(row)

    session.commit()
    print("Process completed!")


if __name__ == "__main__":
    main()


