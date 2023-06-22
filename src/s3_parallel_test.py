import boto3
import itertools
import json
import math
import os
import requests
import time
import yaml
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from enum import Enum


class Executor(Enum):
    THREAD = 1
    PROCESS = 2


def thread_executor():
    return ThreadPoolExecutor(max_workers=max_workers)


def process_executor():
    return ProcessPoolExecutor(max_workers=max_workers)


def get_executor(executor: Executor):
    return process_executor() if executor == executor.PROCESS else thread_executor()


def upload_parts(file_path, key, part_size, part_numbers, upload_id):
    """
    The method is responsible to divide the file into several parts (depending on the part_size) and upload each part.
    It first seeks the file to a starting position, reads the required amount of data and then uploads the read part.

    Args:
        file_path: The original file that needs to be uploaded.
        key: Key of the file in the bucket.
        part_size: Part size to be uploaded, in bytes.
        part_numbers: The total number of parts that will be uploaded (count starts from 1 up to N).
        upload_id

    Returns:
        An array of dictionary, each dictionary has `PartNumber` and `ETag` keys with their proper values. This is
        later used to complete the multipart upload.
    """
    parts = []
    with open(file_path, "rb") as file:
        for part_number in part_numbers:
            # part_number is from 1 upto N, so we have to subtract 1 when calculating the position
            file_position = part_size * (part_number - 1)
            file.seek(file_position)
            part_data = file.read(part_size)
            url = s3.generate_presigned_url(
                ClientMethod="upload_part",
                Params={"Bucket": bucket, "Key": key, "UploadId": upload_id, "PartNumber": part_number},
                ExpiresIn=3600,
            )
            response = requests.put(url, data=part_data)
            parts.append({"PartNumber": part_number, "ETag": response.headers.get("ETag")})
    return parts


def parallel_multipart_upload(file_path, bucket, key, executor_type: Executor):
    """
    The method is responsible to create several threads/processes in order to perform a multipart upload.

    Args:
        file_path: Absolute path for the upload.
        bucket: Name of the bucket.
        key: Key to use when storing the file in the bucket
        executor_type: An Enum that specifies the type of executor. It can be either THREAD or PROCESS

    Returns:
        None

    """
    response = s3.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = response["UploadId"]

    num_parts = math.ceil(os.path.getsize(file_path) / part_size)

    # Split part numbers into chunks based on the number of threads
    part_number_chunks = [list(range(start, num_parts + 1, max_workers)) for start in range(1, max_workers + 1)]
    executor = get_executor(executor_type)
    # each thread/process would upload their respective part, executing `upload_parts` method. Each thread/process will
    # also have their own list of parts.
    futures = [executor.submit(upload_parts, file_path, key, part_size, part_numbers, upload_id) for part_numbers in
               part_number_chunks]
    # retrieve the parts from each future.
    parts = [part for future in futures for part in future.result()]
    # Quobyte throws an error if the parts are not sorted. So simply sort the parts based on their number (PartNumber).
    parts = sorted(parts, key=lambda part: part["PartNumber"])
    # Complete the multipart to combine them all together in a single file on the server.
    s3.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts}
    )


def download_part(part_info):
    """
    The method is responsible to download a specific part of the file that exists on S3 storage.

    Args:
        part_info: A dictionary that contains:
            - url
            - part_number
            - byte_range: Byte range of the file to download
            - filename: Name of the file on S3

    Returns:
        A dictionary that has the following structure:
            - part_number: to be later used for sorting all parts.
            - filename: to be later used when constructing the file locally on the machine.
    """
    url = part_info["url"]
    part_number = part_info["part_number"]
    byte_range = part_info["byte_range"]
    filename = part_info["filename"]

    headers = {"Range": f"bytes={byte_range[0]}-{byte_range[1]}"}

    response = requests.get(url, headers=headers)

    if response.status_code == 200 or response.status_code == 206:
        part_data = response.content
        with open(f"{filename}.part{part_number}", "wb") as f:
            f.write(part_data)
        return {"part_number": part_number,
                "filename": os.path.abspath(f"{filename}.part{part_number}")}
    else:
        print(f"Error downloading part {part_number}: {response.status_code}")
        return None


def parallel_multipart_download(bucket, key, filename, executor_type: Executor):
    """
    The method is responsible to perform a multi-part download of a single file using several threads/processes.

    Args:
        bucket: Name of the bucket.
        key: Key of the file in the bucket.
        filename: The absolute path of the file so that once downloaded, it's written locally on the machine.
        executor_type: An Enum that specifies the type of executor. It can be either THREAD or PROCESS

    Return:
        None

    """
    part_size = 16 * 1024 * 1024
    response = s3.head_object(Bucket=bucket, Key=key)
    file_size = response["ContentLength"]
    num_parts = math.ceil(file_size / part_size)
    parts = []

    # Generate a byte range (start, end) for each file part
    for i in range(num_parts):
        start = i * part_size
        end = start + part_size - 1
        if end > file_size:
            end = file_size
        byte_range = (start, end)
        part_number = i + 1
        parts.append({"url": s3.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": key}),
                      "part_number": part_number,
                      "byte_range": byte_range,
                      "filename": filename})

    with get_executor(executor_type) as executor:
        futures = [executor.submit(download_part, part) for part in parts]
        # Retrieve the downloaded parts from each future.
        downloaded_parts = [future.result() for future in futures]
        # Sort the parts based on the `part_number`.
        sorted_parts = sorted(downloaded_parts, key=lambda x: x["part_number"])

        # Combine the parts into a single file.
        with open(filename, "wb") as outfile:
            for part in sorted_parts:
                part_filename = part["filename"]
                with open(part_filename, "rb") as part_file:
                    outfile.write(part_file.read())
                # Remove the individual part file.
                os.remove(part_filename)


def thread_upload(directory):
    """
    The method takes as parameter a path, which can be a file or an entire directory. If the path is a directory, then
    the entire directory is uploaded to S3. Otherwise, only the file will be uploaded.

    Args:
        directory: Path of the file/directory to upload.
    Returns:
        None.
    """
    if os.path.isdir(directory):
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            for root, dirs, files in os.walk(directory):
                for file in files:
                    file_path = f"{root}{os.sep}{file}"
                    # used later to download the file
                    list_files.append(file_path)
                    executor.submit(parallel_multipart_upload,
                                    file_path=file_path,
                                    bucket=bucket,
                                    key=file_path,
                                    executor_type=Executor.PROCESS)
    else:
        list_files.append(directory)
        parallel_multipart_upload(
            file_path=directory,
            bucket=bucket,
            key=directory,
            executor_type=Executor.PROCESS)


def thread_download(list_files):
    """
    The method takes as parameter a list of files to download from S3. The method creates an executor, dividing the
    files among the workers.

    Args:
        list_files: An array of file keys to download.
    Returns:
        None
    """
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        for file in list_files:
            executor.submit(parallel_multipart_download,
                            bucket=bucket,
                            key=file,
                            filename=file,
                            executor_type=Executor.PROCESS)


def load_yaml(file_path):
    """
    A helper method that loads a yaml file and returns the content.

    Args:
        - file_path: Path of the yaml file to read/load

    Returns:
        Content of the yaml file
    """
    with open(file_path, mode="r") as file:
        yaml_data = yaml.safe_load(file)
    return yaml_data


ceph_endpoint = "<ceph-s3-endpoint>"
quobyte_endpoint = "<quobyte-s3-endpoint>"
ceph_mount = "<ceph-mountpoint-on-vm>"
quobyte_mount = "<quobyte-mountpoint-on-vm>"

config = load_yaml("config.yaml")
s3_endpoint = config["s3_endpoint_url"]
access_key = config["s3_access_key_id"]
secret_key = config["s3_secret_access_key"]
bucket = config["bucket_id"]
part_sizes = config["part_sizes"]
worker_count = config["worker_count"]

mount = ceph_mount if s3_endpoint == ceph_endpoint else quobyte_mount

combinations = list(itertools.product(part_sizes, worker_count))
list_files = []
results = []
upload_path = f"{mount}{os.sep}<your-file-or-folder>"

s3 = boto3.client("s3",
                  aws_access_key_id=access_key,
                  aws_secret_access_key=secret_key,
                  endpoint_url=s3_endpoint)

for item in combinations:
    part_size = item[0] * 1024 * 1024
    max_workers = item[1]
    print(f"Running {part_size} with {max_workers} workers")

    upload_start = time.time()
    thread_upload(upload_path)
    upload_end = time.time()
    thread_download(list_files)
    download_end = time.time()

    list_files.clear()

    result = {
        "part_size": part_size,
        "worker_count": max_workers,
        "upload_time": upload_end - upload_start,
        "download_time": download_end - upload_end
    }
    results.append(result)
    print(json.dumps(result, indent=4))

with open("results.json", mode="w") as file:
    json.dump(results, file, indent=4)
