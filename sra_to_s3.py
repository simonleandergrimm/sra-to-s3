#!/usr/bin/env python3
import argparse
import logging
import os
import subprocess
from multiprocessing import Pool
from pathlib import Path


def setup_logging(log_file):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )


def process_accession(args):
    accession, s3_bucket, temp_dir = args
    try:
        logging.info(f"Processing {accession}")
        temp_path = Path(temp_dir) / accession
        temp_path.mkdir(exist_ok=True)

        raw_s3_dir = f"{s3_bucket}/raw"
        os.chdir(temp_path)

        # Check if files already exist on S3 before downloading
        for read in [1, 2]:
            fastq_file = f"{accession}_{read}.fastq.gz"
            s3_path = f"{raw_s3_dir}/{fastq_file}"
            cmd = ["aws", "s3", "ls", s3_path]
            logging.info(f"{' '.join(cmd)}")
            try:
                subprocess.run(cmd, check=True, capture_output=True, text=True)
                logging.info(f"File {s3_path} already exists on S3, skipping download")
                return (accession, True)
            except subprocess.CalledProcessError:
                pass

        cmd = ["prefetch", accession]
        logging.info(f"{' '.join(cmd)}")
        subprocess.run(cmd, check=True, capture_output=True, text=True)

        cmd = ["fastq-dump", "--split-3", "--gzip", accession]
        logging.info(f"{' '.join(cmd)}")
        subprocess.run(cmd, check=True, capture_output=True, text=True)

        for read in [1, 2]:
            fastq_file = f"{accession}_{read}.fastq.gz"
            if os.path.exists(fastq_file):
                s3_path = f"{raw_s3_dir}/{fastq_file}"
                cmd = ["aws", "s3", "cp", fastq_file, s3_path]
                logging.info(f"{' '.join(cmd)}")
                subprocess.run(cmd, check=True, capture_output=True, text=True)
                os.remove(fastq_file)

        cmd = ["rm", "-rf", accession]
        logging.info(f"{' '.join(cmd)}")
        subprocess.run(cmd, check=True, capture_output=True, text=True)

        os.chdir("..")
        return (accession, True)

    except Exception as e:
        logging.error(f"Error processing {accession}: {str(e)}")
        return (accession, False)


def main():
    parser = argparse.ArgumentParser(description="Download SRA data to S3")
    parser.add_argument(
        "--accession-list", required=True, help="Path to accession list file"
    )
    parser.add_argument("--s3-bucket", required=True, help="S3 bucket path")
    parser.add_argument(
        "--n-processes", type=int, default=16, help="Number of parallel downloads"
    )
    parser.add_argument(
        "--temp-dir",
        default="/tmp/sra_downloads",
        help="Temporary directory for downloads",
    )
    parser.add_argument("--log-file", default="sra_download.log", help="Log file path")

    args = parser.parse_args()

    log_file = args.log_file
    s3_bucket = args.s3_bucket
    s3_bucket = s3_bucket.rstrip('/')
    temp_dir = args.temp_dir
    accession_list = args.accession_list
    n_processes = args.n_processes

    if not os.path.exists(accession_list):
        raise FileNotFoundError(f"Accession list file not found: {accession_list}")

    if not s3_bucket.startswith("s3://"):
        raise ValueError("S3 bucket path must start with 's3://'")


    setup_logging(log_file)
    os.makedirs(temp_dir, exist_ok=True)

    with open(accession_list) as f:
        accessions = [line.strip() for line in f if line.strip()]

    logging.info(f"Starting download of {len(accessions)} accessions")

    pool_args = [(acc, s3_bucket, temp_dir) for acc in accessions]

    with Pool(processes=n_processes) as pool:
        results = []
        for i, result in enumerate(pool.imap_unordered(process_accession, pool_args)):
            results.append(result)
            logging.info(f"Progress: {i+1}/{len(accessions)} accessions processed")

    success_count = sum(1 for _, success in results if success)
    logging.info(
        f"Completed. Successfully processed {success_count}/{len(accessions)} accessions"
    )


if __name__ == "__main__":
    main()
