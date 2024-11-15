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
        os.chdir(temp_path)

        subprocess.run(
            ["prefetch", accession],
            check=True,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            ["fastq-dump", "--split-3", "--gzip", accession],
            check=True,
            capture_output=True,
            text=True,
        )

        for read in [1, 2]:
            fastq_file = f"{accession}_{read}.fastq.gz"
            if os.path.exists(fastq_file):
                s3_path = f"s3://{s3_bucket}/{fastq_file}"
                subprocess.run(
                    ["aws", "s3", "cp", fastq_file, s3_path],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                os.remove(fastq_file)

        subprocess.run(
            ["rm", "-rf", accession],
            check=True,
            capture_output=True,
            text=True,
        )
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
        "--processes", type=int, default=4, help="Number of parallel downloads"
    )
    parser.add_argument(
        "--temp-dir",
        default="/tmp/sra_downloads",
        help="Temporary directory for downloads",
    )
    parser.add_argument("--log-file", default="sra_download.log", help="Log file path")

    args = parser.parse_args()

    setup_logging(args.log_file)
    os.makedirs(args.temp_dir, exist_ok=True)

    with open(args.accession_list) as f:
        accessions = [line.strip() for line in f if line.strip()]

    logging.info(f"Starting download of {len(accessions)} accessions")

    raw_dir = Path(args.s3_bucket) / "raw"
    subprocess.run(
        ["aws", "s3", "mb", raw_dir],
        check=True,
        capture_output=True,
        text=True,
    )

    pool_args = [(acc, raw_dir, args.temp_dir) for acc in accessions]

    with Pool(processes=args.processes) as pool:
        results = pool.map(process_accession, pool_args)

    success_count = sum(1 for _, success in results if success)
    logging.info(
        f"Completed. Successfully processed {success_count}/{len(accessions)} accessions"
    )


if __name__ == "__main__":
    main()
