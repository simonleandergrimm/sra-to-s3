# SRA to S3 Downloader

A simple Python script that downloads sequencing data from NCBI's Sequence Read Archive (SRA) and uploads it to Amazon S3. Ideally run on an EC2 instance. Loosely based on [this repo](https://github.com/harmonbhasin/prepare-data) by Harmon Bhasin.

## Requirements

- Python
- SRA Toolkit. See downloads [here](https://github.com/ncbi/sra-tools/wiki/01.-Downloading-SRA-Toolkit). If you work on AWS EC2, you can simply install the SRA toolkit by executing `setup-yum.sh`.

## Usage
 - Search your Bioproject on NCBI, and go to the dataset.
 - Click on "SRA" under 'Related information' on the right side of the page.
 - You should see a pop-up above 'Links from BioProject in the center, that says "View results as an expanded interactive table using the RunSelector. Send results to RunSelector". Click on "Send to RunSelector".
 - You should see a table with all the runs. Click the button labeled "Accession List" to download all accessions. Then copy it to your machine using scp (for example, `scp -i [location to your key] ~/Downloads/SRR_Acc_List.txt[ec2 instance name]:/home/ec2-user/sra-to-s3/`).
 - Create an S3 bucket into which you want to download the SRA data.
 - Run the script with the following command:

```
python sra_to_s3.py \
--accession-list SRR_Acc_List.txt \
--s3-bucket s3://your-bucket/path \
--n-processes 16
```

## Possible issues

 - Ensure your AWS credentials are properly configured on the EC2 instance.
 - Check how many cores your machine has using `nproc`. Adjust the `--n-processes` flag accordingly.