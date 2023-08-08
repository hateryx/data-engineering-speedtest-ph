import subprocess


def download_parquet():
    YEAR = 2022
    QUARTER = 1
    MONTH = 1

    for i in range(4):
        try:
            target_file = f"s3://ookla-open-data/parquet/performance/type=mobile/year={YEAR}/quarter={QUARTER}/{YEAR}-{MONTH:02}-01_performance_mobile_tiles.parquet"
            aws_cli_command = ["aws", "s3", "cp",
                               "--no-sign-request", target_file, "./parquet_files/"]
            output = subprocess.check_output(
                aws_cli_command, stderr=subprocess.STDOUT)
            QUARTER += 1
            MONTH += 3
        except subprocess.CalledProcessError as e:
            print("Error executing the AWS CLI command:",
                  e.output.decode("utf-8"))
            break


download_parquet()
