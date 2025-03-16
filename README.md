# Massive Processing

This repository is designed for massive processing of data from Science Twitter. Follow the instructions below to set up and use the repository.

***
## Setting AWS Credentials (mandatory)

To set up your AWS credentials on a Windows machine, follow these steps:

1. **Install AWS CLI**:
    Download and install the AWS Command Line Interface (CLI) from the [official website](https://aws.amazon.com/cli/).

2. **Configure AWS CLI**:
    Open a command prompt and run the following command to configure your AWS credentials:

    ```bash
    aws configure
    ```

    You will be prompted to enter your AWS Access Key ID, Secret Access Key, region, and output format. Enter the required information.

3. **Verify Configuration**:
    To verify that your credentials are configured correctly, run:

    ```bash
    aws s3 ls
    ```

    This command should list your S3 buckets if the credentials are set up correctly.

4. **Enable Permissions**:
    Ensure that your AWS user has the necessary permissions to access S3. You can do this by attaching the appropriate policy to your user in the AWS Management Console:

    - Go to the [IAM Console](https://console.aws.amazon.com/iam/).
    - Select **Users** from the sidebar.
    - Click on your username.
    - Go to the **Permissions** tab.
    - Click **Add permissions**.
    - Choose **Attach policies directly**.
    - Search for and select the `AmazonS3FullAccess` policy (or a more restrictive policy if needed).
    - Click **Next: Review** and then **Add permissions**.

By following these steps, you will have configured your AWS credentials and permissions, and be ready to use the S3 client in your script.
***

## Cloning the Repository

First, clone the repository to your local machine:

```bash
git clone https://github.com/yourusername/massive_processing.git
cd massive_processing
```

## Setting Up the Environment

Create a virtual environment and install the required dependencies:

```bash
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

## Running the Main Script

To run the main script, you need to provide a valid input file containing author IDs. Place this file inside the `ids` directory.

Then substitute the path inside the **[main file](main.py)** before running it:

```bash
 valid_ids_path = ("data/ids/ids_filepath.csv")
```

Make sure to replace `ids_filepath.csv` with the actual name of your file.

Finally, choose the appropriate OA id column:

```bash
 authors_ids_col = "id_col"
```

Make sure to replace `id_col` with the actual name of your id_column.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any changes.

## Contact

For any questions or inquiries, please contact [your email].
