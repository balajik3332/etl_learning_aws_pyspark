# 00 — Prerequisites and Setup

> **Start here.** Before you write a single line of code or click anything in AWS, work through this guide from top to bottom. Every step builds on the one before it. Skipping steps will cause failures later that are hard to debug.

---

## Table of Contents

1. [What is this course?](#1-what-is-this-course)
2. [Install local tools](#2-install-local-tools)
3. [Set up your AWS account](#3-set-up-your-aws-account)
4. [Clone the project and install Python packages](#4-clone-the-project-and-install-python-packages)
5. [Cost rules — IMPORTANT, read before touching AWS](#5-cost-rules--important-read-before-touching-aws)
6. [AWS region to use](#6-aws-region-to-use)
7. [Troubleshooting common setup issues](#7-troubleshooting-common-setup-issues)
8. [Understanding your AWS credentials file](#8-understanding-your-aws-credentials-file)
9. [Python virtual environments](#9-python-virtual-environments-optional-but-recommended)
10. [Understanding IAM in more depth](#10-understanding-iam-in-more-depth)
11. [AWS Console navigation tips](#11-aws-console-navigation-tips)
12. [Quick reference — commands you will use often](#12-quick-reference--commands-you-will-use-often)
13. [What each project folder contains](#13-what-each-project-folder-contains)
14. [Knowledge prerequisites — filling the gaps](#14-knowledge-prerequisites--filling-the-gaps)
15. [How to get help](#15-how-to-get-help)

---

## 1. What is this course?

### 1.1 What is ETL?

ETL stands for **Extract, Transform, Load**. It is one of the most common patterns in data engineering. Here is what each word means in plain English:

- **Extract** — Read data from a source. The source might be a CSV file, a database, an API, or a folder full of JSON files.
- **Transform** — Clean and reshape the data. Fix bad values, change data types, rename columns, join tables together, calculate new columns, filter rows you do not need.
- **Load** — Write the cleaned data somewhere useful. Usually a data warehouse, a data lake, or a set of files in a cloud storage bucket.

Think of ETL like cooking. You **extract** the raw ingredients from the fridge, you **transform** them by chopping, mixing, and cooking, and then you **load** the finished dish onto the plate.

In real companies, ETL pipelines run every night (or every hour) to move data from production systems into analytics systems so that data analysts and data scientists can query it without slowing down the production database.

### 1.2 What is batch processing and why does it matter?

There are two main ways to process data:

- **Batch processing** — Collect data for a period of time (an hour, a day, a week), then process it all at once in one big job. Like doing your laundry once a week instead of one item at a time.
- **Stream processing** — Process each event the moment it arrives. Like a live sports scoreboard that updates every second.

Batch processing is simpler, cheaper, and handles the majority of real-world data engineering workloads. Most companies run batch ETL jobs overnight to process the previous day's transactions, user activity, or sensor readings.

This course focuses entirely on batch processing. You will not need to learn about streaming (Kinesis, Kafka) to complete this project.

### 1.3 What will you build and learn?

By the end of this project you will have built a complete batch ETL system on AWS. Here is what that means concretely:

**What you will build:**
- A synthetic data generator that creates realistic CSV, JSON, and Parquet files
- Three ETL patterns: Simple ETL, Change Data Capture (CDC), and a Multi-Step Pipeline
- Automated triggers that start ETL jobs when a file arrives, on a schedule, or on demand
- Email notifications that tell you when a job finishes or fails
- Infrastructure scripts that create all your AWS resources automatically

**What you will learn:**
- How to write PySpark code to transform data at scale
- The difference between AWS Glue (managed ETL service) and EMR Serverless (managed Spark)
- How to trigger AWS jobs from code and from the AWS Console
- How IAM security works and why least-privilege matters
- How to keep your AWS bill under $10 per month while learning

You will implement every ETL pattern **twice** — once using AWS Glue and once using EMR Serverless — so you can compare them directly and understand when to choose each one.

### 1.4 AWS services used in this course

You do not need to know any of these services yet. They are explained in detail in later guides. This table gives you a quick map of what is coming:

| AWS Service | One-line description |
|-------------|----------------------|
| **Amazon S3** | Cloud storage — like a hard drive in the sky. Stores all your data files. |
| **AWS Glue** | Managed ETL service — AWS runs your PySpark jobs so you do not have to manage servers. |
| **EMR Serverless** | Managed Apache Spark — run Spark jobs without managing a cluster. |
| **AWS Lambda** | Serverless functions — small Python functions that run in response to events. |
| **Amazon CloudWatch** | Monitoring and logging — watch your jobs, set billing alarms, view logs. |
| **Amazon SNS** | Simple Notification Service — sends email or messages when your jobs finish. |
| **Amazon SQS** | Simple Queue Service — a message queue that holds job status events. |
| **AWS IAM** | Identity and Access Management — controls who (or what) can do what in your account. |

---

## 2. Install local tools

You need four tools on your laptop before you can do anything else. Work through them in order.

### 2.1 Python 3.9 or higher

**Why you need it:** All the scripts in this project are written in Python. You run them from your terminal.

**Download:**
- Windows / Mac / Linux: https://www.python.org/downloads/
- Download the latest 3.x release (3.11 or 3.12 is fine).
- On Windows, check the box that says **"Add Python to PATH"** during installation. This is easy to miss.

**How to verify it worked:**

Open a new terminal window (Command Prompt on Windows, Terminal on Mac/Linux) and run:

```bash
python --version
```

Expected output (your version number may be different):

```
Python 3.11.7
```

If you see `Python 2.x.x` instead, you have the old Python 2 installed. Try `python3 --version` instead. On Mac you may need to use `python3` everywhere in this guide.

If you see `command not found` or `'python' is not recognized`, Python is not in your PATH. See the troubleshooting table at the end of this guide.

### 2.2 pip (Python package manager)

**Why you need it:** pip installs Python libraries. You use it to install boto3, PySpark, pandas, and everything else this project needs.

pip comes bundled with Python 3.9+, so you probably already have it.

**How to verify it worked:**

```bash
pip --version
```

Expected output:

```
pip 23.3.1 from /usr/local/lib/python3.11/site-packages/pip (python 3.11)
```

**How to upgrade pip (recommended):**

```bash
python -m pip install --upgrade pip
```

### 2.3 AWS CLI version 2

**Why you need it:** The AWS CLI (Command Line Interface) lets you talk to AWS from your terminal. You use it to configure your credentials, run AWS commands, and trigger jobs without using the AWS Console.

**Download:**
- **Windows**: https://awscli.amazonaws.com/AWSCLIV2.msi — download and run the installer
- **Mac**: https://awscli.amazonaws.com/AWSCLIV2.pkg — download and run the installer
- **Linux**: Run these commands in your terminal:

```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
```

**How to verify it worked:**

Open a **new** terminal window after installation (important — the old window will not see the new PATH):

```bash
aws --version
```

Expected output:

```
aws-cli/2.15.10 Python/3.11.6 Darwin/23.0.0 exe/x86_64 prompt/off
```

The key thing is that it says `aws-cli/2.x.x`. If it says `aws-cli/1.x.x` you have the old version — uninstall it and install v2.

### 2.4 Git

**Why you need it:** Git is used to clone (download) this project to your laptop, and to track changes to your code.

**Download:**
- **Windows**: https://git-scm.com/download/win — download and run the installer. Accept all defaults.
- **Mac**: Git comes pre-installed on most Macs. Check first:
  ```bash
  git --version
  ```
  If not installed, macOS will prompt you to install Xcode Command Line Tools.
- **Linux (Ubuntu/Debian)**:
  ```bash
  sudo apt-get update && sudo apt-get install git
  ```

**How to verify it worked:**

```bash
git --version
```

Expected output:

```
git version 2.43.0
```

### 2.5 VS Code (recommended editor)

**Why you need it:** VS Code is a free code editor that works well with Python and has extensions for AWS. You can use any editor you like, but the instructions in this course assume VS Code.

**Download:** https://code.visualstudio.com/download

**Recommended extensions** (install from VS Code's Extensions panel, the square icon on the left sidebar):

- **Python** (by Microsoft) — syntax highlighting, IntelliSense, debugging for Python files
- **AWS Toolkit** (by Amazon) — browse S3 buckets, view CloudWatch logs, manage Lambda functions directly from VS Code

To install an extension: open VS Code → click the Extensions icon (Ctrl+Shift+X / Cmd+Shift+X) → search the extension name → click Install.

---

## 3. Set up your AWS account

This section walks you through creating an AWS account, setting up a secure IAM user, and connecting your terminal to AWS. Take your time here — a misconfigured AWS account is the most common source of problems in this course.

### 3.1 Create a free-tier AWS account

**What is free tier?** AWS gives new accounts 12 months of free usage on many services (S3, Lambda, CloudWatch, SNS, SQS). The ETL services (Glue, EMR) are not free, but the total cost for this course is designed to stay under $10/month.

**Steps:**

1. Go to https://aws.amazon.com and click **Create an AWS Account**
2. Enter your email address and choose an account name (e.g., `my-etl-learning`)
3. Choose the **Free tier** plan when asked
4. Enter a credit card — **you will not be charged** as long as you follow the cost rules in Section 5
5. Complete phone verification
6. Sign in to the AWS Console at https://console.aws.amazon.com

> **Cost warning:** AWS charges by the second for compute services like Glue and EMR. Forgetting to stop a job or leaving a schedule running can add unexpected charges. Section 5 of this guide covers the exact rules to follow to stay under $10/month. Read it carefully before running any AWS commands.

### 3.2 Why you must NOT use the root account

When you create an AWS account, you get a **root account** — the master login tied to your email address. The root account has unlimited access to everything in your account with no restrictions.

**Never use the root account for everyday work.** Here is why:

- If someone steals your root account credentials, they can delete everything, run up a huge bill, or use your account for attacks.
- AWS best practices require you to use an IAM user with only the permissions needed for your work.
- If you accidentally commit your root account keys to GitHub, the damage is catastrophic and irreversible.

You will create a dedicated IAM user called `etl-course-student` in the next section and use only that user's credentials from this point forward.

### 3.3 Create an IAM user step by step

An IAM user is a named identity inside your AWS account. It has its own access keys that you use to authenticate from the CLI.

**Step 1: Open IAM in the AWS Console**

1. Sign in to the AWS Console at https://console.aws.amazon.com using your root account (just this once)
2. In the search bar at the top, type `IAM` and click **IAM** (Identity and Access Management)
3. In the left sidebar, click **Users**
4. Click the **Create user** button

**Step 2: Set the user name and access type**

1. Under **User name**, type: `etl-course-student`
2. Check **Provide user access to the AWS Management Console** if you want to log in to the Console with this user (optional but recommended for beginners)
3. If you enabled console access, choose **I want to create an IAM user** and set a password
4. Click **Next**

**Step 3: Attach permissions**

1. Select **Attach policies directly**
2. Search for and check each of these policies:

| Policy name | What it allows |
|-------------|---------------|
| `AmazonS3FullAccess` | Create, read, write, and delete S3 buckets and objects |
| `AWSGlueConsoleFullAccess` | Create and run AWS Glue jobs, view Glue Data Catalog |
| `AmazonEMRFullAccessPolicy_v2` | Create and submit jobs to EMR Serverless applications |
| `AWSLambda_FullAccess` | Create, update, and invoke Lambda functions |
| `AmazonSNSFullAccess` | Create SNS topics and publish messages |
| `AmazonSQSFullAccess` | Create SQS queues and send/receive messages |
| `CloudWatchFullAccess` | View logs, create metrics, set billing alarms |
| `IAMFullAccess` | Create IAM roles that Glue, EMR, and Lambda will use |

3. After checking all eight policies, click **Next**
4. Review the summary and click **Create user**

> **Note on IAMFullAccess:** Giving an IAM user the ability to create other IAM roles is powerful. In a real company you would scope this down further. For this learning project it is the simplest approach. Never share these credentials with anyone.

**Step 4: Create access keys**

Access keys are how the AWS CLI authenticates as your IAM user from your terminal. They are a pair of strings: an Access Key ID (public, starts with `AKIA`) and a Secret Access Key (private, shown only once).

1. After creating the user, click the user name `etl-course-student` to open the user details
2. Click the **Security credentials** tab
3. Scroll down to **Access keys** and click **Create access key**
4. Select **Command Line Interface (CLI)** as the use case
5. Check the confirmation checkbox and click **Next**
6. Add a description tag (optional, e.g., `etl-course-laptop`) and click **Create access key**
7. You will see your **Access Key ID** and **Secret Access Key**

**Download and store these keys securely right now. You cannot see the Secret Access Key again after you close this page.**

> **CRITICAL security rules:**
> - Never paste your access keys into Slack, email, or a chat window
> - Never commit your access keys to Git — not even in a `.env` file
> - Never share them with anyone
> - If you accidentally expose them, go to IAM immediately and **deactivate** the key, then create a new one

### 3.4 Configure the AWS CLI

Now you connect your terminal to your AWS account using the access keys you just downloaded.

Open your terminal and run:

```bash
aws configure
```

You will be prompted to enter four values. Type each one and press Enter:

```
AWS Access Key ID [None]: AKIAIOSFODNN7EXAMPLE
AWS Secret Access Key [None]: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
Default region name [None]: us-east-1
Default output format [None]: json
```

Replace the example key values with your actual keys. Use exactly `us-east-1` as the region (more on why in Section 6).

This stores your credentials in `~/.aws/credentials` on your machine (a plain text file in your home directory). The AWS CLI reads this file automatically every time you run an `aws` command.

### 3.5 Verify your CLI is working

Run this command:

```bash
aws sts get-caller-identity
```

Expected output (your account ID and ARN will be different):

```json
{
    "UserId": "AIDARANDOMEXAMPLEID",
    "Account": "123456789012",
    "Arn": "arn:aws:iam::123456789012:user/etl-course-student"
}
```

If you see this output, your CLI is correctly configured. The `Arn` line confirms you are authenticated as the `etl-course-student` IAM user, not the root account.

If you see an error, check the troubleshooting table at the end of this guide.

---

## 4. Clone the project and install Python packages

### 4.1 Clone the repository

Open your terminal, navigate to the folder where you keep your projects, and run:

```bash
git clone <repo-url>
cd batch-etl-teaching-project
```

Replace `<repo-url>` with the actual URL of this repository (e.g., `https://github.com/your-username/batch-etl-teaching-project.git`).

After cloning you should see a folder structure like this:

```
batch-etl-teaching-project/
├── data-generator/
├── docs/
├── emr-jobs/
├── glue-jobs/
├── infrastructure/
├── lambda-functions/
├── tests/
├── requirements.txt
└── README.md
```

### 4.2 Install Python packages

All required packages are listed in `requirements.txt`. Install them all with one command:

```bash
pip install -r requirements.txt
```

This may take a few minutes — PySpark is a large package.

**What each package does (in plain English):**

| Package | Version | What it does |
|---------|---------|--------------|
| `boto3` | 1.34.0 | The official AWS SDK for Python. Every script that talks to AWS (creates S3 buckets, starts Glue jobs, sends SNS messages) uses boto3. |
| `faker` | 20.1.0 | Generates realistic fake data — names, emails, phone numbers, addresses. Used by the data generator scripts to create test data that looks real. |
| `pandas` | 2.1.4 | The standard Python library for working with tabular data. Used for reading CSV files, writing Parquet files, and doing data manipulation locally. |
| `pyarrow` | 14.0.2 | A fast library for reading and writing Parquet format files. pandas uses it under the hood when you call `df.to_parquet()`. |
| `pyspark` | 3.4.2 | Apache Spark for Python. Lets you write the same distributed data processing code locally on your laptop that runs on Glue and EMR in the cloud. |
| `pytest` | 7.4.4 | The standard Python testing framework. Runs the automated tests in the `tests/` folder so you can verify your code is correct. |
| `moto` | 4.2.14 | A library that "mocks" AWS services for unit tests. When tests run, moto intercepts boto3 calls and simulates S3, Glue, etc. without making real AWS calls or incurring costs. |

**Verify the installation:**

```bash
python -c "import boto3, pyspark, pandas; print('All packages installed correctly')"
```

Expected output:

```
All packages installed correctly
```

---

## 5. Cost rules — IMPORTANT, read before touching AWS

> **Read this section before running any AWS commands.** AWS charges by the second for compute services. Most setup mistakes that cause unexpected bills happen because someone left a job running or forgot to disable a schedule.

The entire course is designed to run under **$10 per month**. The free-tier services (S3, Lambda, SNS, SQS, CloudWatch) cost nothing within normal usage. The paid services (Glue, EMR Serverless) cost roughly $3.50–$5.00 total if you follow these rules:

### Rule 1: Always set billing alarms first

The very first thing you do in Task 1 (after this setup guide) is create billing alarms at $5 and $9. Do not skip this step. The alarm at $9 is your emergency brake — it means stop everything and review what is running.

Details on how to set billing alarms are in `docs/01-infrastructure-manual-setup.md` and `docs/01-infrastructure-script-guide.md`.

### Rule 2: Never leave EMR applications in STARTED state when not using them

EMR Serverless applications have a "pre-initialized capacity" setting that keeps workers warm so jobs start faster. This is convenient but it costs money even when no jobs are running.

After you finish a learning session:
1. Go to the AWS Console → **EMR Serverless**
2. Find your application
3. Click **Stop application**

Or from the CLI:

```bash
aws emr-serverless stop-application --application-id <your-application-id>
```

### Rule 3: Always disable CloudWatch schedule rules after demos

When you learn about scheduled triggers (Task 5), you create a CloudWatch Event Rule that runs on a cron schedule. If you leave it enabled, it will keep triggering Glue or EMR jobs every hour (or however often you set it), running up costs while you sleep.

After testing a schedule:
1. Go to AWS Console → **CloudWatch** → **Events** → **Rules**
2. Select your rule and click **Disable**

Or from the CLI:

```bash
aws events disable-rule --name <your-rule-name>
```

### Rule 4: Delete test data from S3 regularly

S3 storage is cheap (about $0.023 per GB per month) and the first 5 GB is free. But if you generate large datasets repeatedly without cleaning up, it adds up. The infrastructure scripts set up lifecycle policies to auto-delete data in the landing bucket after 7 days. You can also delete manually:

```bash
aws s3 rm s3://your-bucket-name/landing/ --recursive
```

### Rule 5: Check AWS Cost Explorer daily while learning

While you are actively working through this course, spend 30 seconds each day looking at your current costs:

1. Go to AWS Console → search for **Cost Explorer** → click **Cost Explorer**
2. Look at **Costs by service** for the current month
3. If any service shows unexpected charges, investigate immediately

### Rule 6: Hard stop — if your bill approaches $8, stop all jobs and review

If you check Cost Explorer and your monthly bill is approaching $8 (before the $9 alarm fires):

1. Go to EMR Serverless and stop all applications
2. Go to CloudWatch Events and disable all rules
3. Check S3 for large files you can delete
4. Check Glue for any jobs that might be stuck in running state
5. Review Cost Explorer by service to identify what is causing the charges

This is not expected to happen if you follow the rules above, but treat it as your emergency protocol.

---

## 6. AWS region to use

### Always use us-east-1 (N. Virginia)

Every AWS resource you create in this course must be in the **us-east-1** region. Do not use any other region.

**Why us-east-1?**

- It is the oldest and most complete AWS region. All AWS services are available here.
- EMR Serverless is not available in every region. us-east-1 always has it.
- AWS Glue, Lambda, SNS, SQS, CloudWatch are all available in us-east-1.
- AWS documentation examples almost always use us-east-1.
- Free tier limits apply per region — using a single region keeps things simple.

### How to set your default region in the AWS Console

The AWS Console shows you resources in the currently selected region. Make sure you always have us-east-1 selected:

1. Open the AWS Console at https://console.aws.amazon.com
2. Look at the top-right corner — you will see a region name like "Oregon" or "Ireland"
3. Click that name to open the region dropdown
4. Select **US East (N. Virginia)** — this is us-east-1
5. The page will reload showing resources in us-east-1

> **Common mistake:** Students create resources in one region, then switch to another and cannot find them. If something you created seems to have disappeared, check that you are in us-east-1.

### How to set your default region in the CLI

You already set this in Section 3.4 when you ran `aws configure`. To verify:

```bash
aws configure get region
```

Expected output:

```
us-east-1
```

If it shows something else, run `aws configure` again and set the region to `us-east-1`.

You can also set it for a single command using the `--region` flag:

```bash
aws s3 ls --region us-east-1
```

---

## 7. Troubleshooting common setup issues

| Error | Likely cause | Fix |
|-------|-------------|-----|
| `aws: command not found` | AWS CLI is not installed or not in your terminal's PATH | Reinstall AWS CLI from https://aws.amazon.com/cli/. After installing, open a **new** terminal window. On Windows, restart your PC if the issue persists. |
| `Unable to locate credentials` | You have not run `aws configure`, or your credentials file is missing | Run `aws configure` and enter your Access Key ID, Secret Access Key, region (`us-east-1`), and output format (`json`). |
| `An error occurred (InvalidClientTokenId) when calling the GetCallerIdentity operation` | The access key ID is wrong or the key has been deleted | Go to AWS Console → IAM → Users → `etl-course-student` → Security credentials → check that the access key is active. If in doubt, create a new key and run `aws configure` again. |
| `An error occurred (AuthFailure): AWS was not able to validate the provided access credentials` | The secret access key does not match the access key ID | Run `aws configure` again and carefully re-enter both values. Make sure there are no extra spaces. |
| `python: command not found` | Python is not installed or not in your terminal's PATH | On Windows: reinstall Python and make sure **"Add Python to PATH"** is checked. Restart your terminal after installing. On Mac/Linux: try `python3` instead of `python`. |
| `pip install` fails with `Permission denied` or similar permissions error | Your user account does not have write access to the Python packages directory | Run with the `--user` flag: `pip install --user -r requirements.txt`. This installs packages into your home directory instead of the system directory. |
| `pip: command not found` | pip is not in your PATH, or Python is not installed | Try `python -m pip install -r requirements.txt` instead. If that also fails, reinstall Python (pip is bundled with Python 3.9+). |
| `git: command not found` | Git is not installed | Download and install from https://git-scm.com/downloads. Open a new terminal after installing. |
| `ModuleNotFoundError: No module named 'boto3'` | Python packages were not installed | Make sure you ran `pip install -r requirements.txt` from inside the project directory. Try running `pip list` to see what is installed. |
| `JAVA_HOME is not set` or PySpark won't start | PySpark requires Java, which is not installed | Download Java 11 or Java 17 from https://adoptium.net. After installing, set the `JAVA_HOME` environment variable. On Windows: search "Edit environment variables" in the Start menu. On Mac/Linux: add `export JAVA_HOME=$(/usr/libexec/java_home)` to your `~/.zshrc` or `~/.bashrc`. |
| AWS Console shows no resources after creating them | You are looking at the wrong region | Click the region name in the top-right corner of the AWS Console and switch to **US East (N. Virginia)** (us-east-1). |
| `botocore.exceptions.EndpointResolutionError` | Region or endpoint is wrong | Make sure `aws configure` shows `us-east-1`. Check with `aws configure get region`. |

---

## 8. Understanding your AWS credentials file

When you ran `aws configure`, the CLI created two hidden files on your machine. It is worth knowing what they contain so you can troubleshoot and manage multiple accounts in the future.

### 8.1 Where the files live

- **Mac / Linux:** `~/.aws/credentials` and `~/.aws/config`
- **Windows:** `C:\Users\YourName\.aws\credentials` and `C:\Users\YourName\.aws\config`

### 8.2 What credentials looks like

```ini
[default]
aws_access_key_id = AKIAIOSFODNN7EXAMPLE
aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

### 8.3 What config looks like

```ini
[default]
region = us-east-1
output = json
```

Both files use a "profile" system. The `[default]` profile is what the CLI uses when you do not specify otherwise. You can add named profiles for different accounts — but for this course you only need the default profile.

### 8.4 Reading credentials in Python (boto3)

When your Python scripts call `boto3.client(...)` or `boto3.resource(...)`, boto3 automatically reads credentials from the same `~/.aws/credentials` file. You do not need to hardcode credentials in your scripts.

```python
import boto3

# boto3 reads credentials from ~/.aws/credentials automatically
s3 = boto3.client("s3", region_name="us-east-1")
response = s3.list_buckets()
print(response["Buckets"])
```

This is the correct pattern. If you ever see credentials hardcoded in source code like this, it is a security problem:

```python
# WRONG — never do this
s3 = boto3.client(
    "s3",
    aws_access_key_id="AKIAIOSFODNN7EXAMPLE",      # NEVER hardcode keys
    aws_secret_access_key="wJalrXUtnFEMI/K7MDENG"  # NEVER hardcode secrets
)
```

---

## 9. Python virtual environments (optional but recommended)

A virtual environment isolates the Python packages for one project from all your other Python projects. This prevents version conflicts. For example, one project might need `pandas==1.5` while another needs `pandas==2.1`. Virtual environments keep them separate.

### 9.1 Create a virtual environment

Run this command from inside the project directory:

```bash
python -m venv venv
```

This creates a folder called `venv/` inside your project directory.

### 9.2 Activate the virtual environment

**Mac / Linux:**
```bash
source venv/bin/activate
```

**Windows (Command Prompt):**
```cmd
venv\Scripts\activate.bat
```

**Windows (PowerShell):**
```powershell
venv\Scripts\Activate.ps1
```

After activation, your terminal prompt will change to show `(venv)` at the beginning:

```
(venv) $ 
```

### 9.3 Install packages into the virtual environment

With the virtual environment active, install the project packages:

```bash
pip install -r requirements.txt
```

All packages go into `venv/` and do not affect any other Python installation on your machine.

### 9.4 Deactivate the virtual environment

When you are done working on the project:

```bash
deactivate
```

### 9.5 The venv folder is ignored by Git

The `.gitignore` file in this project already excludes the `venv/` folder. You should not commit virtual environment folders to Git — they are large and machine-specific.

---

## 10. Understanding IAM in more depth

This section gives you enough IAM background to understand what is happening when the course scripts create IAM roles. You do not need to memorize this — come back and re-read it when you encounter IAM-related errors.

### 10.1 The difference between users, groups, roles, and policies

**IAM User** — A person or application identity. Has a username, password (for Console), and/or access keys (for CLI/API). You created one: `etl-course-student`.

**IAM Group** — A collection of users. You attach policies to the group and every user in the group inherits those policies. Not used in this course but common in real companies.

**IAM Role** — An identity that can be assumed by an AWS service (or another user). Roles do not have permanent credentials — they get temporary credentials when assumed. Glue, EMR, and Lambda all use roles. You will create several roles in Task 1.

**IAM Policy** — A JSON document that says what actions are allowed or denied on which resources. Policies are attached to users, groups, or roles.

### 10.2 The principle of least privilege

In security, **least privilege** means giving an identity only the permissions it needs — no more. This limits the damage that can happen if credentials are compromised.

In this course you follow least privilege for service roles:
- The Glue service role can only access the S3 buckets this project creates. It cannot touch other buckets in your account.
- The Lambda function role can start Glue and EMR jobs but cannot delete IAM users or access billing.
- The EMR role can read and write S3 but cannot create new Lambda functions.

Your IAM user `etl-course-student` has broad permissions because it is used interactively during learning. In a real job, you would narrow these down.

### 10.3 What a policy document looks like

Here is a simplified example of an IAM policy that allows reading objects from a specific S3 bucket:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::my-etl-landing-bucket",
                "arn:aws:s3:::my-etl-landing-bucket/*"
            ]
        }
    ]
}
```

- `Effect`: Either `Allow` or `Deny`
- `Action`: What API calls are permitted (e.g., `s3:GetObject` = download a file)
- `Resource`: Which specific AWS resources the action applies to (by ARN — Amazon Resource Name)

### 10.4 IAM roles the course creates in Task 1

When you reach Task 1, the infrastructure scripts create these roles automatically. This table is here so you understand what is being created and why:

| Role name | Used by | Purpose |
|-----------|---------|---------|
| `BatchETL-GlueServiceRole` | AWS Glue | Allows Glue to read from and write to your S3 buckets, update the Glue Data Catalog, and write logs to CloudWatch |
| `BatchETL-EMRServerlessRole` | EMR Serverless | Allows EMR to read from and write to your S3 buckets and write logs to CloudWatch |
| `BatchETL-LambdaTriggerRole` | Lambda trigger functions | Allows Lambda to start Glue and EMR jobs, read S3 event metadata, and publish to SNS |
| `BatchETL-LambdaNotifierRole` | Lambda notifier function | Allows Lambda to read from and delete messages from SQS, and write to CloudWatch logs |
| `BatchETL-DataFetcherRole` | Lambda public dataset fetcher | Allows Lambda to write files to the landing S3 bucket only |

---

## 11. AWS Console navigation tips

If you are new to the AWS Console, here are some navigation patterns that will save you time throughout the course.

### 11.1 The search bar is your best friend

The AWS Console has dozens of services. Instead of hunting through menus, use the search bar at the top of the page. Just start typing the service name:

- Type `S3` → click S3
- Type `Glue` → click AWS Glue
- Type `EMR` → click Amazon EMR
- Type `Lambda` → click Lambda
- Type `IAM` → click IAM
- Type `CloudWatch` → click CloudWatch

### 11.2 Always check the region indicator

The region name is shown in the top-right corner of every page in the Console, to the left of your account name. Before creating or searching for any resource, confirm it says **N. Virginia** (us-east-1).

### 11.3 Recently visited services

The AWS Console keeps a list of recently visited services on the home page. After you visit S3, IAM, and CloudWatch once, they appear in a quick-access bar so you do not have to type them every time.

### 11.4 AWS Console shortcut for IAM user login

Once you have created the `etl-course-student` IAM user with console access, you can log in directly as that user without going through the root account. The IAM user sign-in URL looks like this:

```
https://<your-account-id>.signin.aws.amazon.com/console
```

You can find your account ID in the output of `aws sts get-caller-identity` (the `Account` field). Bookmark this URL so you never need to use the root account again.

---

## 12. Quick reference — commands you will use often

Bookmark this section. These are the AWS CLI commands you will run most frequently throughout the course.

### 12.1 Identity and credentials

```bash
# Confirm which user your CLI is authenticated as
aws sts get-caller-identity

# Show your current CLI configuration
aws configure list
```

### 12.2 S3

```bash
# List all S3 buckets in your account
aws s3 ls

# List contents of a specific bucket
aws s3 ls s3://your-bucket-name/

# List contents of a folder inside a bucket
aws s3 ls s3://your-bucket-name/landing/

# Upload a file to S3
aws s3 cp myfile.csv s3://your-bucket-name/landing/

# Download a file from S3
aws s3 cp s3://your-bucket-name/output/file.parquet ./

# Delete a file from S3
aws s3 rm s3://your-bucket-name/landing/myfile.csv

# Delete all files in a folder (use with caution)
aws s3 rm s3://your-bucket-name/landing/ --recursive
```

### 12.3 AWS Glue

```bash
# List all Glue jobs
aws glue list-jobs

# Get the status of a specific job run
aws glue get-job-run --job-name my-glue-job --run-id jr_abc123

# Start a Glue job manually
aws glue start-job-run --job-name my-glue-job

# List all job runs for a job
aws glue get-job-runs --job-name my-glue-job
```

### 12.4 EMR Serverless

```bash
# List all EMR Serverless applications
aws emr-serverless list-applications

# Start an EMR Serverless application (make it ready to accept jobs)
aws emr-serverless start-application --application-id <app-id>

# Stop an EMR Serverless application (IMPORTANT — do this when done)
aws emr-serverless stop-application --application-id <app-id>

# List job runs for an application
aws emr-serverless list-job-runs --application-id <app-id>
```

### 12.5 Lambda

```bash
# List all Lambda functions
aws lambda list-functions

# Invoke a Lambda function manually (for testing)
aws lambda invoke \
    --function-name my-function-name \
    --payload '{"key": "value"}' \
    output.json && cat output.json
```

### 12.6 CloudWatch Logs

```bash
# List all CloudWatch log groups
aws logs describe-log-groups

# Get recent log events from a log group
aws logs get-log-events \
    --log-group-name /aws/lambda/my-function \
    --log-stream-name <stream-name>

# Tail logs in real time (useful for debugging)
aws logs tail /aws/lambda/my-function --follow
```

### 12.7 Cost and billing

```bash
# Get a cost summary for the current month (requires Cost Explorer enabled)
aws ce get-cost-and-usage \
    --time-period Start=$(date +%Y-%m-01),End=$(date +%Y-%m-%d) \
    --granularity MONTHLY \
    --metrics "BlendedCost"
```

---

## 13. What each project folder contains

Here is a brief explanation of every top-level folder in the project so you know where to look for things:

```
batch-etl-teaching-project/
├── data-generator/         Scripts that create fake CSV, JSON, and Parquet files
│                           and upload them to S3.
│
├── docs/                   You are here. All student instruction guides are in
│                           this folder, named in the order you should read them.
│
├── emr-jobs/               PySpark scripts that run on EMR Serverless.
│                           Each subfolder is one ETL pattern.
│
├── glue-jobs/              PySpark scripts that run on AWS Glue.
│                           Each subfolder is one ETL pattern.
│
├── infrastructure/         Python scripts that create all AWS resources
│                           (S3 buckets, IAM roles, SNS, SQS, CloudWatch alarms).
│
├── lambda-functions/       Python code for Lambda functions:
│                           - s3_event_trigger: fires when a file arrives in S3
│                           - scheduled_trigger: fires on a cron schedule
│                           - notifier: processes job completion messages from SQS
│                           - public_data_fetcher: downloads public datasets to S3
│
├── public-datasets/        Helper scripts for downloading real-world datasets
│                           (e.g., NYC Taxi data) to S3.
│
├── tests/                  Automated tests. Run with: pytest tests/
│
├── requirements.txt        All Python package dependencies with pinned versions.
│
└── README.md               Short project overview and quick start guide.
```

---

## 14. Knowledge prerequisites — filling the gaps

### 14.1 Command line basics

If you are not comfortable with the terminal, here is the minimum you need:

```bash
# Print the current directory you are in
pwd

# List files in the current directory
ls          # Mac/Linux
dir         # Windows

# Change to a different directory
cd my-folder
cd ..          # go up one level

# Create a new directory
mkdir my-new-folder

# Run a Python script
python my_script.py

# Run a Python script with arguments
python my_script.py --rows 1000 --format csv
```

### 14.2 Basic Python you should know

The scripts in this project use standard Python. Here is a quick check — if these patterns look familiar, you are ready:

```python
# Reading a CSV file with pandas
import pandas as pd
df = pd.read_csv("data.csv")
print(df.head())

# Writing a Parquet file
df.to_parquet("output.parquet")

# Calling an AWS API with boto3
import boto3
s3 = boto3.client("s3")
s3.upload_file("local_file.csv", "my-bucket", "remote_key.csv")

# List comprehension
doubled = [x * 2 for x in range(10)]

# Dictionary
config = {"region": "us-east-1", "format": "parquet"}
```

If any of these look unfamiliar, review the Python and pandas documentation before starting. The course assumes you are comfortable with these patterns.

### 14.3 Basic SQL you should know

PySpark uses a DataFrame API that mirrors SQL concepts. If you know these SQL operations, you will find PySpark transformations easy to learn:

- `SELECT col1, col2 FROM table` → selecting columns
- `WHERE col > 100` → filtering rows
- `GROUP BY country` with `COUNT(*)`, `SUM(amount)` → aggregations
- `JOIN` → combining two tables on a common column

You will not write raw SQL in this course, but understanding these operations will make the PySpark code much easier to follow.

---

## 15. How to get help

### 15.1 AWS documentation

AWS has excellent official documentation:

- **AWS Glue developer guide:** https://docs.aws.amazon.com/glue/latest/dg/what-is-glue.html
- **EMR Serverless user guide:** https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html
- **boto3 documentation:** https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
- **PySpark documentation:** https://spark.apache.org/docs/latest/api/python/

### 15.2 Reading error messages

AWS error messages follow a predictable format. Here is how to read them:

```
An error occurred (AccessDeniedException) when calling the StartJobRun operation:
User: arn:aws:iam::123456789:user/etl-course-student is not authorized to perform:
glue:StartJobRun on resource: arn:aws:glue:us-east-1:123456789:job/my-glue-job
```

Breaking this down:
- `AccessDeniedException` — the error type. This always means an IAM permissions problem.
- `calling the StartJobRun operation` — the API call that failed.
- `User: arn:...` — which identity tried to perform the action.
- `is not authorized to perform: glue:StartJobRun` — the specific permission that is missing.
- `on resource: arn:...` — which specific resource was being accessed.

To fix this: go to IAM, find the user or role shown in the error, and add the missing permission (`glue:StartJobRun` in this example).

### 15.3 CloudWatch logs are your debugger

When a Lambda function or Glue/EMR job fails and you cannot see why, CloudWatch Logs is where to look.

For Lambda:
1. Go to AWS Console → Lambda → your function → Monitor tab → View CloudWatch logs

For Glue:
1. Go to AWS Console → Glue → Jobs → your job → Run tab → click the run ID → Output logs

For EMR Serverless:
1. Go to AWS Console → EMR Serverless → your application → Job runs → click the job run → Driver output

The error message in the logs almost always tells you exactly what went wrong.

---

## What you have now

At this point you should have:

- [x] Python 3.9+ installed and verified with `python --version`
- [x] pip installed and up to date
- [x] AWS CLI v2 installed and verified with `aws --version`
- [x] Git installed and verified with `git --version`
- [x] VS Code installed with Python and AWS Toolkit extensions
- [x] AWS free-tier account created
- [x] IAM user `etl-course-student` created with all 8 required policies attached
- [x] Access keys downloaded and stored securely (never committed to Git)
- [x] `aws configure` completed with your keys and `us-east-1` as the region
- [x] `aws sts get-caller-identity` returns your account ID and IAM user ARN
- [x] Repository cloned and `pip install -r requirements.txt` completed successfully
- [x] Cost rules understood — you know how to avoid unexpected charges

---

## Next step

You are ready to create the AWS infrastructure. Continue to:

**Option A (click in the AWS Console):** `docs/01-infrastructure-manual-setup.md`

**Option B (run Python scripts):** `docs/01-infrastructure-script-guide.md`

Both options produce the same result. Choose whichever fits your learning style. Beginners often start with Option A to understand what each resource looks like, then use Option B for future projects.
