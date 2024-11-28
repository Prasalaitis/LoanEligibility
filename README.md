
# Loan Eligibility Dataset Project

This project programmatically downloads a loan eligibility dataset from Kaggle, processes it using Pandas, and sets up a PostgreSQL database with the processed data. The entire solution is Dockerized to ensure portability and ease of use.

## Pre-requisites

To run this project, you'll need the following installed on your system:

- Docker

## Getting Started

Follow these steps to set up and run the project on your local machine.

### 1. Clone the Repository

First, clone this repository to your local machine:

```bash
git clone https://github.com/TuringCollegeSubmissions/egacio-DE2v2.3.5.git loan-eligibility-dataset
cd loan-eligibility-dataset
```

### 2. Set Up Environment Variables

Create a `.env` file in the root of the project with the following content:

```env
#KAGGLE
KAGGLE_USERNAME=""
KAGGLE_KEY=""

#POSTGRES
LOANS_DB_HOST=""
LOANS_DB_PORT=""
LOANS_DB_NAME=""
LOANS_DB_USER=""
LOANS_DB_PASSWORD=""
```

Replace the placeholder values with your actual credentials and API key.

### 3. Build and Run Docker Containers

Build the Docker images and run the containers using Docker Compose:

```bash
docker-compose up --build
```

This command will:

1. Build the Docker image for the Python script.
2. Start the PostgreSQL database container.
3. Run the Python script to download, process the dataset, and populate the PostgreSQL database.

### 4. Pull Docker Image from Docker Hub

Alternatively, you can pull the pre-built Docker image from Docker Hub:

```bash
docker pull prasalaitis/loansv1:loans-app
```

## Project Structure

The project directory contains the following files:

```
loan-eligibility-dataset/
│
│   ├── 235.ipynb
│   ├── docker-compose.yml
│   ├── Dockerfile
│   ├── main.py
│   ├── README.md
│   ├── requirements.txt
├── config/
│   ├── db_setup.py
│   ├── kaggle_setup.py
│   ├── logging_setup.py
├── database/
│   ├── connection_sqlalchemy.py
├── docs/
├── logging/
├── src/
│   ├── get_dataset.py
│   ├── push_dataset.py

```

### Scripts Overview

- **get_dataset.py**: Downloads the dataset from Kaggle and processes it into Pandas DataFrames.
- **push_dataset.py**: Sets up the PostgreSQL database and populates it with the processed data.
- **main.py**: Main script that orchestrates the downloading and database setup processes.

## Usage

After running the Docker containers, the PostgreSQL database will be populated with the loan eligibility dataset. You can connect to the database using your preferred database client with the credentials provided in the `.env` file.