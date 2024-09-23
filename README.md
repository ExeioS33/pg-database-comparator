# PostgreSQL Database Comparator

Proof Of Concept
- 
A Python-based tool to compare database schema objects between two PostgreSQL servers (e.g., PROD vs. Staging). It helps track schema changes between different environments where Continuous Integration/Continuous Deployment (CI/CD) practices and versioning are not in place.

## Features

Using the `psycopg2` library, this tool can compare the following PostgreSQL objects:
- Indexes
- Tables
- Columns
- Functions
- Stored Procedures
- Triggers

### Purpose
- **Track Schema Changes:** Identify schema differences between production and other environments (e.g., staging).
- **No Versioning in Place?** Perfect for teams without CI/CD or versioning practices.
  
### Next Improvements:
- Enhance the connection handler for better flexibility.
- Improve the user interface (consider using a different GUI library).
- Expand database support beyond PostgreSQL.
- Refactor for better readability and follow best practices.
- Add containerization (e.g., Docker) for simplified deployment.

---

## Prerequisites

Before running this tool, ensure you have the following:
- **Python 3.8+** installed.
- **PostgreSQL** databases to compare.
- The following Python packages:
  - `psycopg2`
  - `tkinter`
  - `pytest` (for testing, optional)

## Project Structure

```commandline
project_root/
├── db/
│   ├── __init__.py
│   ├── src/
│   │   ├── DBConnectionHandler.py
│   │   ├── DBComparator.py
│   │   ├── DBObjects.py
│   └── tests/
│       └── test_DBConnectionHandler.py
├── gui/
│   └── gui_main.py
├── data/
│   └── ... (output CSV files)
├── main.py
├── README.md
└── requirements.txt

```

## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-repo/postgres-db-comparator.git
   cd postgres-db-comparator
    ```
   
2. **Clone the repository:**
   ```bash
    python -m venv venv
    source venv/bin/activate    # On Linux or macOS
    venv\Scripts\activate       # On Windows
    pip install -r requirements.txt
   ```

3. **Running**
   ```bash
    python main.py # without gui
    python gui/gui_main.py


## Notes
Based on the actual configs, it is needed to add the databases credentials in the *.pgpass* file to avoid password prompting everytime and to ensure some security best practices. 

e.g : 
- On Unix/Linux/MacOS: ~/.pgpass + chmod 600 ~/.pgpass
- On Windows: %APPDATA%\postgresql\pgpass.conf

example of content :
   ```bash
      hostname:port:database:username:password
      localhost:5432:mydatabase:myuser:mypassword
   ```
