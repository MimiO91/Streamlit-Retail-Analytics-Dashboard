# 📊 Retail Sales & Returns Dashboard
--------------------------------------

An interactive business intelligence dashboard built with **Streamlit**, **MySQL**, **Pandas**, and **Plotly** to analyze retail sales performance, customer behavior, and return rates across countries and products.


## 🚀 Features
----------------

- 🧭 **Filter by Country & Product** to view tailored KPIs
- 💰 **Key Metrics**: Revenue, Orders, Customers, Average Order Value, Units Sold
- 🌍 **Choropleth Map**: Visualize revenue geographically
- 📈 **Bar Charts**: Top countries, products, and customers by performance
- 🔄 **Return Rate Analysis**: Track returns and performance per country


## 🛠️ Tech Stack
-----------------

| Tool/Library     | Purpose                        |
|------------------|--------------------------------|
| **Python**       | Programming Language           |
| **Streamlit**    | Web App & Dashboard Interface  |
| **MySQL**        | Relational Database            |
| **Pandas**       | Data Wrangling                 |
| **Plotly**       | Interactive Visualizations     |


## 📂 Project Structure
------------------------

/retail-dashboard/
│
├── streamlit.app.py # Main Streamlit dashboard script
├── requirements.txt # List of Python dependencies
├── .gitignore # Files and folders to ignore in Git
├── README.md # Project documentation
├── SQL_queries
│ ├── 01_revenue_analysis.sql
│ └── ...
├── data/ # CSV exports (optional if used locally)
│ └── ...


## 🔧 Setup Instructions
------------------------

### 1. Clone the Repository

bash
git clone https://github.com/yourusername/retail-dashboard.git
cd retail-dashboard

### 2. Create and Activate Virtual Environment
bash
Copier
Modifier
python -m venv venv
source venv/Scripts/activate  # For Git Bash on Windows

### 3. Install Required Packages
bash
Copier
Modifier
pip install -r requirements.txt

### 4. Configure MySQL Connection
Edit the get_connection() function inside app.py with your local MySQL credentials:

python
Copier
Modifier
def get_connection():
    return mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="your_mysql_user",
        password="your_mysql_password",
        database="retail_db"
    )
⚠️ Never commit your credentials to GitHub. Use environment variables or a .env file for production.

### 5. Launch the Dashboard
bash
Copier
Modifier
streamlit run app.py

### Then open your browser to:

arduino
Copier
Modifier
http://localhost:8501

# 📈 Use Cases
---------------

## This dashboard can be used for:

Retail business intelligence

Customer segmentation and value analysis

Return rate monitoring

Data storytelling for non-technical stakeholders

Portfolio showcase for data analysts and BI developers

# 🙋‍♀️ Author 
------------
Reem Bouqueau
🎯 Data Analyst | BI Developer | CRM-Savvy

LinkedIn: https://www.linkedin.com/in/r-bouqueau/
GitHub: https://github.com/MimiO91/

# 🧾 License
-------------
This project is licensed under the MIT License.

# ✨ Acknowledgments
---------------------
Dataset inspired by the UCI Online Retail Data Set

Built as part of a professional data portfolio

Thanks to Streamlit, Pandas, MySQL, and Plotly for enabling rich dashboards

