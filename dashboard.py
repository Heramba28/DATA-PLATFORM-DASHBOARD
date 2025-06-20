import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import matplotlib.pyplot as plt
import seaborn as sns
import json
import io
import sqlite3
import requests
from datetime import datetime
from fpdf import FPDF
import base64
from collections import defaultdict
from pandas.api.types import is_numeric_dtype
import re
import traceback
from io import BytesIO
import tempfile

# Initialize session state
def init_session_state():
    session_state_defaults = {
        'original_df': None,
        'processed_df': None,
        'dashboards': {},
        'current_dashboard': None,
        'widget_counter': 0,
        'api_responses': {},
        'sql_queries': {},
        'smart_insights': None,
        'clean_history': [],
        'transform_history': [],
        'current_step': "📤 Upload"
    }
    for key, val in session_state_defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val

init_session_state()

# Helper functions
def to_excel_bytes(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
        writer.close()
    return output.getvalue()

def create_pdf(df, title="Data Report"):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)
    pdf.cell(200, 10, txt=title, ln=1, align='C')
    
    # Add table
    pdf.set_font("Arial", size=8)
    col_widths = [pdf.get_string_width(str(col)) + 6 for col in df.columns]
    
    # Header
    for i, col in enumerate(df.columns):
        pdf.cell(col_widths[i], 10, str(col), border=1)
    pdf.ln()
    
    # Data
    for _, row in df.head(100).iterrows():
        for i, col in enumerate(df.columns):
            pdf.cell(col_widths[i], 10, str(row[col]), border=1)
        pdf.ln()
    
    return pdf.output(dest='S').encode('latin1')

def generate_smart_insights(df):
    """Generate automated insights about the data"""
    insights = defaultdict(list)
    
    # Numeric columns analysis
    numeric_cols = df.select_dtypes(include='number').columns
    for col in numeric_cols:
        stats = {
            'mean': df[col].mean(),
            'median': df[col].median(),
            'std': df[col].std(),
            'min': df[col].min(),
            'max': df[col].max(),
            'skew': df[col].skew(),
            'kurtosis': df[col].kurtosis()
        }
        insights['numeric_stats'].append({col: stats})
        
        # Detect outliers
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        outlier_count = ((df[col] < (q1 - 1.5*iqr)) | (df[col] > (q3 + 1.5*iqr))).sum()
        if outlier_count > 0:
            insights['outliers'].append(f"{col}: {outlier_count} outliers detected")
    
    # Categorical columns analysis
    cat_cols = df.select_dtypes(include=['object', 'category']).columns
    for col in cat_cols:
        value_counts = df[col].value_counts(normalize=True)
        insights['categorical_stats'].append({
            col: {
                'unique_values': len(value_counts),
                'most_common': value_counts.index[0] if len(value_counts) > 0 else "N/A",
                'frequency': f"{value_counts.iloc[0]*100:.1f}%" if len(value_counts) > 0 else "N/A"
            }
        })
    
    # Correlations
    if len(numeric_cols) > 1:
        corr = df[numeric_cols].corr().unstack().sort_values(ascending=False)
        corr = corr[corr != 1].head(3)
        insights['correlations'] = [f"{pair[0]} & {pair[1]}: {val:.2f}" for pair, val in corr.items()]
    
    # Missing values
    missing = df.isnull().sum()
    missing = missing[missing > 0]
    if len(missing) > 0:
        insights['missing_values'] = [f"{col}: {count} ({count/len(df)*100:.1f}%)" 
                                     for col, count in missing.items()]
    
    # Data quality issues
    for col in df.columns:
        if df[col].dtype == 'object':
            # Check for mixed data types
            type_counts = df[col].apply(type).value_counts()
            if len(type_counts) > 1:
                insights['data_issues'].append(f"{col}: Mixed data types detected")
            
            # Check for inconsistent formatting
            if df[col].str.contains(r'\d{4}-\d{2}-\d{2}', regex=True, na=False).any():
                if not pd.api.types.is_datetime64_any_dtype(df[col]):
                    insights['data_issues'].append(f"{col}: Date-like strings detected - consider converting to datetime")
    
    return dict(insights)

def fix_duplicate_columns(df):
    """Ensure column names are unique by adding suffixes to duplicates"""
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        cols[cols == dup] = [f"{dup}_{i}" if i != 0 else dup 
                            for i in range(sum(cols == dup))]
    df.columns = cols
    return df

def safe_convert_to_numeric(series):
    """Convert series to numeric, handling errors gracefully"""
    original = series.copy()
    converted = pd.to_numeric(series, errors='coerce')
    if converted.isna().any() and not original.isna().any():
        # Show warning about conversion issues
        failed_count = converted.isna().sum()
        st.warning(f"{failed_count} values couldn't be converted to numeric in column '{series.name}'")
    return converted

def clean_column_name(name):
    """Clean column names for better readability"""
    # Remove special characters and extra spaces
    name = re.sub(r'[^\w\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    # Convert to title case
    return name.title()

def display_data_preview(df):
    """Display data preview with enhanced formatting"""
    with st.expander("Data Preview", expanded=True):
        st.dataframe(df.head(10).style.set_properties(**{'background-color': '#f0f2f6'}))
        st.caption(f"Shape: {df.shape[0]} rows × {df.shape[1]} columns")
        
        # Column types summary
        col_types = pd.DataFrame({
            'Column': df.columns,
            'Type': df.dtypes.astype(str),
            'Missing (%)': (df.isnull().sum() / len(df) * 100).round(1)
        })
        st.dataframe(col_types)

# Page config
st.set_page_config(
    page_title="Universal Data Platform", 
    layout="wide",
    page_icon="📊"
)
st.title("🚀 Universal Data Platform")

# Sidebar with Smart Insights feature
with st.sidebar:
    st.header("🔍 Smart Insights")
    if st.session_state.processed_df is not None and st.button("Generate Smart Insights"):
        df = fix_duplicate_columns(st.session_state.processed_df.copy())
        with st.spinner("Analyzing data..."):
            st.session_state.smart_insights = generate_smart_insights(df)
    
    if st.session_state.smart_insights:
        st.subheader("Key Insights")
        
        # Overview metrics
        if 'numeric_stats' in st.session_state.smart_insights:
            num_cols = len([k for k in st.session_state.smart_insights['numeric_stats']])
            st.metric("Numeric Columns", num_cols)
            
        if 'categorical_stats' in st.session_state.smart_insights:
            cat_cols = len(st.session_state.smart_insights['categorical_stats'])
            st.metric("Categorical Columns", cat_cols)
            
        if 'missing_values' in st.session_state.smart_insights:
            missing_cols = len(st.session_state.smart_insights['missing_values'])
            st.metric("Columns with Missing Values", missing_cols, delta_color="inverse")
        
        # Detailed insights
        if 'outliers' in st.session_state.smart_insights and st.session_state.smart_insights['outliers']:
            with st.expander("⚠️ Outliers Detected"):
                for outlier in st.session_state.smart_insights['outliers']:
                    st.warning(outlier)
        
        if 'correlations' in st.session_state.smart_insights and st.session_state.smart_insights['correlations']:
            with st.expander("🔗 Top Correlations"):
                for corr in st.session_state.smart_insights['correlations']:
                    st.info(corr)
        
        if 'missing_values' in st.session_state.smart_insights and st.session_state.smart_insights['missing_values']:
            with st.expander("❌ Missing Values"):
                for missing in st.session_state.smart_insights['missing_values']:
                    st.error(missing)
                    
        if 'data_issues' in st.session_state.smart_insights and st.session_state.smart_insights['data_issues']:
            with st.expander("⚠️ Data Quality Issues"):
                for issue in st.session_state.smart_insights['data_issues']:
                    st.warning(issue)

# Navigation
steps = [
    "📤 Upload", 
    "🧹 Clean", 
    "🔧 Transform",
    "🔍 EDA Playground",
    "📊 Visualize"
]

# Navigation with persistent state
if 'current_step' not in st.session_state:
    st.session_state.current_step = steps[0]
    
# Sidebar navigation
st.sidebar.markdown("---")
st.sidebar.header("Navigation")
for step_name in steps:
    if st.sidebar.button(step_name, key=f"nav_{step_name}"):
        st.session_state.current_step = step_name

# ---------------------- UPLOAD ----------------------
if st.session_state.current_step == "📤 Upload":
    st.header("📤 Data Upload")
    
    # Sample datasets for quick start
    sample_datasets = {
        "Iris Dataset": "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv",
        "Titanic Dataset": "https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv",
        "Diamonds Dataset": "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/diamonds.csv"
    }
    
    col1, col2 = st.columns([3, 1])
    with col1:
        upload_option = st.radio("Select Data Source", 
                               ["CSV/Excel", "JSON", "SQL Database", "API", "Sample Dataset"])
    with col2:
        if upload_option == "Sample Dataset":
            selected_sample = st.selectbox("Choose Sample Dataset", list(sample_datasets.keys()))
    
    if upload_option == "CSV/Excel":
        file = st.file_uploader("Upload File", type=["csv", "xlsx", "xls"])
        if file:
            try:
                if file.name.endswith(".csv"):
                    df = pd.read_csv(file)
                else:
                    df = pd.read_excel(file)
                
                df = fix_duplicate_columns(df)
                st.session_state.original_df = df.copy()
                st.session_state.processed_df = df.copy()
                st.success("✅ File uploaded successfully!")
                display_data_preview(df)
                
            except Exception as e:
                st.error(f"Error: {str(e)}")
                st.error("Detailed error message:")
                st.code(traceback.format_exc())
    
    elif upload_option == "JSON":
        json_file = st.file_uploader("Upload JSON File", type=["json"])
        if json_file:
            try:
                data = json.load(json_file)
                if isinstance(data, list):
                    df = pd.json_normalize(data)
                else:
                    df = pd.DataFrame([data])
                
                df = fix_duplicate_columns(df)
                st.session_state.original_df = df.copy()
                st.session_state.processed_df = df.copy()
                st.success("✅ JSON parsed successfully!")
                display_data_preview(df)
            except Exception as e:
                st.error(f"Error parsing JSON: {str(e)}")
    
    elif upload_option == "SQL Database":
        st.subheader("SQL Database Connection")
        db_type = st.selectbox("Database Type", ["SQLite", "MySQL", "PostgreSQL"])
        
        if db_type == "SQLite":
            db_file = st.file_uploader("Upload SQLite DB", type=["db", "sqlite", "sqlite3"])
            if db_file:
                try:
                    # Save to temp file
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp:
                        tmp.write(db_file.getvalue())
                        tmp_path = tmp.name
                    
                    conn = sqlite3.connect(tmp_path)
                    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)['name'].tolist()
                    selected_table = st.selectbox("Select Table", tables)
                    
                    if st.button("Load Data"):
                        df = pd.read_sql(f"SELECT * FROM {selected_table}", conn)
                        df = fix_duplicate_columns(df)
                        st.session_state.original_df = df.copy()
                        st.session_state.processed_df = df.copy()
                        st.success("✅ Data loaded from SQLite!")
                        display_data_preview(df)
                except Exception as e:
                    st.error(f"SQL Error: {str(e)}")
    
    elif upload_option == "API":
        st.subheader("API Data Fetch")
        api_url = st.text_input("API Endpoint URL", "https://api.example.com/data")
        if st.button("Fetch Data"):
            try:
                with st.spinner("Fetching data from API..."):
                    response = requests.get(api_url)
                    if response.status_code == 200:
                        data = response.json()
                        df = pd.json_normalize(data)
                        df = fix_duplicate_columns(df)
                        st.session_state.original_df = df.copy()
                        st.session_state.processed_df = df.copy()
                        st.success("✅ API data fetched successfully!")
                        display_data_preview(df)
                    else:
                        st.error(f"API Error: {response.status_code} - {response.reason}")
            except Exception as e:
                st.error(f"API Error: {str(e)}")
    
    elif upload_option == "Sample Dataset":
        st.subheader("Sample Dataset")
        st.info(f"Loading {selected_sample} dataset...")
        try:
            url = sample_datasets[selected_sample]
            df = pd.read_csv(url)
            df = fix_duplicate_columns(df)
            st.session_state.original_df = df.copy()
            st.session_state.processed_df = df.copy()
            st.success(f"✅ {selected_sample} loaded successfully!")
            display_data_preview(df)
        except Exception as e:
            st.error(f"Error loading sample dataset: {str(e)}")

# ---------------------- CLEAN ----------------------
elif st.session_state.current_step == "🧹 Clean":
    st.header("🧹 Data Cleaning")
    if st.session_state.processed_df is None:
        st.warning("Please upload data first")
    else:
        df = fix_duplicate_columns(st.session_state.processed_df.copy())
        
        # Show data summary
        st.subheader("Data Summary")
        display_data_preview(df)
        
        # Cleaning options in tabs
        clean_tabs = st.tabs([
            "🧹 Missing Values", 
            "🔠 Data Types", 
            "♻️ Duplicates", 
            "✂️ Column Operations"
        ])
        
        with clean_tabs[0]:  # Missing Values
            st.subheader("Handle Missing Values")
            
            if st.checkbox("Show missing values summary", True):
                missing = df.isnull().sum().reset_index()
                missing.columns = ['Column', 'Missing Values']
                missing['Percentage'] = (missing['Missing Values'] / len(df) * 100).round(1)
                st.dataframe(missing)
            
            col1, col2 = st.columns(2)
            with col1:
                clean_method = st.selectbox("Handling Method", 
                                          ["Select...", "Drop NA", "Fill with Mean", "Fill with Median", 
                                           "Fill with Mode", "Fill with Custom Value"])
            
            if clean_method != "Select...":
                if clean_method == "Drop NA":
                    if st.button("Remove Missing Values"):
                        df = df.dropna()
                        st.session_state.clean_history.append("Dropped all rows with missing values")
                        st.success("Removed rows with missing values")
                        st.balloons()
                
                elif clean_method.startswith("Fill with"):
                    with col2:
                        selected_col = st.selectbox("Select Column", df.columns)
                    
                    if clean_method == "Fill with Mean":
                        fill_val = df[selected_col].mean()
                    elif clean_method == "Fill with Median":
                        fill_val = df[selected_col].median()
                    elif clean_method == "Fill with Mode":
                        fill_val = df[selected_col].mode()[0] if len(df[selected_col].mode()) > 0 else None
                    else:
                        fill_val = st.text_input("Custom Value")
                    
                    if st.button("Apply Fill"):
                        if fill_val is not None:
                            df[selected_col] = df[selected_col].fillna(fill_val)
                            st.session_state.clean_history.append(f"Filled missing values in '{selected_col}' with {fill_val}")
                            st.success(f"Filled missing values with {fill_val}")
                            st.balloons()
                        else:
                            st.error("Invalid fill value")
        
        with clean_tabs[1]:  # Data Types
            st.subheader("Convert Data Types")
            
            col1, col2 = st.columns(2)
            with col1:
                dtype_col = st.selectbox("Column to Convert", df.columns)
                current_type = str(df[dtype_col].dtype)
                st.info(f"Current type: {current_type}")
            
            with col2:
                new_type = st.selectbox("New Type", ["Select...", "str", "int", "float", "datetime", "category"])
                
                if new_type != "Select..." and st.button("Convert Data Type"):
                    try:
                        if new_type == "datetime":
                            df[dtype_col] = pd.to_datetime(df[dtype_col], errors='coerce')
                        elif new_type == "numeric":
                            df[dtype_col] = safe_convert_to_numeric(df[dtype_col])
                        else:
                            df[dtype_col] = df[dtype_col].astype(new_type)
                            
                        st.session_state.clean_history.append(f"Converted '{dtype_col}' from {current_type} to {new_type}")
                        st.success(f"Converted {dtype_col} to {new_type}")
                        st.balloons()
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
        
        with clean_tabs[2]:  # Duplicates
            st.subheader("Handle Duplicates")
            
            dup_count = df.duplicated().sum()
            st.info(f"Found {dup_count} duplicate rows")
            
            if dup_count > 0 and st.checkbox("Show duplicate rows"):
                st.dataframe(df[df.duplicated()].head(10))
            
            if st.button("Remove Duplicates"):
                df = df.drop_duplicates()
                st.session_state.clean_history.append(f"Removed {dup_count} duplicate rows")
                st.success(f"Removed {dup_count} duplicates")
                st.balloons()
        
        with clean_tabs[3]:  # Column Operations
            st.subheader("Column Operations")
            
            col_op = st.selectbox("Operation", 
                                 ["Select...", "Rename Column", "Delete Column", "Clean Column Names"])
            
            if col_op == "Rename Column":
                col1, col2 = st.columns(2)
                with col1:
                    old_name = st.selectbox("Select Column", df.columns)
                with col2:
                    new_name = st.text_input("New Column Name", old_name)
                
                if st.button("Rename") and old_name != new_name:
                    df = df.rename(columns={old_name: new_name})
                    st.session_state.clean_history.append(f"Renamed column '{old_name}' to '{new_name}'")
                    st.success(f"Renamed column '{old_name}' to '{new_name}'")
                    st.balloons()
            
            elif col_op == "Delete Column":
                columns_to_delete = st.multiselect("Select Columns to Delete", df.columns)
                
                if st.button("Delete Selected Columns") and columns_to_delete:
                    df = df.drop(columns=columns_to_delete)
                    st.session_state.clean_history.append(f"Deleted columns: {', '.join(columns_to_delete)}")
                    st.success(f"Deleted {len(columns_to_delete)} columns")
                    st.balloons()
            
            elif col_op == "Clean Column Names":
                if st.button("Clean All Column Names"):
                    new_columns = [clean_column_name(col) for col in df.columns]
                    df.columns = new_columns
                    st.session_state.clean_history.append("Cleaned all column names")
                    st.success("Column names cleaned")
                    st.balloons()
        
        # History of cleaning operations
        if st.session_state.clean_history:
            with st.expander("🔧 Cleaning History"):
                for i, op in enumerate(st.session_state.clean_history, 1):
                    st.write(f"{i}. {op}")
        
        # Action buttons
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            if st.button("💾 Save Changes"):
                st.session_state.processed_df = df.copy()
                st.success("Cleaning changes saved!")
        with col2:
            if st.button("🔄 Reset to Original"):
                st.session_state.processed_df = st.session_state.original_df.copy()
                st.session_state.clean_history = []
                st.success("Reset to original data")
        with col3:
            st.download_button(
                label="📥 Download Cleaned Data (CSV)",
                data=df.to_csv(index=False).encode('utf-8'),
                file_name="cleaned_data.csv",
                mime="text/csv"
            )

# ---------------------- TRANSFORM ----------------------
elif st.session_state.current_step == "🔧 Transform":
    st.header("🔧 Data Transformation")
    if st.session_state.processed_df is None:
        st.warning("Please upload and clean data first")
    else:
        df = fix_duplicate_columns(st.session_state.processed_df.copy())
        
        # Show data summary
        st.subheader("Data Preview")
        st.dataframe(df.head())
        
        # Transformation options
        st.subheader("Transformation Tools")
        operation = st.selectbox("Select Operation", 
                               ["Select...", "Filter", "Sort", "GroupBy", "Pivot", "Merge", "Split Column", "Combine Columns"])
        
        if operation == "Filter":
            col1, col2 = st.columns(2)
            with col1:
                col = st.selectbox("Column", df.columns)
                filter_type = st.selectbox("Filter Type", 
                                         ["Equals", "Contains", "Greater Than", "Less Than"])
            
            with col2:
                if filter_type == "Equals":
                    if pd.api.types.is_numeric_dtype(df[col]):
                        value = st.number_input("Value")
                    else:
                        value = st.text_input("Value")
                elif filter_type == "Contains":
                    value = st.text_input("Value")
                elif filter_type == "Greater Than":
                    value = st.number_input("Value")
                elif filter_type == "Less Than":
                    value = st.number_input("Value")
            
            if st.button("Apply Filter"):
                try:
                    if filter_type == "Equals":
                        df = df[df[col] == value]
                    elif filter_type == "Contains":
                        df = df[df[col].astype(str).str.contains(value, na=False)]
                    elif filter_type == "Greater Than":
                        df = df[df[col] > value]
                    elif filter_type == "Less Than":
                        df = df[df[col] < value]
                    
                    st.session_state.transform_history.append(f"Filtered '{col}' {filter_type} '{value}'")
                    st.success(f"Filter applied: {len(df)} rows remaining")
                    st.dataframe(df.head())
                except Exception as e:
                    st.error(f"Error: {str(e)}")
        
        elif operation == "Sort":
            col1, col2 = st.columns(2)
            with col1:
                sort_col = st.selectbox("Sort By", df.columns)
            with col2:
                asc = st.checkbox("Ascending", True)
            
            if st.button("Sort Data"):
                df = df.sort_values(sort_col, ascending=asc)
                st.session_state.transform_history.append(f"Sorted by '{sort_col}' {'ascending' if asc else 'descending'}")
                st.success("Data sorted")
                st.dataframe(df.head())
        
        elif operation == "GroupBy":
            col1, col2 = st.columns(2)
            with col1:
                group_col = st.selectbox("Group By", df.columns)
            with col2:
                agg_col = st.selectbox("Aggregate Column", df.select_dtypes(include='number').columns)
            
            agg_func = st.selectbox("Function", ["sum", "mean", "count", "min", "max"])
            
            if st.button("Apply GroupBy"):
                try:
                    df = df.groupby(group_col)[agg_col].agg(agg_func).reset_index()
                    st.session_state.transform_history.append(f"Grouped by '{group_col}' with {agg_func} of '{agg_col}'")
                    st.success("GroupBy applied")
                    st.dataframe(df)
                except Exception as e:
                    st.error(f"Error: {str(e)}")
        
        elif operation == "Pivot":
            col1, col2, col3 = st.columns(3)
            with col1:
                index_col = st.selectbox("Index", df.columns)
            with col2:
                columns_col = st.selectbox("Columns", df.columns)
            with col3:
                values_col = st.selectbox("Values", df.select_dtypes(include='number').columns)
            
            if st.button("Create Pivot Table"):
                try:
                    df = df.pivot_table(index=index_col, columns=columns_col, values=values_col, aggfunc='mean')
                    st.session_state.transform_history.append(f"Pivot table: index={index_col}, columns={columns_col}, values={values_col}")
                    st.success("Pivot table created")
                    st.dataframe(df.head())
                except Exception as e:
                    st.error(f"Error: {str(e)}")
        
        elif operation == "Split Column":
            col1, col2 = st.columns(2)
            with col1:
                split_col = st.selectbox("Column to Split", df.columns)
            with col2:
                delimiter = st.text_input("Delimiter", ",")
            
            new_col_names = st.text_input("New Column Names (comma separated)", "col1,col2")
            
            if st.button("Split Column"):
                try:
                    new_cols = [name.strip() for name in new_col_names.split(",")]
                    split_df = df[split_col].str.split(delimiter, expand=True, n=len(new_cols)-1)
                    split_df.columns = new_cols[:split_df.shape[1]]
                    
                    # Add new columns to DataFrame
                    df = pd.concat([df, split_df], axis=1)
                    
                    st.session_state.transform_history.append(f"Split column '{split_col}' by '{delimiter}'")
                    st.success("Column split completed")
                    st.dataframe(df.head())
                except Exception as e:
                    st.error(f"Error: {str(e)}")
        
        elif operation == "Combine Columns":
            col1, col2 = st.columns(2)
            with col1:
                col1_name = st.selectbox("First Column", df.columns)
            with col2:
                col2_name = st.selectbox("Second Column", df.columns)
            
            new_col_name = st.text_input("New Column Name", "combined")
            separator = st.text_input("Separator", " - ")
            
            if st.button("Combine Columns"):
                try:
                    df[new_col_name] = df[col1_name].astype(str) + separator + df[col2_name].astype(str)
                    st.session_state.transform_history.append(f"Combined '{col1_name}' and '{col2_name}' into '{new_col_name}'")
                    st.success("Columns combined")
                    st.dataframe(df.head())
                except Exception as e:
                    st.error(f"Error: {str(e)}")
        
        # History of transformations
        if st.session_state.transform_history:
            with st.expander("🔧 Transformation History"):
                for i, op in enumerate(st.session_state.transform_history, 1):
                    st.write(f"{i}. {op}")
        
        # Action buttons
        col1, col2 = st.columns(2)
        with col1:
            if st.button("💾 Save Transformation"):
                st.session_state.processed_df = df.copy()
                st.success("Transformation saved!")
        with col2:
            st.download_button(
                label="📥 Download Transformed Data (CSV)",
                data=df.to_csv(index=False).encode('utf-8'),
                file_name="transformed_data.csv",
                mime="text/csv"
            )

# ---------------------- EDA PLAYGROUND ----------------------
elif st.session_state.current_step == "🔍 EDA Playground":
    st.header("🔍 Exploratory Data Analysis Playground")
    if st.session_state.processed_df is None:
        st.warning("Please upload and process data first")
    else:
        df = fix_duplicate_columns(st.session_state.processed_df.copy())
        
        # Data summary
        st.subheader("Data Summary")
        display_data_preview(df)
        
        # EDA tools
        eda_tool = st.selectbox("Choose EDA Tool", 
                               ["Quick Analysis", "Interactive Analysis", "Correlation Analysis"])
        
        if eda_tool == "Quick Analysis":
            st.subheader("Automated Analysis")
            
            if st.button("Run Quick Analysis"):
                with st.spinner("Analyzing data..."):
                    insights = generate_smart_insights(df)
                    
                    st.subheader("Data Overview")
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Total Rows", len(df))
                    col2.metric("Total Columns", len(df.columns))
                    
                    num_cols = len(df.select_dtypes(include='number').columns)
                    cat_cols = len(df.select_dtypes(include=['object', 'category']).columns)
                    col3.metric("Numeric/Categorical", f"{num_cols}/{cat_cols}")
                    
                    if 'missing_values' in insights:
                        st.subheader("Missing Values")
                        for missing in insights['missing_values']:
                            st.error(missing)
                    
                    if 'outliers' in insights:
                        st.subheader("Outliers")
                        for outlier in insights['outliers']:
                            st.warning(outlier)
                    
                    if 'correlations' in insights:
                        st.subheader("Top Correlations")
                        for corr in insights['correlations']:
                            st.info(corr)
                    
                    st.subheader("Numeric Columns Distribution")
                    num_cols = df.select_dtypes(include='number').columns
                    if len(num_cols) > 0:
                        for col in num_cols:
                            fig, ax = plt.subplots()
                            sns.histplot(df[col], kde=True, ax=ax)
                            ax.set_title(f"Distribution of {col}")
                            st.pyplot(fig)
        
        elif eda_tool == "Interactive Analysis":
            st.subheader("Interactive Analysis")
            
            col1, col2 = st.columns([1, 3])
            with col1:
                analysis_type = st.selectbox("Analysis Type",
                                          ["Distribution", "Correlation", "Outliers", "Time Series"])
                
                x_axis = st.selectbox("X Axis", df.columns)
                if analysis_type != "Distribution":
                    y_axis = st.selectbox("Y Axis", df.columns)
                
                if analysis_type == "Correlation":
                    corr_method = st.selectbox("Method", ["pearson", "spearman"])
                    
                if st.button("Generate Visualization"):
                    try:
                        with col2:
                            if analysis_type == "Distribution":
                                fig = px.histogram(df, x=x_axis, marginal="box", title=f"Distribution of {x_axis}")
                            elif analysis_type == "Correlation":
                                fig = px.scatter(df, x=x_axis, y=y_axis, trendline="ols", 
                                               title=f"{x_axis} vs {y_axis}")
                                correlation = df[[x_axis, y_axis]].corr(method=corr_method).iloc[0,1]
                                st.info(f"Correlation: {correlation:.2f}")
                            elif analysis_type == "Outliers":
                                fig = px.box(df, y=x_axis, title=f"Boxplot of {x_axis}")
                            elif analysis_type == "Time Series":
                                fig = px.line(df, x=x_axis, y=y_axis, title=f"{y_axis} over {x_axis}")
                            
                            st.plotly_chart(fig, use_container_width=True)
                    except Exception as e:
                        st.error(f"Error creating visualization: {str(e)}")
        
        elif eda_tool == "Correlation Analysis":
            st.subheader("Correlation Analysis")
            numeric_cols = df.select_dtypes(include='number').columns.tolist()
            
            if len(numeric_cols) >= 2:
                st.info("Select columns for correlation analysis")
                selected_cols = st.multiselect("Select Columns", numeric_cols, default=numeric_cols[:2])
                
                if len(selected_cols) >= 2:
                    # Pair plot
                    if st.checkbox("Show Pair Plot"):
                        fig = px.scatter_matrix(df[selected_cols])
                        st.plotly_chart(fig, use_container_width=True)
                    
                    # Correlation matrix
                    st.subheader("Correlation Matrix")
                    corr_matrix = df[selected_cols].corr()
                    
                    fig, ax = plt.subplots(figsize=(10, 8))
                    sns.heatmap(corr_matrix, annot=True, cmap="coolwarm", ax=ax)
                    st.pyplot(fig)
                    
                    # Top correlations
                    st.subheader("Top Correlations")
                    corr = corr_matrix.unstack().sort_values(ascending=False)
                    corr = corr[corr != 1].drop_duplicates().head(5)
                    
                    for (col1, col2), value in corr.items():
                        st.info(f"{col1} & {col2}: {value:.2f}")
                else:
                    st.warning("Select at least 2 columns for correlation analysis")
            else:
                st.warning("Need at least 2 numeric columns for correlation analysis")

# ---------------------- VISUALIZE ----------------------
elif st.session_state.current_step == "📊 Visualize":
    st.header("📊 Data Visualization")
    if st.session_state.processed_df is None:
        st.warning("Please upload and process data first")
    else:
        df = fix_duplicate_columns(st.session_state.processed_df.copy())
        
        st.subheader("Chart Builder")
        col1, col2 = st.columns([1, 3])
        
        with col1:
            chart_type = st.selectbox("Chart Type", 
                                    ["Bar", "Line", "Scatter", "Pie", "Histogram", "Box", "Heatmap"])
            
            if chart_type != "Heatmap":
                x_axis = st.selectbox("X Axis", df.columns)
                if chart_type not in ["Histogram", "Box", "Pie"]:
                    y_axis = st.selectbox("Y Axis", df.columns)
                elif chart_type == "Pie":
                    y_axis = st.selectbox("Values", df.select_dtypes(include='number').columns)
            else:
                # For heatmap, we need to select multiple columns
                numeric_cols = df.select_dtypes(include='number').columns.tolist()
                if len(numeric_cols) < 2:
                    st.warning("Need at least 2 numeric columns for heatmap")
                else:
                    heatmap_cols = st.multiselect("Select Columns for Heatmap", numeric_cols, default=numeric_cols[:3])
            
            color = st.selectbox("Color By", [None] + list(df.columns))
            group_by = st.selectbox("Group By", [None] + list(df.columns)) if chart_type in ["Bar", "Line"] else None
            
            if st.button("Generate Chart"):
                try:
                    with col2:
                        if chart_type == "Bar":
                            if group_by:
                                fig = px.bar(df, x=x_axis, y=y_axis, color=group_by, barmode='group')
                            else:
                                fig = px.bar(df, x=x_axis, y=y_axis, color=color)
                        elif chart_type == "Line":
                            fig = px.line(df, x=x_axis, y=y_axis, color=group_by)
                        elif chart_type == "Scatter":
                            fig = px.scatter(df, x=x_axis, y=y_axis, color=color)
                        elif chart_type == "Pie":
                            fig = px.pie(df, names=x_axis, values=y_axis, color=color)
                        elif chart_type == "Histogram":
                            fig = px.histogram(df, x=x_axis, color=color)
                        elif chart_type == "Box":
                            fig = px.box(df, x=x_axis, y=y_axis, color=color)
                        elif chart_type == "Heatmap" and len(heatmap_cols) >= 2:
                            corr_matrix = df[heatmap_cols].corr()
                            fig = px.imshow(corr_matrix, text_auto=True, aspect="auto")
                        
                        st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Error creating chart: {str(e)}")
        
        st.subheader("Dashboard Builder")
        dashboard_name = st.text_input("Dashboard Name", "My Dashboard")
        if st.button("Add to Dashboard"):
            if dashboard_name:
                if dashboard_name not in st.session_state.dashboards:
                    st.session_state.dashboards[dashboard_name] = []
                
                st.session_state.dashboards[dashboard_name].append({
                    'type': 'chart',
                    'chart_type': chart_type,
                    'x_axis': x_axis if chart_type != "Heatmap" else None,
                    'y_axis': y_axis if chart_type not in ["Histogram", "Box", "Pie", "Heatmap"] else None,
                    'color': color,
                    'group_by': group_by
                })
                st.success(f"Added to dashboard '{dashboard_name}'")
            else:
                st.warning("Please enter a dashboard name")
        
        # Dashboard management
        if st.session_state.dashboards:
            st.subheader("Saved Dashboards")
            selected_dashboard = st.selectbox("Select Dashboard", list(st.session_state.dashboards.keys()))
            
            if selected_dashboard:
                widgets = st.session_state.dashboards[selected_dashboard]
                for i, widget in enumerate(widgets):
                    with st.container():
                        st.markdown(f"**Widget {i+1}**: {widget['chart_type']} Chart")
                        if widget['chart_type'] == "Bar":
                            fig = px.bar(df, x=widget['x_axis'], y=widget['y_axis'], color=widget['group_by'], barmode='group')
                        elif widget['chart_type'] == "Line":
                            fig = px.line(df, x=widget['x_axis'], y=widget['y_axis'], color=widget['group_by'])
                        elif widget['chart_type'] == "Scatter":
                            fig = px.scatter(df, x=widget['x_axis'], y=widget['y_axis'], color=widget['color'])
                        elif widget['chart_type'] == "Pie":
                            fig = px.pie(df, names=widget['x_axis'], values=widget['y_axis'], color=widget['color'])
                        elif widget['chart_type'] == "Histogram":
                            fig = px.histogram(df, x=widget['x_axis'], color=widget['color'])
                        elif widget['chart_type'] == "Box":
                            fig = px.box(df, x=widget['x_axis'], y=widget['y_axis'], color=widget['color'])
                        elif widget['chart_type'] == "Heatmap":
                            # Simplified for demo - would need to store columns
                            fig = px.imshow(df.corr(), text_auto=True, aspect="auto")
                        
                        st.plotly_chart(fig, use_container_width=True)
                        
                        if st.button(f"Remove Widget {i+1}", key=f"remove_{selected_dashboard}_{i}"):
                            st.session_state.dashboards[selected_dashboard].pop(i)
                            st.experimental_rerun()



# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("Built for data analysts")
st.sidebar.markdown("Some bugs might be occur")