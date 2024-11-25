import streamlit as st
import pandas as pd
from mitosheet.streamlit.v1 import spreadsheet
from etl_pipeline import run_etl_pipeline
from streamlit_option_menu import option_menu

# Initialize Streamlit App
st.set_page_config(
    page_title="Excel Processing System",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Sidebar menu for navigation
with st.sidebar:
    st.image(
        "https://media.licdn.com/dms/image/v2/C4D0BAQGtkiuGujzBJg/company-logo_200_200/company-logo_200_200/0/1647590698565/vanilla_steel_logo?e=2147483647&v=beta&t=_kzJxw1IBEPFvHxt8okz_ZUgnzkreVhLXqwS7XaZlwQ",
        width=100,
    )  # Add your company logo
    st.title("Company Name")  # Add your company name
    selected_menu = option_menu(
        menu_title="Main Menu",
        options=["Dashboard", "Process Excel Sheets", "ETL Logs", "Settings"],
        icons=["grid", "file-earmark-spreadsheet", "book", "gear"],
        menu_icon="cast",
        default_index=0,
    )

# Handle menu selection
if selected_menu == "Dashboard":
    st.title("Dashboard Overview")
    st.subheader("Summary of Uploaded Files")

    if "uploaded_files" in st.session_state:
        summary_data = {
            "File Name": list(st.session_state.uploaded_files.keys()),
            "Status": [
                file["status"] for file in st.session_state.uploaded_files.values()
            ],
        }
        summary_table = pd.DataFrame(summary_data)
        st.table(summary_table)

    st.subheader("Graphs and Analytics")
    col1, col2 = st.columns(2)
    with col1:
        st.bar_chart([3, 2, 1])
    with col2:
        st.line_chart([1, 2, 3])

elif selected_menu == "Process Excel Sheets":
    st.title("Excel Sheet Processor")

    # Temporary storage for uploaded files
    if "uploaded_files" not in st.session_state:
        st.session_state.uploaded_files = {}

    # File upload section
    st.markdown("### Upload Excel Files")
    uploaded_files_area = st.file_uploader(
        "Upload Excel files here:", accept_multiple_files=True, type=["xlsx"]
    )

    if uploaded_files_area:
        for uploaded_file in uploaded_files_area:
            if uploaded_file.name not in st.session_state.uploaded_files:
                st.session_state.uploaded_files[uploaded_file.name] = {
                    "data": pd.read_excel(uploaded_file),
                    "path": f"inputs/tmp/{uploaded_file.name}",
                    "status": "Uploaded",
                    "output": None,
                }
    # Buttons for processing all and clearing tabs
    row = st.columns([1, 4, 1])

    with row[0]:
        # Process all files at once
        if st.button("Process All Files"):
            # Prepare the paths for processing
            file_paths = [
                info["path"] for info in st.session_state.uploaded_files.values()
            ]

            # Run ETL pipeline for all files and collect outputs
            etl_results = run_etl_pipeline(file_paths)

            # Update session state with results
            for idx, tab_name in enumerate(st.session_state.uploaded_files.keys()):
                etl_status, etl_output, etl_errors = etl_results[idx]
                if etl_status:
                    st.session_state.uploaded_files[tab_name]["status"] = "Success"
                    st.session_state.uploaded_files[tab_name]["output"] = etl_output
                else:
                    st.session_state.uploaded_files[tab_name]["status"] = "Failed"
                    st.session_state.uploaded_files[tab_name]["output"] = etl_errors
    # Pagination for Tabs
    tab_names = list(st.session_state.uploaded_files.keys())
    num_tabs = len(tab_names)
    tabs_per_page = 5

    with row[1]:
        if num_tabs > tabs_per_page:
            num_pages = (num_tabs + tabs_per_page - 1) // tabs_per_page
            current_page = st.number_input(
                "navigate page",
                min_value=1,
                max_value=num_pages,
                value=1,
                step=1,
            )

            start_index = (current_page - 1) * tabs_per_page
            end_index = start_index + tabs_per_page
            paginated_tab_names = tab_names[start_index:end_index]
        else:
            paginated_tab_names = tab_names

    with row[2]:
        if st.button("Clear All Tabs", key="clear_tabs"):
            st.session_state.uploaded_files.clear()
            # st.experimental_rerun()

    # Create tabs for each uploaded file
    if st.session_state.uploaded_files:
        tab_names = list(st.session_state.uploaded_files.keys())
        tabs = st.tabs(paginated_tab_names)

        for idx, tab_name in enumerate(paginated_tab_names):
            with tabs[idx]:
                st.subheader(f"Processing: {tab_name}")
                file_data = st.session_state.uploaded_files[tab_name]["data"]
                temp_path = st.session_state.uploaded_files[tab_name]["path"]

                # Mito Table
                st.write("### Interactive Table (Mito)")
                mitosheet_result, _ = spreadsheet(
                    file_data, key=f"spreadsheet_{tab_name}"
                )
                edited_data = mitosheet_result.get("df1", None)

                if isinstance(edited_data, pd.DataFrame):
                    # Save the modified DataFrame back to the original file
                    edited_data.to_excel(temp_path, index=False)
                    st.session_state.uploaded_files[tab_name]["data"] = edited_data
                else:
                    st.error("Error: Could not extract data from Mito table.")
                    continue

                # Analysis Button
                if st.button(
                    f"Run ETL Pipeline for {tab_name}", key=f"analyze_{tab_name}"
                ):
                    st.write(f"Running ETL pipeline for {tab_name}...")
                    etl_status, etl_output, etl_errors = run_etl_pipeline(temp_path)

                    if etl_status:
                        etl_output = edited_data
                        st.success(
                            f"ETL pipeline completed successfully for {tab_name}!"
                        )
                        st.session_state.uploaded_files[tab_name]["status"] = "Success"
                        st.session_state.uploaded_files[tab_name]["output"] = etl_output
                    else:
                        st.error(
                            f"ETL pipeline failed for {tab_name}. Check the errors below."
                        )
                        st.session_state.uploaded_files[tab_name]["status"] = "Failed"
                        st.session_state.uploaded_files[tab_name]["output"] = etl_errors

                # Expandable Output Section
                with st.expander("Processed Output", expanded=True):
                    output_data = st.session_state.uploaded_files[tab_name]["output"]

                    if isinstance(output_data, pd.DataFrame):
                        st.write("Processed Output Table")
                        st.dataframe(output_data)

                        # Export to CSV
                        csv_output = output_data.to_csv(index=False)
                        st.download_button(
                            label="Download CSV",
                            data=csv_output,
                            file_name=f"{tab_name}_output.csv",
                            mime="text/csv",
                        )
                    elif output_data:
                        st.error("Errors during ETL processing:")
                        for error in output_data:
                            st.error(error)
                    else:
                        st.warning(
                            "No output available yet. Please run the ETL pipeline."
                        )

                # Collapsible Graph Section
                with st.expander("Visualize Output Data", expanded=False):
                    if isinstance(output_data, pd.DataFrame):
                        st.write("Graph View")
                        st.bar_chart(output_data.select_dtypes(include=["number"]))
                    else:
                        st.info("No data to visualize yet.")

elif selected_menu == "ETL Logs":
    st.title("ETL Logs")
    st.write("View and manage ETL logs here.")

elif selected_menu == "Settings":
    st.title("Settings")
    st.write("Configure application settings here.")
