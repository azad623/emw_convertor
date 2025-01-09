import streamlit as st
import pandas as pd
from datetime import datetime
import glob
from streamlit_option_menu import option_menu
from emw_convertor.pipeline.pipeline_manager import pipeline_run
import os
import shutil
from io import BytesIO
from emw_convertor import log_output_path
from emw_convertor.pages.dashboard_manager import DashboardManager
from emw_convertor.getters.data_getter import load_excel_file
from emw_convertor.pages.dashboard import render_dashboard
from emw_convertor.pipeline.transformation import (
    drop_rows_with_missing_values,
)
from streamlit_navigation_bar import st_navbar

# Initialize Streamlit App
st.set_page_config(
    page_title="Excel-Verarbeitungssystem",
    page_icon="images/vanilla.png",
    layout="wide",
    initial_sidebar_state="expanded",
)
logs_folder = log_output_path


def read_log_file(file_path):
    """
    Reads the content of a log file.

    Args:
        file_path (str): Path to the log file.

    Returns:
        str: Content of the log file as a string.
    """
    try:
        with open(file_path, "r") as file:
            return file.read()
    except Exception as e:
        return f"Fehler beim Lesen der Datei: {e}"


def cleanup_session_folder():
    """Clean up the session folder when the session ends."""
    session_folder = st.session_state.get("session_folder")
    if session_folder and os.path.exists(session_folder):
        shutil.rmtree(session_folder)
        st.session_state["session_folder"] = None


def save_uploaded_file(uploaded_file, save_path):
    """Save an uploaded file to the specified path."""
    with open(save_path, "wb") as f:
        f.write(uploaded_file.getbuffer())


def convert_to_excel(df):
    """Convert a DataFrame to an in-memory Excel file."""
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df = sanitize_dataframe(df)
        df.to_excel(writer, index=False, sheet_name="Sheet1")
        print(buffer.getvalue)
    return buffer.getvalue()


def force_string_conversion(df):
    return df.map(lambda x: str(x) if x is not None else "")


def sanitize_dataframe(df):
    """
    Sanitize a DataFrame by:
    - Replacing NaN and pd.NA values with None.
    - Ensuring column names are unique.
    - Converting all data to JSON-compatible types.
    - Removing columns where all values are NaN or empty.
    """
    # Ensure unique column names
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        dup_indices = cols[cols == dup].index
        cols[dup_indices] = [
            f"{dup}_{i}" if i > 0 else dup for i in range(len(dup_indices))
        ]
    df.columns = cols

    # Replace problematic values with None
    df = df.replace([pd.NA, pd.NaT, float("nan"), float("inf"), -float("inf")], None)

    # Convert all data to JSON-compatible strings
    df = df.map(lambda x: str(x) if pd.notnull(x) else None)

    # Remove columns where all values are NaN or empty
    df.dropna(axis=1, how="all", inplace=True)

    return df


# Sidebar menu
with st.sidebar:
    (
        col1,
        col2,
        col3,
    ) = st.columns([3, 1, 1])
    with col1:
        st.image(
            "images/emw.svg.png", use_container_width=False, width=200
        )  # Adjust width as needed
    with col2:
        st.image("images/vanilla.png", use_container_width=False, width=60)
    # st.title("EMW/Vanilla Steel")
    selected_menu = option_menu(
        menu_title="Hauptmenü",
        options=[
            "Dashboard",
            "Excel-Dateien verarbeiten",
            "ETL-Protokolle",
            "Einstellungen",
        ],
        icons=["grid", "file-earmark-spreadsheet", "book", "gear"],
        menu_icon="cast",
        default_index=0,
    )

current_year = datetime.now().year
st.markdown(
    f"""
    <style>
        .main-footer {{
            position: fixed;
            bottom: 0;
            left: 0;
            right: 0;
            background-color: #f1f1f1;
            text-align: center;
            padding: 10px 0;
            font-size: 12px;
            color: gray;
        }}
        .css-1v3fvcr {{
            padding-bottom: 50px; /* Adjust to accommodate footer */
        }}
    </style>
    <div class="main-footer">
        © {current_year} Vanillasteel. All Rights Reserved.
    </div>
    """,
    unsafe_allow_html=True,
)

if selected_menu == "Dashboard":
    render_dashboard()
    # pass

elif selected_menu == "Excel-Dateien verarbeiten":
    st.markdown(
        "<h2 style='color: #003eff; font-family: 'Times New Roman', Times, serif;'>SLExA® Datenprozessorsystem</h2>",
        unsafe_allow_html=True,
    )

    if "uploaded_files" not in st.session_state:
        st.session_state.uploaded_files = {}

    uploaded_files_area = st.file_uploader(
        "Hier Excel-Dateien hochladen:",
        accept_multiple_files=True,
        type=["xlsx"],
        key="file_uploader",
    )

    if uploaded_files_area:
        session_folder = "inputs/tmp"
        for uploaded_file in uploaded_files_area:
            if uploaded_file.name not in st.session_state.uploaded_files:
                save_path = os.path.join(session_folder, uploaded_file.name)
                save_uploaded_file(uploaded_file, save_path)
                st.session_state.uploaded_files[uploaded_file.name] = {
                    "data": drop_rows_with_missing_values(
                        load_excel_file(save_path), threshold=0.7
                    ),
                    "path": save_path,
                    "status": "Hochgeladen",
                    "output": None,
                }

    if st.button("Alles löschen", key="clear_button"):
        st.session_state.uploaded_files.clear()
        cleanup_session_folder()

    if st.session_state.uploaded_files:
        tab_names = list(st.session_state.uploaded_files.keys())
        tabs = st.tabs(tab_names)

        for idx, tab_name in enumerate(tab_names):
            analysis_disabled = st.session_state.uploaded_files[tab_name].get(
                "analysis_disabled", False
            )
            with tabs[idx]:
                # st.markdown(f"### Bearbeitung: {tab_name}")

                # Determine which dataframe to display
                if st.session_state.uploaded_files[tab_name]["output"] is not None:
                    display_data = st.session_state.uploaded_files[tab_name]["data"]
                    display_data = pd.DataFrame(display_data.to_dict(orient="records"))
                    display_data = force_string_conversion(display_data)
                else:
                    display_data = st.session_state.uploaded_files[tab_name]["data"]
                    display_data = pd.DataFrame(display_data.to_dict(orient="records"))
                    display_data = force_string_conversion(display_data)

                # Sanitize DataFrame
                display_data = sanitize_dataframe(display_data)

                temp_path = st.session_state.uploaded_files[tab_name]["path"]

                st.markdown("### Zusätzliche Einstellungen")

                # Maintain the state of the checkbox and dropdown consistently
                same_value_key = f"same_value_{tab_name}"
                dimension_key = f"dimension_dropdown_{tab_name}"

                # Initialize session state for 'same_value'
                if same_value_key not in st.session_state:
                    st.session_state[same_value_key] = True

                st.session_state[f"same_value_{tab_name}"] = st.checkbox(
                    "Die Güte und die Dimension sind gleich.",
                    value=st.session_state[same_value_key],
                    key=f"same_value_checkbox_{tab_name}",
                )

                col1, col2, col3 = st.columns([3, 3, 2])
                with col1:
                    column_names = display_data.columns.tolist()
                    column_names.append("None")
                    default_value = "None"
                    grade_selection = st.selectbox(
                        "Wählen Sie die Spalte für Güte:",
                        options=column_names,
                        index=(
                            column_names.index(default_value)
                            if default_value in column_names
                            else 0
                        ),
                        key=f"grade_dropdown_{tab_name}",
                    )
                with col2:
                    dimension_selection = st.selectbox(
                        "Wählen Sie die Spalte für Dimensionen:",
                        options=column_names,
                        index=(
                            column_names.index(default_value)
                            if default_value in column_names
                            else 0
                        ),
                        disabled=st.session_state[same_value_key],
                        key=dimension_key,
                    )

                editable_df_1 = st.data_editor(
                    sanitize_dataframe(display_data),
                    key=f"editor_{tab_name}",
                    num_rows="dynamic",  # Allow adding/removing rows
                )

                # Analysis Button
                if st.button(
                    f"Analyse ausführen für {tab_name}",
                    key=f"run_{tab_name}",
                    disabled=analysis_disabled,
                ):
                    with st.spinner("Bearbeitungsbeleg"):
                        file_path = st.session_state.uploaded_files[tab_name]["path"]
                        etl_status, etl_output, etl_errors = pipeline_run(
                            header_names={
                                "grades": grade_selection,
                                "dimensions": (
                                    dimension_selection
                                    if not st.session_state.get(
                                        f"same_value_{tab_name}", False
                                    )
                                    else grade_selection
                                ),
                            },
                            file_path=file_path,
                        )

                        # Read the corresponding log file
                        log_file_path = os.path.join(
                            logs_folder,
                            f"{os.path.splitext(os.path.basename(tab_name))[0]}.error.log",
                        )
                        if os.path.exists(log_file_path):
                            log_content = read_log_file(log_file_path)
                            # Store the log content in session state
                            st.session_state.uploaded_files[tab_name][
                                "log"
                            ] = log_content
                        else:
                            st.session_state.uploaded_files[tab_name][
                                "log"
                            ] = "Keine Protokolldatei gefunden."

                    if etl_status:
                        sanitized_output = sanitize_dataframe(etl_output)
                        st.session_state.uploaded_files[tab_name][
                            "status"
                        ] = "Erfolgreich"
                        sanitized_output = pd.DataFrame(
                            sanitized_output.to_dict(orient="records")
                        )
                        sanitized_output = force_string_conversion(sanitized_output)
                        st.session_state.uploaded_files[tab_name][
                            "output"
                        ] = sanitized_output
                        # sanitized_output.to_excel(file_path, index=False)

                        # os.remove(file_path)
                        st.success(
                            f"ETL-Pipeline erfolgreich abgeschlossen für {tab_name}!"
                        )

                        dashboard = DashboardManager()
                        dashboard.save_process_results(
                            {
                                "filename": tab_name,
                                "supplier": "EMW",
                                "dataframe": etl_output,
                                "success": etl_status,
                            }
                        )

                        if etl_errors:
                            st.error(f"{etl_errors}")

                        with st.expander(label=f"Ergebnisse..", expanded=False):
                            editable_df_2 = st.data_editor(
                                sanitized_output,
                                key=f"editor_update_{tab_name}",
                                num_rows="dynamic",  # Allow adding/removing rows
                            )

                            # Download Button
                            st.download_button(
                                type="primary",
                                label="Excel Herunterladen",
                                data=convert_to_excel(sanitized_output),
                                file_name=f"{tab_name}_updated.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key=f"download_button_{tab_name}",
                                # on_click=set_download_state(tab_name)
                            )

                    else:
                        st.session_state.uploaded_files[tab_name][
                            "status"
                        ] = "Fehlgeschlagen"
                        st.error(f"ETL-Pipeline fehlgeschlagen für {tab_name}.")
                        st.error(f"{etl_errors}")

elif selected_menu == "ETL-Protokolle":
    st.markdown(
        "<h2 style='color: #003eff;font-family: 'Times New Roman', Times, serif;'>ETL-Protokolle</h2>",
        unsafe_allow_html=True,
    )

    st.info("Dies ist die Protokollübersicht der ETL-Verarbeitung.")

    uploaded_files = st.session_state.get("uploaded_files", {})
    log_files = [
        (file_name, file_info.get("log", "Keine Protokolldatei gefunden."))
        for file_name, file_info in uploaded_files.items()
        if file_info.get("log")
    ]

    if not log_files:
        st.warning(
            "Keine Protokolldateien vorhanden. Führen Sie die ETL-Pipeline aus, um Protokolle zu generieren."
        )
    else:
        tabs = st.tabs([file_name for file_name, _ in log_files])

        for idx, (file_name, log_content) in enumerate(log_files):
            with tabs[idx]:
                st.text_area(
                    label=f"Protokoll für {file_name}",
                    value=log_content,
                    height=300,
                    key=f"log_file_{idx}",
                )

elif selected_menu == "Einstellungen":
    st.markdown(
        "<h2 style='color: #003eff;font-family: 'Times New Roman', Times, serif;'>Konfigurationsseite</h2>",
        unsafe_allow_html=True,
    )
    st.subheader("Verwalten Sie Ihre Anwendungseinstellungen")

    # Abschnitte mit Expandern erstellen für eine übersichtliche Darstellung
    with st.expander("Ordnerverwaltung", expanded=True):
        st.write("Verwalten Sie unten die Ordner Ihrer Anwendung.")

        # Vorhandene Ordner auflisten
        folder_path = "inputs"  # Ersetzen Sie dies mit Ihrem Ordnerpfad
        if os.path.exists(folder_path):
            folders = [
                f
                for f in os.listdir(folder_path)
                if os.path.isdir(os.path.join(folder_path, f))
            ]
            if folders:
                selected_folder = st.selectbox(
                    "Wählen Sie einen Ordner zum Löschen aus:",
                    folders,
                    key="folder_select",
                )
                if st.button("Ausgewählten Ordner löschen", key="delete_folder"):
                    folder_to_delete = os.path.join(folder_path, selected_folder)
                    try:
                        shutil.rmtree(folder_to_delete)
                        st.success(
                            f"Der Ordner '{selected_folder}' wurde erfolgreich gelöscht."
                        )
                        os.makedirs(
                            os.path.join(folder_path, selected_folder), exist_ok=True
                        )
                    except Exception as e:
                        st.error(f"Fehler beim Löschen des Ordners: {e}")
            else:
                st.info("Es sind keine Ordner zum Löschen verfügbar.")
        else:
            st.warning(f"Der Ordnerpfad '{folder_path}' existiert nicht.")

    with st.expander("Weitere Einstellungen", expanded=False):
        st.write("Konfigurieren Sie unten zusätzliche Anwendungseinstellungen.")

        # Beispiel für Schalter und Schieberegler
        enable_feature = st.checkbox("Erweiterte Funktionen aktivieren", value=True)
        slider_value = st.slider(
            "Verarbeitungsschwelle festlegen:", min_value=0, max_value=100, value=50
        )

        # Einstellungen übernehmen
        if st.button("Einstellungen übernehmen", key="apply_settings"):
            st.success("Die Einstellungen wurden erfolgreich aktualisiert.")
            st.write(f"Erweiterte Funktionen aktiviert: {enable_feature}")
            st.write(f"Verarbeitungsschwelle: {slider_value}")

    with st.expander("Systeminformationen", expanded=False):
        st.write("Sehen Sie sich Systeminformationen an.")

        # Beispiel für Systeminformationen
        st.text(f"Aktuelles Arbeitsverzeichnis: {os.getcwd()}")
        st.text(f"Python-Version: {os.sys.version}")
