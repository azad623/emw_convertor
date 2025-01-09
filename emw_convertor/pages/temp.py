import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Define columns to analyze
columns_to_analyze = [
    "Güte_",
    "Auflage_",
    "Oberfläche_",
    "Dicke_",
    "Länge_",
    "Breit_",
]
st.markdown(
    "<h2 style='color: #003eff; font-family: 'Times New Roman', Times, serif;'>Dashboard Übersicht</h2>",
    unsafe_allow_html=True,
)

# File status summary
# Check if any file is processed
has_processed_files = any(
    file_info["status"] == "Erfolgreich"
    for file_info in st.session_state.get("uploaded_files", {}).values()
)

if not has_processed_files:
    st.info(
        "Es wurden noch keine Dateien verarbeitet. Führen Sie die ETL-Pipeline aus, um Statistiken zu sehen."
    )
else:
    # File status summary
    file_status_counts = {"Hochgeladen": 0, "Erfolgreich": 0, "Fehlgeschlagen": 0}
    processed_files_data = []

    for file_name, file_info in st.session_state.get("uploaded_files", {}).items():
        file_status_counts[file_info["status"]] += 1

        if file_info["status"] == "Erfolgreich" and file_info["output"] is not None:
            df = pd.DataFrame(file_info["output"])
            filled_counts = {
                col: (
                    df[col]
                    .apply(
                        lambda x: (
                            None if isinstance(x, str) and x.strip() == "" else x
                        )
                    )  # Replace empty strings with None
                    .notnull()
                    .sum()
                    if col in df.columns
                    else 0
                )
                for col in columns_to_analyze
            }

            filled_counts["gesamt"] = df.shape

            processed_files_data.append(
                {
                    "Dateiname": file_name,
                    "Status": file_info["status"],
                    **filled_counts,
                }
            )
        else:
            processed_files_data.append(
                {
                    "Dateiname": file_name,
                    "Status": file_info["status"],
                    **{col: None for col in columns_to_analyze},
                }
            )

    # Display statistics table
    st.markdown("<br><br>", unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1, 7, 1])
    with col2:

        stats_df = pd.DataFrame(processed_files_data)
        st.markdown(
            "<div style='text-align: left; font-size: 20px;'> Status und Statistiken der hochgeladenen Dateien </div><br>",
            unsafe_allow_html=True,
        )
        st.dataframe(stats_df)

    st.markdown("----")
    # Create bar chart for filled values


with st.expander("Detaillierte Analyse für hochgeladene Dateien", expanded=True):
    if has_processed_files:

        # Define columns to analyze
        columns_to_analyze = [
            "Güte_",
            "Auflage_",
            "Oberfläche_",
            "Dicke_",
            "Länge_",
            "Breit_",
        ]

        for file_name, file_info in st.session_state["uploaded_files"].items():
            if file_info["status"] == "Erfolgreich":
                df = file_info["output"]
