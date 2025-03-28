import streamlit as st
from emw_convertor.pages.dashboard_manager import DashboardManager
import pandas as pd
import plotly.express as px


def apply_custom_styles():
    st.markdown(
        """
        <style>
            /* General Styles */
            body {
                font-family: 'Arial', sans-serif;
                background-color: #f8f9fa;
            }
            .main-container {
                padding: 20px;
            }
            .dashboard-header {
                text-align: center;
                margin-bottom: 40px;
                font-size: 24px;
                color: #343a40;
            }
            .dashboard-card {
                background: white;
                padding: 20px;
                border-radius: 10px;
                box-shadow: 0px 4px 6px rgba(0, 0, 0, 0.1);
                margin-bottom: 20px;
            }
            .metric-card {
                display: flex;
                justify-content: space-between;
                align-items: center;
                background: #ffffff;
                border-radius: 10px;
                padding: 15px 20px;
                box-shadow: 0px 4px 6px rgba(0, 0, 0, 0.1);
                margin-bottom: 20px;
            }
            .metric-title {
                color: #6c757d;
                font-size: 14px;
            }
            .metric-value {
                font-size: 24px;
                color: #007bff;
                font-weight: bold;
            }
            .table-container {
                overflow-x: auto;
            }
        </style>
    """,
        unsafe_allow_html=True,
    )


def render_dashboard():
    """Render the dashboard with all statistics"""
    # Initialize manager
    dashboard = DashboardManager()

    # Apply custom styles from your original code
    apply_custom_styles()

    # Header with reset button
    col1, col2 = st.columns([6, 1])
    with col1:
        st.markdown(
            "<div class='dashboard-header'>📊 Excel-Verarbeitungsstatistiken</div>",
            unsafe_allow_html=True,
        )
    with col2:
        if st.button("🔄 Reset Stats"):
            dashboard.reset_database()
            st.success("Statistik erfolgreich zurückgesetzt!")
            st.rerun()

    # Get statistics
    stats = dashboard.get_dashboard_stats()

    # Display main metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(
            f"""
            <div class='metric-card'>
                <div>
                    <div class='metric-title'>Verarbeitete Dateien</div>
                    <div class='metric-value'>{stats['total_files']}</div>
                </div>
                <div>📁</div>
            </div>
        """,
            unsafe_allow_html=True,
        )
    with col2:
        st.markdown(
            f"""
            <div class='metric-card'>
                <div>
                    <div class='metric-title'>Zeilen Gesamt</div>
                    <div class='metric-value'>{stats['total_rows']:,}</div>
                </div>
                <div>📊</div>
            </div>
        """,
            unsafe_allow_html=True,
        )
    with col3:
        st.markdown(
            f"""
            <div class='metric-card'>
                <div>
                    <div class='metric-title'>Einzigartiger Lieferant</div>
                    <div class='metric-value'>{stats['unique_supplier']}</div>
                </div>
                <div>👥</div>
            </div>
        """,
            unsafe_allow_html=True,
        )

    # Value Distribution Graphs
    st.markdown(
        "<h5 style='color:#343a40;'>Wertverteilungen</h5>", unsafe_allow_html=True
    )

    if stats["frequencies"]:
        tabs = st.tabs(
            [
                "Güterverteilung",
                "Dickenverteilung",
                "Breitenverteilung",
                "Auflageverteilung",
            ]
        )

        for tab, (key, freq) in zip(tabs, stats["frequencies"].items()):
            with tab:
                if freq:
                    # Convert freq to DataFrame
                    df = pd.DataFrame(list(freq.items()), columns=["value", "count"])
                    print(df.head())

                    # Apply filters based on the key
                    if key.lower() == "dicke_":
                        df["value"] = df["value"].astype(str).str.replace(",", ".")
                        df["value"] = pd.to_numeric(df["value"], errors="coerce")
                        df = df[
                            (df["value"] >= 0.1) & (df["value"] <= 16.0)
                        ]  # Limit values to 0-16 mm for Dicke
                        print(f"After filtering ({key}):\n", df)

                    elif key.lower() == "breit_":
                        df["value"] = df["value"].astype(str).str.replace(",", ".")
                        df["value"] = pd.to_numeric(df["value"], errors="coerce")
                        df = df[
                            (df["value"] >= 0) & (df["value"] <= 2000)
                        ]  # Limit values to 0-2.0 mm for Breite
                        print(f"After filtering ({key}):\n", df)

                    # Add more conditions for other keys if needed
                    elif key.lower() == "auflage_":  # Ensure Auflage stays as string
                        df["value"] = df["value"].astype(str)
                        df["value"] = pd.Categorical(df["value"])
                    else:
                        df["value"] = df["value"].astype(str)
                    #     df = df[(df["value"] >= min_value) & (df["value"] <= max_value)]

                    # Plot the data
                    fig = px.bar(
                        df,
                        x="value",
                        y="count",
                        title=f"{key.title()} Distribution",
                        color="count",
                        color_continuous_scale="viridis",
                    )
                    if key.lower() == "auflage_":
                        fig.update_xaxes(type="category")
                    fig.update_layout(
                        paper_bgcolor="#f8f9fa",
                        plot_bgcolor="#f8f9fa",
                        font=dict(color="#343a40"),
                        margin=dict(t=50, b=30, l=30, r=30),
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info(f"Noch keine {key} Daten verfügbar")

    # Processing History
    st.markdown(
        "<h5 style='color:#343a40;'>Verarbeitungshistorie</h5>", unsafe_allow_html=True
    )

    if not stats["file_history"].empty:
        history_display = stats["file_history"][
            ["filename", "supplier", "upload_date", "rows_processed"]
        ]
        st.dataframe(
            history_display.style.format(
                {
                    "upload_date": lambda x: x.strftime("%Y-%m-%d %H:%M"),
                    "rows_processed": "{:,}".format,
                }
            ),
            use_container_width=True,
        )
    else:
        st.info("Noch keine Bearbeitungshistorie vorhanden")


# Main render function
def render():
    render_dashboard()
