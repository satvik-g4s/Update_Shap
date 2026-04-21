import streamlit as st
import pandas as pd
import numpy as np
from supabase import create_client

st.set_page_config(layout="wide")

st.title("Hour Recon Processor")

# =========================
# 🔑 CONFIG
# =========================
supabaseUrl = st.secrets["SUPABASE_URL"]
supabaseKey = st.secrets["SUPABASE_KEY"]

# =========================
# 🔌 CONNECTION
# =========================
def get_client():
    return create_client(supabaseUrl, supabaseKey)

# =========================
# 📌 COLUMN HEADERS
# =========================
HOUR_RECON_COLUMNS = """hub	location	zone_coc	owner	customer_code	customer_name	order_no	invoice_no	wf_taskid	period_from	period_to	attendance_number	shap_hrs	performed_hrs	billed_hrs	variance	branch_hrs	excess_paid	reliever_duty	excess_billing	short_billing	disciplinary_deduction	short_missing_roster	inter_assignment_adjustment	indirect_hours	training_ojt	complimentary_hrs	inter_hub_billing	inter_company_billing	billing_cycle_calc	billing_cycle_hrs_should_be	diff_with_bill_cycle	total_b	check_diff	bfl_remarks	ssc_query	status"""

SHAP_COLUMNS = """LocationCode	Client Code	SoNo	ShapHours	NormalHours	OTHours"""

# =========================
# 🧹 TRUNCATE
# =========================
def truncate_table(table_name):
    supabase = get_client()
    supabase.table(table_name).delete().neq("id", 0).execute()

def truncate_all():
    truncate_table("hour_recon")
    truncate_table("hour_recon_pivot")

# =========================
# 📥 MAIN UPLOAD
# =========================
def process_and_upload_excel_strict(file):
    supabase = get_client()

    df = pd.read_excel(file, header=1)

    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"[^\w]+", "_", regex=True)
    )

    df = df.drop(columns=["unnamed_38", "key"], errors="ignore")

    df = df.rename(columns={
        "attendance_as_per_billing_period": "attendance_number",
        "check_a_b": "check_diff",
        "ssc_query_if_any": "ssc_query",
    })

    for col in ["period_from", "period_to"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)

    df = df.replace({np.nan: None})
    df = df.drop_duplicates(subset=["location", "order_no", "wf_taskid"])

    for i in range(0, len(df), 500):
        batch = df.iloc[i:i+500].to_dict(orient="records")
        supabase.table("hour_recon").insert(batch).execute()

# =========================
# 📊 PIVOT
# =========================
def build_and_upload_pivot_from_cloud():
    supabase = get_client()

    all_data, start, limit = [], 0, 1000

    while True:
        res = supabase.table("hour_recon").select("*") \
            .range(start, start+limit-1).execute()
        if not res.data:
            break
        all_data.extend(res.data)
        start += limit

    df = pd.DataFrame(all_data)

    df = df.drop(columns=[
        "invoice_no","wf_taskid",
        "period_from","period_to",
        "bfl_remarks","ssc_query","id"
    ], errors="ignore")

    group_cols = ["location","customer_code","order_no"]

    agg_dict = {
        "customer_name":"first",
        "owner":"first"
    }

    num_cols = df.select_dtypes(include=["number"]).columns

    for col in num_cols:
        agg_dict[col] = "sum"

    pivot_df = df.groupby(group_cols, as_index=False).agg(agg_dict)

    pivot_df["pivot_key"] = (
        pivot_df["location"].astype(str) + "_" +
        pivot_df["customer_code"].astype(str) + "_" +
        pivot_df["order_no"].astype(str)
    )

    pivot_df = pivot_df.replace({np.nan: None})

    supabase.table("hour_recon_pivot").delete().neq("id",0).execute()

    for i in range(0,len(pivot_df),500):
        batch = pivot_df.iloc[i:i+500].to_dict(orient="records")
        supabase.table("hour_recon_pivot").insert(batch).execute()

# =========================
# 📤 SHAP
# =========================
def update_shap_hours_from_file(file):
    supabase = get_client()

    try:
        df = pd.read_csv(file) if file.name.endswith(".csv") else pd.read_excel(file)
    except Exception as e:
        st.error("Error reading SHAP file")
        st.stop()

    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"[^\w]+","_",regex=True)
    )

    df = df.rename(columns={
        "locationcode":"location",
        "client_code":"customer_code",
        "clientcode":"customer_code",
        "sono":"order_no",
        "shaphours":"shap_hours"
    })

    df["pivot_key"] = (
        df["location"].astype(str)+"_"+
        df["customer_code"].astype(str)+"_"+
        df["order_no"].astype(str)
    )

    df["shap_hours"] = pd.to_numeric(df["shap_hours"], errors="coerce")
    df = df.dropna(subset=["pivot_key","shap_hours"])

    df = df.groupby("pivot_key", as_index=False).agg({
        "location":"first",
        "customer_code":"first",
        "order_no":"first",
        "shap_hours":"sum"
    })

    pivot = supabase.table("hour_recon_pivot") \
        .select("pivot_key, shap_hours_i, shap_hours_ii, shap_hours_iii") \
        .execute().data

    pivot_df = pd.DataFrame(pivot)

    merged = df.merge(pivot_df, on="pivot_key", how="left")

    def assign(row):
        if pd.isna(row["shap_hours_i"]):
            return [row["shap_hours"], row["shap_hours_ii"], row["shap_hours_iii"]]
        elif pd.isna(row["shap_hours_ii"]):
            return [row["shap_hours_i"], row["shap_hours"], row["shap_hours_iii"]]
        elif pd.isna(row["shap_hours_iii"]):
            return [row["shap_hours_i"], row["shap_hours_ii"], row["shap_hours"]]
        else:
            raise Exception(f"❌ 4th upload not allowed: {row['pivot_key']}")

    merged[["shap_hours_i","shap_hours_ii","shap_hours_iii"]] = \
        merged.apply(lambda r: pd.Series(assign(r)), axis=1)

    full = supabase.table("hour_recon_pivot").select("*").execute().data
    full_df = pd.DataFrame(full)

    full_df = full_df.drop(columns=["shap_hours_i","shap_hours_ii","shap_hours_iii"], errors="ignore")

    final_df = full_df.merge(
        merged[["pivot_key","shap_hours_i","shap_hours_ii","shap_hours_iii"]],
        on="pivot_key",
        how="left"
    )

    final_df = final_df.replace({np.nan:None})
    final_df = final_df.drop(columns=["id"], errors="ignore")

    supabase.table("hour_recon_pivot").delete().neq("id",0).execute()

    for i in range(0,len(final_df),500):
        batch = final_df.iloc[i:i+500].to_dict(orient="records")
        supabase.table("hour_recon_pivot").insert(batch).execute()

# =========================
# 📥 DOWNLOAD
# =========================
def download_pivot():
    supabase = get_client()
    data = supabase.table("hour_recon_pivot").select("*").execute().data
    return pd.DataFrame(data)

# =========================
# 🌐 TABS
# =========================
tab1, tab2, tab3 = st.tabs(["SHAP + Download", "Hour Recon Upload", "Guidelines"])

# =========================
# TAB 1
# =========================
with tab1:
    st.subheader("SHAP Upload")

    shap_file = st.file_uploader("Upload SHAP File (.csv / .xlsx)", type=["csv","xlsx"])
    st.caption(SHAP_COLUMNS)

    if st.button("Run SHAP Upload"):
        if shap_file:
            try:
                update_shap_hours_from_file(shap_file)
                st.success("SHAP Uploaded")
            except Exception as e:
                st.error(str(e))
        else:
            st.error("Upload SHAP file")

    st.divider()

    if st.button("Download Pivot"):
        try:
            df = download_pivot()
            st.download_button("Download Report", df.to_csv(index=False), "pivot.csv")
        except Exception as e:
            st.error(str(e))

# =========================
# TAB 2
# =========================
with tab2:
    st.subheader("Hour Recon Upload")

    file = st.file_uploader("Upload Excel (.xlsx)", type=["xlsx"])
    st.caption(HOUR_RECON_COLUMNS)

    if st.button("Run Upload + Pivot"):
        if file:
            try:
                truncate_all()
                process_and_upload_excel_strict(file)
                build_and_upload_pivot_from_cloud()
                st.success("Uploaded + Pivot Built")
            except Exception as e:
                st.error(str(e))
        else:
            st.error("Upload file")

    st.divider()

    if st.button("Delete Tables"):
        try:
            truncate_all()
            st.warning("Tables Cleared")
        except Exception as e:
            st.error(str(e))

# =========================
# TAB 3
# =========================
with tab3:
    st.subheader("What This Tool Does")
    st.write("Processes Hour Recon data, builds pivot, and integrates SHAP hours.")

    st.subheader("How to Use")
    st.write("Upload files → Run → Download output")

    st.subheader("Output Details")
    st.write("Aggregated by Location → Customer → Order with SHAP splits.")

    st.subheader("Financial Logic")
    st.write("Variance = Billed Hours − Performed Hours. SHAP stored in 3 slots.")
