import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import validators
from concurrent.futures import ThreadPoolExecutor, as_completed

# App config
st.set_page_config(page_title="B2B Contact Scraper", layout="wide")

# Styles
st.markdown("""
<style>
    .main {background-color: #0e1117;}
    h1, h2, h3, h4, h5, h6, .stText, .stMarkdown {
        color: #00ffe0 !important;
    }
    .css-1v3fvcr {color: #00ffe0 !important;}
</style>
""", unsafe_allow_html=True)

# Title
st.title("\ud83d\udcec B2B Contact Scraper")
st.write("Upload a CSV with a `company_domain` column. This tool checks which websites are active, then extracts emails and UK phone numbers from active ones.")

# Upload CSV
uploaded_file = st.file_uploader("\ud83d\udcc1 Upload CSV", type=["csv"])

if uploaded_file:
    df = pd.read_csv(uploaded_file)

    # Detect domain column
    domain_col = next((col for col in df.columns if "domain" in col.lower()), None)

    if domain_col is None:
        st.error("\u274c Could not detect a domain column. Please include a column like 'company_domain'.")
    else:
        st.success(f"\u2705 Detected domain column: `{domain_col}`")
        st.subheader("\ud83d\udd0d Preview of Uploaded Data")
        st.dataframe(df.head(10))

        def is_valid_domain(domain):
            if not isinstance(domain, str):
                return False
            if not validators.domain(domain):
                return False
            blocked = ['facebook', 'linkedin', 'instagram', 'twitter', 'youtube']
            return not any(s in domain.lower() for s in blocked)

        def is_website_active(domain):
            try:
                if not domain.startswith("http"):
                    domain = "http://" + domain
                response = requests.head(domain, timeout=5)
                return response.status_code < 400
            except:
                return False

        def extract_contacts(domain):
            emails = set()
            phones = set()
            try:
                if not domain.startswith("http"):
                    domain = "http://" + domain
                response = requests.get(domain, timeout=8)
                soup = BeautifulSoup(response.text, "html.parser")
                text = soup.get_text()
                found_emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}", text)
                for email in found_emails:
                    if not any(s in email for s in ['facebook', 'linkedin', 'instagram', 'youtube']):
                        emails.add(email)
                found_phones = re.findall(r"(?:(?:\\+44\\s?7\\d{3}|\\(?07\\d{3}\\)?)\\s?\\d{3}\\s?\\d{3})", text)
                phones.update(found_phones)
            except:
                pass
            return {"Emails": ", ".join(emails), "Phones": ", ".join(phones)}

        st.subheader("\ud83c\udf10 Checking Website Statuses")
        status_bar = st.progress(0)

        # Parallelize status check
        domain_list = df[domain_col].tolist()

        def validate_and_check(domain):
            if not is_valid_domain(domain):
                return "Invalid or Social"
            return "Active" if is_website_active(domain) else "Inactive"

        with ThreadPoolExecutor(max_workers=70) as executor:
            status_futures = list(executor.map(validate_and_check, domain_list))

        df["Website Status"] = status_futures

        # Update progress bar manually
        for i in range(len(df)):
            status_bar.progress((i + 1) / len(df))

        active_df = df[df["Website Status"] == "Active"].copy()

        st.subheader("\ud83d\udcca Summary")
        st.write(f"Total entries: {len(df)}")
        st.write(f"\u2705 Active websites: {len(active_df)}")
        st.write(f"\u274c Inactive/invalid/social: {len(df) - len(active_df)}")

        if not active_df.empty:
            st.subheader("\ud83d\udd04 Scraping Contacts...")
            scraping_bar = st.progress(0)
            results = []

            with ThreadPoolExecutor(max_workers=70) as executor:
                futures = {executor.submit(extract_contacts, domain): domain for domain in active_df[domain_col]}
                for i, future in enumerate(as_completed(futures)):
                    try:
                        result = future.result()
                    except:
                        result = {"Emails": "", "Phones": ""}
                    results.append(result)
                    scraping_bar.progress((i + 1) / len(futures))

            # Insert results immediately after domain column
            emails = [r["Emails"] for r in results]
            phones = [r["Phones"] for r in results]
            insert_at = active_df.columns.get_loc(domain_col) + 1
            active_df.insert(insert_at, "Emails", emails)
            active_df.insert(insert_at + 1, "Phone Numbers", phones)

            st.subheader("\ud83d\udce5 Filtered Results (Active Websites Only)")
            st.dataframe(active_df[[domain_col, "Website Status", "Emails", "Phone Numbers"]])

            # Deduplicate before merging
            dedup_active_data = active_df[[domain_col, "Emails", "Phone Numbers"]].drop_duplicates(subset=domain_col)

            # Merge with full dataframe
            final_df = df.merge(dedup_active_data, on=domain_col, how="left")

            # Reorder merged columns
            insert_at = final_df.columns.get_loc(domain_col) + 1
            emails_col = final_df.pop("Emails")
            phones_col = final_df.pop("Phone Numbers")
            final_df.insert(insert_at, "Emails", emails_col)
            final_df.insert(insert_at + 1, "Phone Numbers", phones_col)

            st.subheader("\ud83d\udcc4 Final Results (Full CSV with Added Data)")
            st.dataframe(final_df.head(20))

            # Download buttons
            st.download_button("\u2b07\ufe0f Download Filtered Results", active_df.to_csv(index=False), file_name="filtered_results.csv")
            st.download_button("\u2b07\ufe0f Download Full Results", final_df.to_csv(index=False), file_name="full_results.csv")
