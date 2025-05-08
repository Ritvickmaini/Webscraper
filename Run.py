import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import validators
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

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
st.title("📬 B2B Contact Scraper")
st.write("Upload a CSV with a `company_domain` column. This tool checks which websites are active, then extracts emails and UK phone numbers from active ones.")

# Upload CSV
uploaded_file = st.file_uploader("📁 Upload CSV", type=["csv"])

if uploaded_file:
    df = pd.read_csv(uploaded_file)

    # Detect domain column
    domain_col = next((col for col in df.columns if "domain" in col.lower()), None)

    if domain_col is None:
        st.error("❌ Could not detect a domain column. Please include a column like 'company_domain'.")
    else:
        st.success(f"✅ Detected domain column: `{domain_col}`")
        st.subheader("🔍 Preview of Uploaded Data")
        st.dataframe(df.head(10))

        def is_website_active(url):
            try:
                if not url.startswith("http"):
                    url = "http://" + url
                response = requests.head(url, timeout=5)
                return response.status_code < 400
            except:
                return False

        def is_valid_domain(domain):
            if not isinstance(domain, str):
                return False
            if not validators.domain(domain):
                return False
            blocked = ['facebook', 'linkedin', 'instagram', 'twitter', 'youtube']
            return not any(s in domain.lower() for s in blocked)

        st.subheader("🌐 Checking Website Statuses")
        active_status = []
        status_bar = st.progress(0)

        for i, domain in enumerate(df[domain_col]):
            if not is_valid_domain(domain):
                active_status.append("Invalid or Social")
            else:
                active_status.append("Active" if is_website_active(domain) else "Inactive")
            status_bar.progress((i + 1) / len(df))

        df["Website Status"] = active_status
        active_df = df[df["Website Status"] == "Active"].copy()

        st.subheader("📊 Summary")
        st.write(f"Total entries: {len(df)}")
        st.write(f"✅ Active websites: {len(active_df)}")
        st.write(f"❌ Inactive/invalid/social: {len(df) - len(active_df)}")

        def extract_contacts(domain):
            emails = set()
            phones = set()
            try:
                if not domain.startswith("http"):
                    domain = "http://" + domain
                response = requests.get(domain, timeout=8)
                soup = BeautifulSoup(response.text, "html.parser")
                text = soup.get_text()
                found_emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
                for email in found_emails:
                    if not any(s in email for s in ['facebook', 'linkedin', 'instagram', 'youtube']):
                        emails.add(email)
                found_phones = re.findall(r"(?:(?:\+44\s?7\d{3}|\(?07\d{3}\)?)\s?\d{3}\s?\d{3})", text)
                phones.update(found_phones)
            except:
                pass
            return {"Emails": ", ".join(emails), "Phones": ", ".join(phones)}

        if st.button("🚀 Start Scraping Contacts from Active Websites"):
            st.subheader("🔄 Scraping Contacts...")
            scraping_bar = st.progress(0)
            results = []
            futures = []

            with ThreadPoolExecutor(max_workers=70) as executor:
                futures = {executor.submit(extract_contacts, domain): domain for domain in active_df[domain_col]}

                for i, future in enumerate(as_completed(futures)):
                    try:
                        result = future.result()
                    except:
                        result = {"Emails": "", "Phones": ""}
                    results.append(result)
                    scraping_bar.progress((i + 1) / len(futures))

            # Insert columns right after domain_col
            emails = [r["Emails"] for r in results]
            phones = [r["Phones"] for r in results]
            insert_at = active_df.columns.get_loc(domain_col) + 1
            active_df.insert(insert_at, "Emails", emails)
            active_df.insert(insert_at + 1, "Phone Numbers", phones)

            st.subheader("📥 Filtered Results (Active Websites Only)")
            st.dataframe(active_df[[domain_col, "Website Status", "Emails", "Phone Numbers"]])

            # Merge with full df
            final_df = df.merge(
                active_df[[domain_col, "Emails", "Phone Numbers"]],
                on=domain_col,
                how="left"
            )

            # Move new columns after domain_col
            insert_at = final_df.columns.get_loc(domain_col) + 1
            emails_col = final_df.pop("Emails")
            phones_col = final_df.pop("Phone Numbers")
            final_df.insert(insert_at, "Emails", emails_col)
            final_df.insert(insert_at + 1, "Phone Numbers", phones_col)

            st.subheader("📄 Final Results (Full CSV with Added Data)")
            st.dataframe(final_df.head(20))

            st.download_button("⬇️ Download Filtered Results", active_df.to_csv(index=False), file_name="filtered_results.csv")
            st.download_button("⬇️ Download Full Results", final_df.to_csv(index=False), file_name="full_results.csv")
