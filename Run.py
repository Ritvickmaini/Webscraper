import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import validators
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

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
st.write("Upload a CSV with a `company_domain` column. This tool checks which websites are active, then extracts emails and UK phone numbers from both active and inactive websites.")

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

        start_time = time.time()  # Start time tracking

        # Using ThreadPoolExecutor to check website statuses concurrently
        with ThreadPoolExecutor(max_workers=70) as executor:
            futures = {executor.submit(is_website_active, domain): domain for domain in df[domain_col]}
            for i, future in enumerate(as_completed(futures)):
                try:
                    result = future.result()
                    active_status.append("Active" if result else "Inactive")
                except:
                    active_status.append("Inactive")
                status_bar.progress((i + 1) / len(df))

        df["Website Status"] = active_status

        st.subheader("📊 Summary")
        st.write(f"Total entries: {len(df)}")
        st.write(f"✅ Active websites: {len(df[df['Website Status'] == 'Active'])}")
        st.write(f"❌ Inactive/invalid/social: {len(df[df['Website Status'] != 'Active'])}")

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

        # Auto-start scraping right after status check
        st.subheader("🔄 Scraping Contacts...")
        scraping_bar = st.progress(0)
        results = []

        with ThreadPoolExecutor(max_workers=70) as executor:
            futures = {executor.submit(extract_contacts, domain): domain for domain in df[domain_col]}
            for i, future in enumerate(as_completed(futures)):
                try:
                    result = future.result()
                except:
                    result = {"Emails": "", "Phones": ""}
                results.append(result)
                scraping_bar.progress((i + 1) / len(futures))

        # Insert results immediately after domain column or at the end if domain column is not found
        if domain_col is not None:
            insert_at = df.columns.get_loc(domain_col) + 1
        else:
            insert_at = len(df.columns)

        # Adding Emails and Phones columns
        emails = [r["Emails"] for r in results]
        phones = [r["Phones"] for r in results]

        df.insert(insert_at, "Emails", emails)
        df.insert(insert_at + 1, "Phone Numbers", phones)

        # Check if 'Website Status' column exists
        if "Website Status" in df.columns:
            # If it exists, just update it
            df["Website Status"] = active_status
        else:
            # If it doesn't exist, insert it
            df.insert(insert_at + 2, "Website Status", active_status)

        end_time = time.time()  # End time tracking
        elapsed_time = end_time - start_time  # Calculate elapsed time
        st.write(f"⏱️ Total Time Taken: {elapsed_time:.2f} seconds")

        st.subheader("📥 Final Results (Active and Inactive Websites)")
        st.dataframe(df)

        # Download final CSV with the updated columns
        st.download_button("⬇️ Download Full Results", df.to_csv(index=False), file_name="b2b_contact_scraper_results.csv")
