import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import validators
from concurrent.futures import ThreadPoolExecutor
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
st.write("Upload a CSV with a `company_domain` column. This tool will check which websites are active, then extract emails and UK phone numbers from active ones.")

# Upload CSV
uploaded_file = st.file_uploader("📁 Upload CSV", type=["csv"])

if uploaded_file:
    df = pd.read_csv(uploaded_file)

    # Detect domain column
    domain_col = None
    for col in df.columns:
        if "domain" in col.lower():
            domain_col = col
            break

    if domain_col is None:
        st.error("❌ Could not detect a domain column. Please make sure your CSV contains a column with company domains (e.g., 'company_domain').")
    else:
        st.success(f"✅ Detected domain column: `{domain_col}`")

        # Display preview
        st.subheader("🔍 Preview of Uploaded Data")
        st.dataframe(df.head(10))

        # Function to check if website is active
        def is_website_active(url):
            try:
                if not url.startswith("http"):
                    url = "http://" + url
                response = requests.head(url, timeout=5)
                return response.status_code < 400
            except:
                return False

        # Filter out social media and invalid domains
        def is_valid_domain(domain):
            if not isinstance(domain, str):
                return False
            if not validators.domain(domain):
                return False
            social_keywords = ['facebook', 'linkedin', 'instagram', 'twitter', 'youtube']
            return not any(s in domain.lower() for s in social_keywords)

        # Check website statuses
        st.subheader("🌐 Checking Website Statuses")
        active_status = []
        status_bar = st.progress(0)

        valid_domains = [d for d in df[domain_col] if is_valid_domain(d)]

        for i, domain in enumerate(df[domain_col]):
            if not is_valid_domain(domain):
                active_status.append("Invalid or Social")
                continue

            if is_website_active(domain):
                active_status.append("Active")
            else:
                active_status.append("Inactive")
            status_bar.progress((i + 1) / len(df[domain_col]))

        df["Website Status"] = active_status

        # Filter for active websites only
        active_df = df[df["Website Status"] == "Active"].copy()

        st.subheader("📊 Summary")
        st.write(f"Total entries: {len(df)}")
        st.write(f"✅ Active websites: {len(active_df)}")
        st.write(f"❌ Inactive or invalid/social: {len(df) - len(active_df)}")

        # Scraping function
        def extract_contacts(domain):
            emails = set()
            phones = set()
            if not domain.startswith("http"):
                domain = "http://" + domain
            try:
                response = requests.get(domain, timeout=8)
                soup = BeautifulSoup(response.text, "html.parser")
                text = soup.get_text()
                # Extract emails
                found_emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
                for email in found_emails:
                    if not any(social in email for social in ['facebook', 'linkedin', 'instagram', 'youtube']):
                        emails.add(email)
                # Extract UK phone numbers
                found_phones = re.findall(r"(?:(?:\+44\s?7\d{3}|\(?07\d{3}\)?)\s?\d{3}\s?\d{3})", text)
                phones.update(found_phones)
            except:
                pass
            return {"Emails": ", ".join(emails), "Phones": ", ".join(phones)}

        if st.button("🚀 Start Scraping Contacts from Active Websites"):
            st.subheader("🔄 Scraping Contacts...")
            scraping_bar = st.progress(0)

            urls = active_df[domain_col].tolist()
            results = []

            with st.spinner("⚙️ Scraping in progress..."):
                with ThreadPoolExecutor(max_workers=50) as executor:
                    for i, result in enumerate(executor.map(extract_contacts, urls)):
                        results.append(result)
                        scraping_bar.progress((i + 1) / len(urls))

            emails = [res["Emails"] for res in results]
            phones = [res["Phones"] for res in results]
            active_df["Emails"] = emails
            active_df["Phone Numbers"] = phones

            # Show filtered result
            st.subheader("📥 Filtered Results (Active Websites Only)")
            st.dataframe(active_df[[domain_col, "Website Status", "Emails", "Phone Numbers"]])

            # Merge with full df
            final_df = df.merge(active_df[[domain_col, "Emails", "Phone Numbers"]], on=domain_col, how="left")

            st.subheader("📄 Final Results (Full CSV with Added Data)")
            st.dataframe(final_df.head(20))

            # Downloads
            st.download_button("⬇️ Download Filtered Results", active_df.to_csv(index=False), file_name="filtered_results.csv")
            st.download_button("⬇️ Download Full Results", final_df.to_csv(index=False), file_name="full_results.csv")
