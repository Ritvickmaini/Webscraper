import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor
import time

# ✅ MUST BE FIRST
st.set_page_config(page_title="Smart Contact Scraper", layout="wide")

# ✅ PING ROUTE FOR UPTIMEROBOT
query_params = st.query_params
if query_params.get("ping") == ["true"]:
    st.write("✅ App is alive!")
    st.stop()

# Regex patterns
EMAIL_REGEX = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
PHONE_REGEX = r'(\+?\d[\d\s\-\(\)]{7,}\d)'
SOCIAL_DOMAINS = ['facebook.com', 'linkedin.com', 'twitter.com', 'instagram.com', 'youtube.com']

# Streamlit UI
st.markdown("""
    <style>
        body { background-color: #0f1117; color: #f0f2f6; }
        .main { background-color: #0f1117; }
        .stApp { max-width: 1400px; margin: 0 auto; padding: 2rem; }
        h1, h2, h3, .stMarkdown { color: #00ffe0; }
        .css-1d391kg, .css-18e3th9 {
            background-color: #1c1f26 !important;
            border-radius: 10px !important;
            border: 1px solid #2e2e2e !important;
        }
        .stButton>button {
            background-color: #00ffe0;
            color: black;
            border-radius: 8px;
            font-weight: bold;
        }
    </style>
""", unsafe_allow_html=True)

st.markdown("<h1 style='text-align: center;'>🧠 Smart Contact Scraper</h1>", unsafe_allow_html=True)
st.markdown("Upload a CSV with company domains — this app scrapes **email addresses** and **UK phone numbers** from websites.")

uploaded_file = st.file_uploader("📤 Upload your CSV", type=['csv'])

def is_social_url(url):
    return any(social in url for social in SOCIAL_DOMAINS)

def is_uk_phone_number(number):
    number = re.sub(r'\D', '', number)
    if number.startswith('44'):
        number = '0' + number[2:]
    if number.startswith('0'):
        if number.startswith('01') or number.startswith('02'):
            return len(number) == 11
        elif number.startswith('07'):
            return len(number) == 11
    return False

def is_valid_phone(number):
    digits_only = re.sub(r'\D', '', number)
    if len(digits_only) < 8:
        return False
    if re.match(r'\d{4}[-/]\d{1,2}[-/]\d{1,2}', number):
        return False
    if re.match(r'\d{1,2}[-/]\d{1,2}[-/]\d{2,4}', number):
        return False
    return is_uk_phone_number(number)

def extract_contacts(url):
    if not isinstance(url, str) or is_social_url(url):
        return "", ""

    if not url.startswith("http"):
        url = "http://" + url

    try:
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')

        visible_text = " ".join(soup.stripped_strings)
        emails = re.findall(EMAIL_REGEX, visible_text)

        phone_numbers = set()
        for tag in ['header', 'footer']:
            section = soup.find(tag)
            if section:
                section_text = section.get_text(separator=" ", strip=True)
                raw_numbers = re.findall(PHONE_REGEX, section_text)
                cleaned = [n.strip() for n in raw_numbers if is_valid_phone(n)]
                phone_numbers.update(cleaned)

        return ", ".join(set(emails)), ", ".join(sorted(phone_numbers))
    except:
        return "Error", "Error"

def check_website_status_fast(url):
    if not isinstance(url, str):
        return "🔴 Inactive"
    try:
        if not url.startswith("http"):
            url = "http://" + url
        response = requests.head(url, timeout=5, allow_redirects=True)
        if response.status_code in [200, 301, 302]:
            return "🟢 Active"
        else:
            return "🔴 Inactive"
    except:
        return "🔴 Inactive"

def recheck_inactive_site(url):
    try:
        if not url.startswith("http"):
            url = "http://" + url
        response = requests.get(url, timeout=8)
        if response.status_code in [200, 301, 302]:
            return "🟢 Active"
        else:
            return "🔴 Inactive"
    except:
        return "🔴 Inactive"

if uploaded_file:
    try:
        df = pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"❌ Error reading CSV: {e}")
        st.stop()

    SOCIAL_KEYWORDS = ['linkedin', 'facebook', 'instagram', 'twitter', 'youtube']
    url_col = None
    for col in df.columns:
        col_lower = col.lower()
        if (
            any(kw in col_lower for kw in ['domain', 'website', 'url']) and
            not any(social in col_lower for social in SOCIAL_KEYWORDS)
        ):
            url_col = col
            break

    if not url_col:
        st.error("❌ Couldn't detect a valid URL column (e.g., 'company_domain', 'website', or 'url').")
        st.stop()

    st.success(f"✅ Detected URL column: `{url_col}`")
    st.subheader("📄 Uploaded File Preview")
    st.dataframe(df)

    urls = df[url_col].astype(str)

    # Step 1: Website status check
    with st.spinner("🔍 Checking which websites are active before scraping..."):
        with ThreadPoolExecutor(max_workers=100) as executor:
            status_list = list(executor.map(check_website_status_fast, urls))

        inactive_indices = [i for i, status in enumerate(status_list) if status == "🔴 Inactive"]
        re_urls = urls.iloc[inactive_indices].tolist()

        with ThreadPoolExecutor(max_workers=25) as executor:
            rechecked = list(executor.map(recheck_inactive_site, re_urls))

        for idx, new_status in zip(inactive_indices, rechecked):
            status_list[idx] = new_status

        df["Website Status"] = status_list
        df = df[df["Website Status"] == "🟢 Active"]

    df['Emails'] = ''
    df['Phone Numbers'] = ''
    urls = df[url_col].tolist()

    with st.spinner("⚙️ Scraping in progress..."):
        st.markdown("""
        <div class="loading-container">
            <div class="loader"></div>
            <div style="color: #00ffe0; font-weight: bold;">Scraping websites, please wait...</div>
        </div>
        """, unsafe_allow_html=True)
        time.sleep(0.5)
        with ThreadPoolExecutor(max_workers=50) as executor:
            results = list(executor.map(extract_contacts, urls))

    df['Emails'] = [res[0] for res in results]
    df['Phone Numbers'] = [res[1] for res in results]

    cols = df.columns.tolist()
    if url_col in cols:
        cols.remove('Emails')
        cols.remove('Phone Numbers')
        cols.insert(cols.index(url_col) + 1, 'Emails')
        cols.insert(cols.index(url_col) + 2, 'Phone Numbers')
    df = df[cols]

    filtered_df = df[
        ((df['Emails'].str.strip() != "") & (df['Emails'].str.strip() != "Error")) |
        ((df['Phone Numbers'].str.strip() != "") & (df['Phone Numbers'].str.strip() != "Error"))
    ]

    total = len(df)
    emails_found = sum(1 for e in df['Emails'] if e.strip() not in ["", "Error"])
    phones_found = sum(1 for p in df['Phone Numbers'] if p.strip() not in ["", "Error"])
    errors = sum(1 for e, p in zip(df['Emails'], df['Phone Numbers']) if e == "Error" or p == "Error")

    st.success("🎉 Scraping Completed")

    st.subheader("📄 Full Results (All Rows)")
    st.dataframe(df)

    st.subheader("📊 Filtered Results (With Emails or UK Phone Numbers)")
    st.dataframe(filtered_df)

    st.markdown("### 📈 Scraping Summary")
    st.markdown(f"""
    - 🏢 **Total Active Domains Processed:** `{total}`  
    - 📬 **Emails Found:** `{emails_found}`  
    - 📞 **UK Phone Numbers Found:** `{phones_found}`  
    - ⚠️ **Errors:** `{errors}`  
    """)

    def convert_df_to_csv(df_to_convert):
        return df_to_convert.to_csv(index=False).encode('utf-8')

    csv_filtered = convert_df_to_csv(filtered_df)
    csv_full = convert_df_to_csv(df)

    st.download_button(
        label="📥 Download Filtered CSV (Only Leads with Contacts)",
        data=csv_filtered,
        file_name='scraped_contacts_filtered.csv',
        mime='text/csv'
    )

    st.download_button(
        label="📄 Download Full CSV (All Results)",
        data=csv_full,
        file_name='scraped_contacts_full.csv',
        mime='text/csv'
    )
