from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import requests
from bs4 import BeautifulSoup

def scrape_from_marketplace(page_number) -> pd.DataFrame:
    """Scraping data from the marketplace such as links, brands, products,
     prices, count of favorites, ratings, and rating scores."""

    URL = f'https://www.trendyol.com/cep-telefonu-x-c103498?pi={page_number}'
    r = requests.get(URL)

    if r.status_code == 200:
        html_content = r.content
        print(f"Page {page_number} response successfully returned.")
    else:
        print(f"Error on page {page_number}: {r.status_code}")
        return pd.DataFrame()

    soup = BeautifulSoup(r.text, 'html.parser')
    product_card = soup.find_all('div', class_='p-card-wrppr with-campaign-view')

    product_brand = [product.find('span', class_='prdct-desc-cntnr-ttl').text
                     if product.find('span', class_='prdct-desc-cntnr-ttl') else None
                     for product in product_card]
    product_names_list = [product.get('title') if product.get('title') else None for product in product_card]
    product_links = ["https://www.trendyol.com" + product.find("a")["href"]
                     if product.find("a") else None for product in product_card]
    product_prices = [
        product.find('div', class_='prc-box-dscntd').text if product.find('div', class_='prc-box-dscntd') else None
        for product in product_card]
    product_favorites = [
        product.find('p', class_='social-proof-text').text if product.find('p', class_='social-proof-text') else None
        for product in product_card]
    product_ratings = [
        product.find('span', class_='ratingCount').text if product.find('span', class_='ratingCount') else None
        for product in product_card]
    product_rating_scores = [
        product.find('span', class_='rating-score').text if product.find('span', class_='rating-score') else None
        for product in product_card]

    data = {
        'LINK': product_links,
        'BRAND': product_brand,
        'PRODUCT': product_names_list,
        'PRICE': product_prices,
        'COUNT OF FAVORITE': product_favorites,
        'RATING': product_ratings,
        'RATING SCORE': product_rating_scores
    }

    df = pd.DataFrame(data)
    return df

def extract_from_page() -> pd.DataFrame:
    """Extract the data from page."""
    combined_df = pd.DataFrame()
    for page_number in range(1, 168):
        df = scrape_from_marketplace(page_number)
        combined_df = pd.concat([combined_df, df], ignore_index=True)

    return combined_df

def load_page_into_csv() -> str:
    """Load the data into csv format."""
    extracted_data = extract_from_page()
    file = extracted_data.to_csv('C:/Users/MONSTER/PycharmProjects/first_ELT_project/trendyol_mobile_phones.csv'
                                 , index=False)

    return file


default_args = {
    'owner': 'Emirhan Alkan',
    'start_date': days_ago(0),
    'email': '[mremirhan131@gmai.com]',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DAG = DAG(
    'elt_data_pipeline',
    default_args=default_args,
    description='Apache Airflow DAG for ELT',
    schedule_interval=timedelta(days=1),
)

scrape_task = PythonOperator(
    task_id="scraping",
    python_callable=scrape_from_marketplace,  
    dag=DAG,
)

extract_task = PythonOperator(
    task_id="extract_from_page",
    python_callable=extract_from_page,  
    dag=DAG,
)

load_task = PythonOperator(
    task_id="load_page_into_csv",
    python_callable=load_page_into_csv,  
    dag=DAG,
)

extract_task >> load_task


