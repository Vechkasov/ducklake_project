import streamlit as st
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from plotly import express as px
import numpy as np
from datetime import datetime, timedelta

def install_extensions(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        INSTALL ducklake;
        INSTALL postgres_scanner;
        INSTALL httpfs;
    """)

def create_secrets(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
    CREATE OR REPLACE SECRET minio_storage (
        TYPE S3,
        KEY_ID 'minioadmin',
        SECRET 'minioadmin',
        ENDPOINT '217.16.16.57:9000',
        REGION 'eu-central-1',
        USE_SSL false,
        URL_STYLE 'path',
        SCOPE 's3://ducklake/'
    );
    """)

    conn.execute("""
    CREATE OR REPLACE SECRET pg_meta (
        TYPE POSTGRES,
        HOST '217.16.16.57',
        PORT 5444,
        DATABASE 'ducklake_catalog',
        USER 'ducklake',
        PASSWORD 'ducklake'
    );
    """)

    conn.execute("""
    ATTACH 'ducklake:postgres:' AS lake (
        META_SECRET pg_meta,
        DATA_PATH 's3://ducklake/'
    );
    """)
    
    conn.execute("USE lake;")

st.set_page_config(page_title="Аналитика событий", layout="wide")
st.title("Аналитика пользовательских событий")

@st.cache_data
def load_data():
    with duckdb.connect() as conn:
        install_extensions(conn)
        create_secrets(conn)
    
        tables = {
            'events': 'mart.events_distribution_hourly',
            'purchases': 'mart.purchased_products_count_hourly', 
            'conversions': 'mart.click_conversion_rates_hourly',
            'campaigns': 'mart.campaign_purchase_analysis_hourly',
            'products': 'mart.top_performing_products',
            'users_segments': 'mart.users_segments_agg',
            'users_purchases': 'mart.users_purchases_segments_agg'
        }
        
        data = {}
        for key, table in tables.items():
            data[key] = conn.execute(f"SELECT * FROM {table}").df()
        
    return data

data = load_data()

st.sidebar.header("Фильтры")

date_range = st.sidebar.date_input(
    "Диапазон дат",
    value=(datetime.now() - timedelta(days=7), datetime.now())
)



start_date, end_date = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])
os_names = data['events'].os_name.unique()
device_type = data['events'].device_type.unique()
browser_name = data['events'].browser_name.unique()

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "События", 
    "Продукты", 
    "Конверсии", 
    "Кампании",
    "Пользователи"
])

with tab1:
    st.header("Распределение событий по часам")
    
    events_filtered = data['events'][
        (data['events']['date_part'] >= start_date) & 
        (data['events']['date_part'] <= end_date)
    ]
    
    selected_events = st.multiselect(
        "Выберите события",
        options=events_filtered['event'].unique(),
        default=events_filtered['event'].unique(),
        key='events_select'
    )

    selected_browsers= st.sidebar.multiselect(
        "Выберите тип браузера",
        options=browser_name,
        default=browser_name,
        key='browser_select'
    )

    selected_os= st.sidebar.multiselect(
        "Выберите тип ОС",
        options=os_names,
        default=os_names,
        key='os_select'
    )

    selected_devices= st.sidebar.multiselect(
        "Выберите тип устройства",
        options=device_type,
        default=device_type,
        key='device_select'
    )
    
    if selected_events:
        events_to_show = events_filtered[(events_filtered['event'].isin(selected_events)) &
                                         (events_filtered['os_name'].isin(selected_os)) &
                                         (events_filtered['device_type'].isin(selected_devices)) &
                                         (events_filtered['browser_name'].isin(selected_browsers))]
        
        pivot_events = events_to_show.pivot_table(
            values='cnt', 
            index='date_part', 
            columns='event', 
            aggfunc='sum'
        ).fillna(0)
        
        fig, ax = plt.subplots(figsize=(12, 6))
        pivot_events.plot(ax=ax)
        ax.set_xlabel("Время")
        ax.set_ylabel("Количество событий")
        ax.legend(title="Тип события")
        ax.grid(True)
        st.pyplot(fig)
        
        st.subheader("Детальные данные")
        st.dataframe(events_to_show.sort_values('date_part', ascending=False))

with tab2:
    st.header("Продажи по часам")
    purchases_filtered = data['purchases'][
        (data['purchases']['date_part'] >= start_date) & 
        (data['purchases']['date_part'] <= end_date)
    ].sort_values('date_part')
    
    if not purchases_filtered.empty:
        fig, ax = plt.subplots(figsize=(14, 8))
        
        x = np.arange(len(purchases_filtered))
        y = purchases_filtered['cnt'].values
        
        bars = ax.bar(purchases_filtered['date_part'], y, alpha=0.7, label='Продажи за час')
        
        z = np.polyfit(x, y, 1)
        p = np.poly1d(z)
        ax.plot(purchases_filtered['date_part'], p(x), "r--", linewidth=2, label='Тренд')
        
        ax.set_xlabel("Время")
        ax.set_ylabel("Количество проданных товаров")
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.set_title("Динамика продаж по часам с трендом")
        
        plt.xticks(rotation=45)
        plt.tight_layout()
        st.pyplot(fig)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            total_sales = purchases_filtered['cnt'].sum()
            st.metric("Суммарные продажи за период", f"{total_sales:,}")
        with col2:
            avg_sales = purchases_filtered['cnt'].mean()
            st.metric("Средние продажи в час", f"{avg_sales:,.0f}")
        with col3:
            max_sales = purchases_filtered['cnt'].max()
            st.metric("Максимальные продажи в час", f"{max_sales:,}")
    else:
        st.info("Нет данных о продажах за выбранный период")
    
    st.markdown("---")
    
    st.header("Топ товаров")
    top_n = st.slider("Количество товаров для отображения", 5, 20, 10, key='products_slider')
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        fig, ax = plt.subplots(figsize=(10, 6))
        top_products = data['products'].nlargest(top_n, 'cnt')
        ax.barh(top_products['product'], top_products['cnt'])
        ax.set_xlabel("Количество просмотров")
        ax.set_title(f"Топ-{top_n} товаров по просмотрам")
        st.pyplot(fig)
    
    with col2:
        st.subheader("Данные топ товаров")
        st.dataframe(top_products[['product', 'cnt']].rename(columns={'product': 'Товар', 'cnt': 'Просмотры'}))

with tab3:
    st.header("Конверсии по источникам")
    
    conversions_filtered = data['conversions'][
        (data['conversions']['date_part'] >= start_date) & 
        (data['conversions']['date_part'] <= end_date)
    ]
    
    st.subheader("Суммарные конверсии по источникам")
    source_totals = conversions_filtered.groupby('utm_source')['sm'].sum()
    
    if not source_totals.empty:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        ax1.pie(source_totals.values, labels=source_totals.index, autopct='%1.1f%%', startangle=90)
        ax1.set_title("Распределение конверсий по источникам")
        
        ax2.bar(source_totals.index, source_totals.values)
        ax2.set_xlabel("Источник")
        ax2.set_ylabel("Количество конверсий")
        ax2.tick_params(axis='x', rotation=45)
        ax2.set_title("Конверсии по источникам")
        
        st.pyplot(fig)
    else:
        st.info("Нет данных о конверсиях за выбранный период")
    
    st.subheader("Динамика конверсий по источникам")
    pivot_conv = conversions_filtered.pivot_table(
        values='sm', 
        index='date_part', 
        columns='utm_source', 
        aggfunc='sum'
    ).fillna(0)
    
    if not pivot_conv.empty:
        fig, ax = plt.subplots(figsize=(12, 6))
        pivot_conv.plot(ax=ax)
        ax.set_xlabel("Время")
        ax.set_ylabel("Конверсии")
        ax.legend(title="UTM Source")
        ax.grid(True)
        st.pyplot(fig)

with tab4:
    st.header("Анализ кампаний")
    
    campaigns_filtered = data['campaigns'][
        (data['campaigns']['date_part'] >= start_date) & 
        (data['campaigns']['date_part'] <= end_date)
    ]
    
    selected_campaigns = st.multiselect(
        "Выберите кампании",
        options=campaigns_filtered['utm_campaign'].unique(),
        default=campaigns_filtered['utm_campaign'].unique()[:2] if not campaigns_filtered.empty else [],
        key='campaigns_select'
    )
    
    if selected_campaigns and not campaigns_filtered.empty:
        campaigns_to_show = campaigns_filtered[
            campaigns_filtered['utm_campaign'].isin(selected_campaigns)
        ]
        
        pivot_campaigns = campaigns_to_show.pivot_table(
            values='sm', 
            index='date_part', 
            columns='utm_campaign', 
            aggfunc='sum'
        ).fillna(0)
        
        fig, ax = plt.subplots(figsize=(12, 6))
        pivot_campaigns.plot(ax=ax)
        ax.set_xlabel("Время")
        ax.set_ylabel("Продажи")
        ax.legend(title="Кампания")
        ax.grid(True)
        st.pyplot(fig)
        
        st.subheader("Сравнение кампаний")
        campaign_comparison = campaigns_to_show.groupby('utm_campaign')['sm'].agg(['sum', 'mean', 'count'])
        st.dataframe(campaign_comparison.style.format({'sum': '{:.0f}', 'mean': '{:.1f}'}))
    else:
        st.info("Нет данных о кампаниях за выбранный период")

with tab5:
    st.header("Аналитика пользователей")

    st.subheader('Динамика числа пользователей по каналам')

    users_purch = data['users_purchases']
    users_segments = data['users_segments']

    selected_source = st.multiselect(
        "Выберите источник",
        options=users_segments.utm_source.unique(),
        default=users_segments.utm_source.unique(),
        key='source_select'
    )

    users_to_show = users_purch[
                            (users_purch['os_name'].isin(selected_os)) &
                            (users_purch['device_type'].isin(selected_devices)) &
                            (users_purch['utm_source'].isin(selected_source))
                            ]
    segments_to_show = users_segments[
                            (users_segments['os_name'].isin(selected_os)) &
                            (users_segments['device_type'].isin(selected_devices)) &
                            (users_segments['utm_source'].isin(selected_source))
                            ]


    st.subheader('Распределение пользователей по OC и типу устройства')

    bar = px.bar(segments_to_show.groupby(['os_name', 'device_type'], as_index=False).users.sum(), x="device_type", y="users", color='os_name')

    st.plotly_chart(bar)

    st.subheader('Распределение пользователей по источникам, %')

    pie = px.pie(segments_to_show.groupby(['utm_source'], as_index=False).users.sum(), names="utm_source", values="users")

    st.plotly_chart(pie)

    st.subheader('Число покупок пользователей по источникам')

    bar1 = px.bar(users_to_show.groupby(['utm_source'], as_index=False).products.sum(), x="utm_source", y="products")

    st.plotly_chart(bar1)

st.sidebar.header("Общая статистика")
total_events = data['events']['cnt'].sum()
total_purchases = data['purchases']['cnt'].sum() 
total_products = len(data['products'])

st.sidebar.metric("Всего событий", f"{total_events:,}")
st.sidebar.metric("Всего продаж", f"{total_purchases:,}") 
st.sidebar.metric("Уникальных товаров", total_products)