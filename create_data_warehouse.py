import pandas as pd
import pyodbc
import numpy as np
from datetime import datetime
import os

# Konfiguracja połączenia z bazą danych
server = '192.168.0.118'
database = 'Olist'
driver = 'SQL Server Native Client 11.0'
username = 'sa'
password = 'password'

connection_string = (
    f'DRIVER={{{driver}}};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'UID={username};'
    f'PWD={password};'
    f'Encrypt=no;'
)

def create_connection():
    """Tworzy połączenie z bazą danych"""
    try:
        conn = pyodbc.connect(connection_string)
        return conn
    except Exception as e:
        print(f"Błąd połączenia z bazą danych: {e}")
        return None

def load_csv_data():
    """Ładuje dane z plików CSV"""
    print("Ładowanie danych z plików CSV...")
    
    # Ścieżki do plików
    data_dir = "data"
    
    # Ładowanie danych Olist z odpowiednim kodowaniem
    customers = pd.read_csv(f"{data_dir}/olist/olist_customers_dataset.csv", encoding='utf-8')
    orders = pd.read_csv(f"{data_dir}/olist/olist_orders_dataset.csv", encoding='utf-8')
    order_items = pd.read_csv(f"{data_dir}/olist/olist_order_items_dataset.csv", encoding='utf-8')
    sellers = pd.read_csv(f"{data_dir}/olist/olist_sellers_dataset.csv", encoding='utf-8')
    products = pd.read_csv(f"{data_dir}/olist/olist_products_dataset.csv", encoding='utf-8')
    
    # Ładowanie danych o miastach brazylijskich - może być inne kodowanie
    try:
        cities = pd.read_csv(f"{data_dir}/cities/BRAZIL_CITIES_REV2022.CSV", encoding='utf-8')
    except UnicodeDecodeError:
        try:
            cities = pd.read_csv(f"{data_dir}/cities/BRAZIL_CITIES_REV2022.CSV", encoding='latin-1')
        except UnicodeDecodeError:
            cities = pd.read_csv(f"{data_dir}/cities/BRAZIL_CITIES_REV2022.CSV", encoding='cp1252')
    
    print(f"Załadowano {len(customers)} klientów")
    print(f"Załadowano {len(orders)} zamówień")
    print(f"Załadowano {len(order_items)} pozycji zamówień")
    print(f"Załadowano {len(sellers)} sprzedawców")
    print(f"Załadowano {len(products)} produktów")
    print(f"Załadowano {len(cities)} miast")
    
    return customers, orders, order_items, sellers, products, cities

def create_state_region_mapping():
    """Tworzy mapowanie stanów na regiony Brazylii"""
    state_region_mapping = {
        # Region Północny (Norte)
        'AC': 'Norte', 'AP': 'Norte', 'AM': 'Norte', 'PA': 'Norte', 
        'RO': 'Norte', 'RR': 'Norte', 'TO': 'Norte',
        
        # Region Północno-wschodni (Nordeste)
        'AL': 'Nordeste', 'BA': 'Nordeste', 'CE': 'Nordeste', 'MA': 'Nordeste',
        'PB': 'Nordeste', 'PE': 'Nordeste', 'PI': 'Nordeste', 'RN': 'Nordeste', 'SE': 'Nordeste',
        
        # Region Środkowo-zachodni (Centro-Oeste)
        'GO': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'MS': 'Centro-Oeste', 'DF': 'Centro-Oeste',
        
        # Region Południowo-wschodni (Sudeste)
        'ES': 'Sudeste', 'MG': 'Sudeste', 'RJ': 'Sudeste', 'SP': 'Sudeste',
        
        # Region Południowy (Sul)
        'PR': 'Sul', 'RS': 'Sul', 'SC': 'Sul'
    }
    return state_region_mapping

def create_database_schema(conn):
    """Tworzy schemat bazy danych dla hurtowni"""
    print("Tworzenie schematu bazy danych...")
    
    cursor = conn.cursor()
    
    # Usuwanie istniejących tabel (w odwrotnej kolejności ze względu na klucze obce)
    drop_tables = [
        "DROP TABLE IF EXISTS FactOrders",
        "DROP TABLE IF EXISTS DimTime",
        "DROP TABLE IF EXISTS DimCustomerLocation", 
        "DROP TABLE IF EXISTS DimSellerLocation",
        "DROP TABLE IF EXISTS DimProduct",
        "DROP TABLE IF EXISTS DimOrderStatus"
    ]
    
    for drop_sql in drop_tables:
        try:
            cursor.execute(drop_sql)
        except:
            pass
    
    # Wymiar czasu - struktura hierarchiczna
    cursor.execute("""
    CREATE TABLE DimTime (
        TimeKey INT IDENTITY(1,1) PRIMARY KEY,
        FullDate DATE UNIQUE,
        Year INT,
        Quarter INT,
        Month INT,
        MonthName NVARCHAR(20),
        Day INT,
        DayOfWeek INT,
        DayName NVARCHAR(20),
        WeekOfYear INT
    )
    """)
    
    # Wymiar lokalizacji klienta - zdenormalizowany, hierarchiczny
    cursor.execute("""
    CREATE TABLE DimCustomerLocation (
        CustomerLocationKey INT IDENTITY(1,1) PRIMARY KEY,
        CustomerZipCode NVARCHAR(10),
        CustomerCity NVARCHAR(100),
        CustomerState NVARCHAR(5),
        CustomerRegion NVARCHAR(50),
        CityPopulation INT,
        StatePopulation BIGINT,
        GDP_Per_Capita DECIMAL(18,2),
        HDI DECIMAL(5,4),
        Latitude DECIMAL(10,6),
        Longitude DECIMAL(10,6)
    )
    """)
    
    # Wymiar lokalizacji sprzedawcy - zdenormalizowany, hierarchiczny  
    cursor.execute("""
    CREATE TABLE DimSellerLocation (
        SellerLocationKey INT IDENTITY(1,1) PRIMARY KEY,
        SellerZipCode NVARCHAR(10),
        SellerCity NVARCHAR(100),
        SellerState NVARCHAR(5),
        SellerRegion NVARCHAR(50),
        CityPopulation INT,
        StatePopulation BIGINT,
        GDP_Per_Capita DECIMAL(18,2),
        HDI DECIMAL(5,4),
        Latitude DECIMAL(10,6),
        Longitude DECIMAL(10,6)
    )
    """)
    
    # Wymiar produktu
    cursor.execute("""
    CREATE TABLE DimProduct (
        ProductKey INT IDENTITY(1,1) PRIMARY KEY,
        ProductId NVARCHAR(50) UNIQUE,
        ProductCategory NVARCHAR(100),
        ProductWeight DECIMAL(10,2),
        ProductLength DECIMAL(10,2),
        ProductHeight DECIMAL(10,2),
        ProductWidth DECIMAL(10,2),
        ProductVolume AS (ProductLength * ProductHeight * ProductWidth),
        WeightCategory NVARCHAR(20),
        SizeCategory NVARCHAR(20)
    )
    """)
    
    # Wymiar statusu zamówienia
    cursor.execute("""
    CREATE TABLE DimOrderStatus (
        OrderStatusKey INT IDENTITY(1,1) PRIMARY KEY,
        OrderStatus NVARCHAR(50) UNIQUE,
        StatusDescription NVARCHAR(200)
    )
    """)
    
    # Tabela faktów
    cursor.execute("""
    CREATE TABLE FactOrders (
        FactKey INT IDENTITY(1,1) PRIMARY KEY,
        OrderId NVARCHAR(50),
        TimeKey INT,
        CustomerLocationKey INT,
        SellerLocationKey INT,
        ProductKey INT,
        OrderStatusKey INT,
        
        -- Miary addytywne
        TotalPrice DECIMAL(18,2),
        FreightValue DECIMAL(18,2),
        Quantity INT,
        
        -- Miary nieaddytywne (będą agregowane osobno)
        DeliveryTimeDays INT,
        ProcessingTimeDays INT,
        
        -- Daty dla obliczeń
        OrderDate DATETIME,
        DeliveryDate DATETIME,
        EstimatedDeliveryDate DATETIME,
        
        FOREIGN KEY (TimeKey) REFERENCES DimTime(TimeKey),
        FOREIGN KEY (CustomerLocationKey) REFERENCES DimCustomerLocation(CustomerLocationKey),
        FOREIGN KEY (SellerLocationKey) REFERENCES DimSellerLocation(SellerLocationKey),
        FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey),
        FOREIGN KEY (OrderStatusKey) REFERENCES DimOrderStatus(OrderStatusKey)
    )
    """)
    
    conn.commit()
    print("Schemat bazy danych został utworzony!")

def populate_dim_time(conn, start_date='2016-01-01', end_date='2019-12-31'):
    """Wypełnia wymiar czasu"""
    print("Wypełnianie wymiaru czasu...")
    
    cursor = conn.cursor()
    
    # Generowanie dat
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    month_names = {
        1: 'Styczeń', 2: 'Luty', 3: 'Marzec', 4: 'Kwiecień',
        5: 'Maj', 6: 'Czerwiec', 7: 'Lipiec', 8: 'Sierpień',
        9: 'Wrzesień', 10: 'Październik', 11: 'Listopad', 12: 'Grudzień'
    }
    
    day_names = {
        0: 'Poniedziałek', 1: 'Wtorek', 2: 'Środa', 3: 'Czwartek',
        4: 'Piątek', 5: 'Sobota', 6: 'Niedziela'
    }
    
    for date in date_range:
        quarter = (date.month - 1) // 3 + 1
        
        cursor.execute("""
        INSERT INTO DimTime (FullDate, Year, Quarter, Month, MonthName, Day, DayOfWeek, DayName, WeekOfYear)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            date.date(),
            date.year,
            quarter,
            date.month,
            month_names[date.month],
            date.day,
            date.weekday(),
            day_names[date.weekday()],
            date.isocalendar()[1]
        ))
    
    conn.commit()
    print(f"Dodano {len(date_range)} rekordów do wymiaru czasu")

def populate_location_dimensions(conn, customers, sellers, cities):
    """Wypełnia wymiary lokalizacji (zdenormalizowane)"""
    print("Wypełnianie wymiarów lokalizacji...")
    
    cursor = conn.cursor()
    state_region_mapping = create_state_region_mapping()
    
    # Przygotowanie danych o miastach z dodatkowymi informacjami
    cities_clean = cities.copy()
    cities_clean['CITY'] = cities_clean['CITY'].str.strip().str.lower()
    cities_clean['STATE'] = cities_clean['STATE'].str.strip().str.upper()
    
    # Agregacja danych na poziomie stanu
    state_stats = cities_clean.groupby('STATE').agg({
        'IBGE_POP': 'sum',
        'GDP_CAPITA': 'mean',
        'IDHM': 'mean'
    }).reset_index()
    
    # Wypełnianie wymiaru lokalizacji klienta
    customer_locations = customers[['customer_zip_code_prefix', 'customer_city', 'customer_state']].drop_duplicates()
    
    for _, row in customer_locations.iterrows():
        zip_code = str(row['customer_zip_code_prefix'])
        city = row['customer_city'].strip().lower() if pd.notna(row['customer_city']) else 'nieznane'
        state = row['customer_state'].strip().upper() if pd.notna(row['customer_state']) else 'UNK'
        region = state_region_mapping.get(state, 'Nieznany')
        
        # Znajdowanie danych o mieście
        city_data = cities_clean[
            (cities_clean['CITY'] == city) & 
            (cities_clean['STATE'] == state)
        ]
        
        if not city_data.empty:
            city_pop = int(city_data.iloc[0]['IBGE_POP']) if pd.notna(city_data.iloc[0]['IBGE_POP']) else 0
            gdp_per_capita = float(city_data.iloc[0]['GDP_CAPITA']) if pd.notna(city_data.iloc[0]['GDP_CAPITA']) else 0
            hdi = float(city_data.iloc[0]['IDHM']) if pd.notna(city_data.iloc[0]['IDHM']) else 0
            lat = float(city_data.iloc[0]['LAT']) if pd.notna(city_data.iloc[0]['LAT']) else 0
            lon = float(city_data.iloc[0]['LONG']) if pd.notna(city_data.iloc[0]['LONG']) else 0
        else:
            city_pop = 0
            gdp_per_capita = 0
            hdi = 0
            lat = 0
            lon = 0
        
        # Dane o stanie
        state_data = state_stats[state_stats['STATE'] == state]
        state_pop = int(state_data.iloc[0]['IBGE_POP']) if not state_data.empty and pd.notna(state_data.iloc[0]['IBGE_POP']) else 0
        
        cursor.execute("""
        INSERT INTO DimCustomerLocation 
        (CustomerZipCode, CustomerCity, CustomerState, CustomerRegion, CityPopulation, StatePopulation, GDP_Per_Capita, HDI, Latitude, Longitude)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (zip_code, city, state, region, city_pop, state_pop, gdp_per_capita, hdi, lat, lon))
    
    # Wypełnianie wymiaru lokalizacji sprzedawcy
    seller_locations = sellers[['seller_zip_code_prefix', 'seller_city', 'seller_state']].drop_duplicates()
    
    for _, row in seller_locations.iterrows():
        zip_code = str(row['seller_zip_code_prefix'])
        city = row['seller_city'].strip().lower() if pd.notna(row['seller_city']) else 'nieznane'
        state = row['seller_state'].strip().upper() if pd.notna(row['seller_state']) else 'UNK'
        region = state_region_mapping.get(state, 'Nieznany')
        
        # Znajdowanie danych o mieście
        city_data = cities_clean[
            (cities_clean['CITY'] == city) & 
            (cities_clean['STATE'] == state)
        ]
        
        if not city_data.empty:
            city_pop = int(city_data.iloc[0]['IBGE_POP']) if pd.notna(city_data.iloc[0]['IBGE_POP']) else 0
            gdp_per_capita = float(city_data.iloc[0]['GDP_CAPITA']) if pd.notna(city_data.iloc[0]['GDP_CAPITA']) else 0
            hdi = float(city_data.iloc[0]['IDHM']) if pd.notna(city_data.iloc[0]['IDHM']) else 0
            lat = float(city_data.iloc[0]['LAT']) if pd.notna(city_data.iloc[0]['LAT']) else 0
            lon = float(city_data.iloc[0]['LONG']) if pd.notna(city_data.iloc[0]['LONG']) else 0
        else:
            city_pop = 0
            gdp_per_capita = 0
            hdi = 0
            lat = 0
            lon = 0
        
        # Dane o stanie
        state_data = state_stats[state_stats['STATE'] == state]
        state_pop = int(state_data.iloc[0]['IBGE_POP']) if not state_data.empty and pd.notna(state_data.iloc[0]['IBGE_POP']) else 0
        
        cursor.execute("""
        INSERT INTO DimSellerLocation 
        (SellerZipCode, SellerCity, SellerState, SellerRegion, CityPopulation, StatePopulation, GDP_Per_Capita, HDI, Latitude, Longitude)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (zip_code, city, state, region, city_pop, state_pop, gdp_per_capita, hdi, lat, lon))
    
    conn.commit()
    print(f"Dodano {len(customer_locations)} lokalizacji klientów")
    print(f"Dodano {len(seller_locations)} lokalizacji sprzedawców")

def populate_dim_product(conn, products):
    """Wypełnia wymiar produktu"""
    print("Wypełnianie wymiaru produktu...")
    
    cursor = conn.cursor()
    
    for _, row in products.iterrows():
        product_id = row['product_id']
        category = row['product_category_name'] if pd.notna(row['product_category_name']) else 'nieznana'
        weight = float(row['product_weight_g']) if pd.notna(row['product_weight_g']) else 0
        length = float(row['product_length_cm']) if pd.notna(row['product_length_cm']) else 0
        height = float(row['product_height_cm']) if pd.notna(row['product_height_cm']) else 0
        width = float(row['product_width_cm']) if pd.notna(row['product_width_cm']) else 0
        
        # Kategoryzacja wagi
        if weight == 0:
            weight_category = 'Nieznana'
        elif weight <= 100:
            weight_category = 'Lekki'
        elif weight <= 1000:
            weight_category = 'Średni'
        else:
            weight_category = 'Ciężki'
        
        # Kategoryzacja rozmiaru (na podstawie objętości)
        volume = length * height * width
        if volume == 0:
            size_category = 'Nieznany'
        elif volume <= 1000:
            size_category = 'Mały'
        elif volume <= 10000:
            size_category = 'Średni'
        else:
            size_category = 'Duży'
        
        cursor.execute("""
        INSERT INTO DimProduct 
        (ProductId, ProductCategory, ProductWeight, ProductLength, ProductHeight, ProductWidth, WeightCategory, SizeCategory)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (product_id, category, weight, length, height, width, weight_category, size_category))
    
    conn.commit()
    print(f"Dodano {len(products)} produktów")

def populate_dim_order_status(conn, orders):
    """Wypełnia wymiar statusu zamówienia"""
    print("Wypełnianie wymiaru statusu zamówienia...")
    
    cursor = conn.cursor()
    
    status_descriptions = {
        'delivered': 'Zamówienie zostało dostarczone',
        'shipped': 'Zamówienie zostało wysłane',
        'processing': 'Zamówienie jest przetwarzane',
        'canceled': 'Zamówienie zostało anulowane',
        'unavailable': 'Produkt niedostępny',
        'created': 'Zamówienie utworzone',
        'approved': 'Zamówienie zatwierdzone',
        'invoiced': 'Zamówienie zafakturowane'
    }
    
    unique_statuses = orders['order_status'].unique()
    
    for status in unique_statuses:
        if pd.notna(status):
            description = status_descriptions.get(status, f'Status: {status}')
            cursor.execute("""
            INSERT INTO DimOrderStatus (OrderStatus, StatusDescription)
            VALUES (?, ?)
            """, (status, description))
    
    conn.commit()
    print(f"Dodano {len(unique_statuses)} statusów zamówień")

def populate_fact_table(conn, orders, order_items, customers, sellers, products):
    """Wypełnia tabelę faktów"""
    print("Wypełnianie tabeli faktów...")
    
    cursor = conn.cursor()
    
    # Łączenie wszystkich potrzebnych danych
    fact_data = order_items.merge(orders, on='order_id', how='inner')
    fact_data = fact_data.merge(customers, on='customer_id', how='inner')
    fact_data = fact_data.merge(sellers, on='seller_id', how='inner')
    fact_data = fact_data.merge(products, on='product_id', how='inner')
    
    print(f"Przygotowano {len(fact_data)} rekordów do załadowania")
    
    # Pobieranie kluczy wymiarów
    time_keys = {}
    cursor.execute("SELECT TimeKey, FullDate FROM DimTime")
    for row in cursor.fetchall():
        time_keys[row[1]] = row[0]
    
    customer_location_keys = {}
    cursor.execute("SELECT CustomerLocationKey, CustomerZipCode, CustomerCity, CustomerState FROM DimCustomerLocation")
    for row in cursor.fetchall():
        key = f"{row[1]}_{row[2]}_{row[3]}"
        customer_location_keys[key] = row[0]
    
    seller_location_keys = {}
    cursor.execute("SELECT SellerLocationKey, SellerZipCode, SellerCity, SellerState FROM DimSellerLocation")
    for row in cursor.fetchall():
        key = f"{row[1]}_{row[2]}_{row[3]}"
        seller_location_keys[key] = row[0]
    
    product_keys = {}
    cursor.execute("SELECT ProductKey, ProductId FROM DimProduct")
    for row in cursor.fetchall():
        product_keys[row[1]] = row[0]
    
    status_keys = {}
    cursor.execute("SELECT OrderStatusKey, OrderStatus FROM DimOrderStatus")
    for row in cursor.fetchall():
        status_keys[row[1]] = row[0]
    
    # Wstawianie danych do tabeli faktów
    inserted_count = 0
    batch_size = 1000
    batch_data = []
    
    for _, row in fact_data.iterrows():
        try:
            # Parsowanie dat
            order_date = pd.to_datetime(row['order_purchase_timestamp'])
            delivery_date = pd.to_datetime(row['order_delivered_customer_date']) if pd.notna(row['order_delivered_customer_date']) else None
            estimated_delivery_date = pd.to_datetime(row['order_estimated_delivery_date']) if pd.notna(row['order_estimated_delivery_date']) else None
            
            # Znajdowanie kluczy
            time_key = time_keys.get(order_date.date())
            if not time_key:
                continue
                
            customer_key_str = f"{row['customer_zip_code_prefix']}_{row['customer_city'].strip().lower()}_{row['customer_state'].strip().upper()}"
            customer_location_key = customer_location_keys.get(customer_key_str)
            if not customer_location_key:
                continue
                
            seller_key_str = f"{row['seller_zip_code_prefix']}_{row['seller_city'].strip().lower()}_{row['seller_state'].strip().upper()}"
            seller_location_key = seller_location_keys.get(seller_key_str)
            if not seller_location_key:
                continue
                
            product_key = product_keys.get(row['product_id'])
            if not product_key:
                continue
                
            status_key = status_keys.get(row['order_status'])
            if not status_key:
                continue
            
            # Obliczanie miar
            total_price = float(row['price']) if pd.notna(row['price']) else 0
            freight_value = float(row['freight_value']) if pd.notna(row['freight_value']) else 0
            quantity = 1  # każdy wiersz to jedna pozycja
            
            # Obliczanie czasu dostawy (nieaddytywna miara)
            delivery_time_days = None
            processing_time_days = None
            
            if delivery_date and order_date:
                delivery_time_days = (delivery_date - order_date).days
            
            if pd.notna(row['order_approved_at']):
                approved_date = pd.to_datetime(row['order_approved_at'])
                processing_time_days = (approved_date - order_date).days
            
            batch_data.append((
                row['order_id'], time_key, customer_location_key, seller_location_key,
                product_key, status_key, total_price, freight_value, quantity,
                delivery_time_days, processing_time_days, order_date,
                delivery_date, estimated_delivery_date
            ))
            
            if len(batch_data) >= batch_size:
                cursor.executemany("""
                INSERT INTO FactOrders 
                (OrderId, TimeKey, CustomerLocationKey, SellerLocationKey, ProductKey, OrderStatusKey,
                 TotalPrice, FreightValue, Quantity, DeliveryTimeDays, ProcessingTimeDays,
                 OrderDate, DeliveryDate, EstimatedDeliveryDate)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, batch_data)
                
                inserted_count += len(batch_data)
                batch_data = []
                
                if inserted_count % 5000 == 0:
                    print(f"Wstawiono {inserted_count} rekordów...")
                    conn.commit()
        
        except Exception as e:
            print(f"Błąd przetwarzania rekordu: {e}")
            continue
    
    # Wstawienie pozostałych rekordów
    if batch_data:
        cursor.executemany("""
        INSERT INTO FactOrders 
        (OrderId, TimeKey, CustomerLocationKey, SellerLocationKey, ProductKey, OrderStatusKey,
         TotalPrice, FreightValue, Quantity, DeliveryTimeDays, ProcessingTimeDays,
         OrderDate, DeliveryDate, EstimatedDeliveryDate)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch_data)
        inserted_count += len(batch_data)
    
    conn.commit()
    print(f"Dodano {inserted_count} rekordów do tabeli faktów")

def create_sample_queries(conn):
    """Tworzy przykładowe zapytania analityczne"""
    print("\nTworzenie przykładowych zapytań analitycznych...")
    
    cursor = conn.cursor()
    
    queries = [
        {
            'name': 'Sprzedaż według regionów i czasu (hierarchie)',
            'sql': """
            SELECT 
                cl.CustomerRegion,
                cl.CustomerState,
                dt.Year,
                dt.Quarter,
                SUM(f.TotalPrice) as TotalSales,
                SUM(f.FreightValue) as TotalFreight,
                COUNT(*) as OrderCount,
                AVG(CAST(f.DeliveryTimeDays AS FLOAT)) as AvgDeliveryTime
            FROM FactOrders f
            JOIN DimCustomerLocation cl ON f.CustomerLocationKey = cl.CustomerLocationKey
            JOIN DimTime dt ON f.TimeKey = dt.TimeKey
            WHERE f.DeliveryTimeDays IS NOT NULL
            GROUP BY cl.CustomerRegion, cl.CustomerState, dt.Year, dt.Quarter
            ORDER BY TotalSales DESC
            """
        },
        {
            'name': 'Analiza produktów według kategorii i rozmiaru',
            'sql': """
            SELECT 
                p.ProductCategory,
                p.WeightCategory,
                p.SizeCategory,
                COUNT(*) as ProductsSold,
                SUM(f.TotalPrice) as Revenue,
                AVG(f.TotalPrice) as AvgPrice,
                SUM(f.Quantity) as TotalQuantity
            FROM FactOrders f
            JOIN DimProduct p ON f.ProductKey = p.ProductKey
            GROUP BY p.ProductCategory, p.WeightCategory, p.SizeCategory
            HAVING COUNT(*) > 10
            ORDER BY Revenue DESC
            """
        },
        {
            'name': 'Wydajność dostaw według lokalizacji sprzedawcy',
            'sql': """
            SELECT 
                sl.SellerRegion,
                sl.SellerState,
                COUNT(*) as OrderCount,
                AVG(CAST(f.DeliveryTimeDays AS FLOAT)) as AvgDeliveryTime,
                MIN(f.DeliveryTimeDays) as MinDeliveryTime,
                MAX(f.DeliveryTimeDays) as MaxDeliveryTime,
                SUM(f.TotalPrice) as TotalRevenue
            FROM FactOrders f
            JOIN DimSellerLocation sl ON f.SellerLocationKey = sl.SellerLocationKey
            WHERE f.DeliveryTimeDays IS NOT NULL AND f.DeliveryTimeDays >= 0
            GROUP BY sl.SellerRegion, sl.SellerState
            HAVING COUNT(*) > 50
            ORDER BY AvgDeliveryTime ASC
            """
        }
    ]
    
    for query in queries:
        print(f"\n--- {query['name']} ---")
        try:
            cursor.execute(query['sql'])
            results = cursor.fetchall()
            print(f"Zwrócono {len(results)} wyników")
            
            # Wyświetlenie pierwszych 5 wyników
            for i, row in enumerate(results[:5]):
                print(f"  {i+1}: {row}")
            if len(results) > 5:
                print(f"  ... i {len(results)-5} więcej")
                
        except Exception as e:
            print(f"Błąd wykonania zapytania: {e}")

def main():
    """Główna funkcja programu"""
    print("=== TWORZENIE HURTOWNI DANYCH OLIST ===")
    
    # Połączenie z bazą danych
    conn = create_connection()
    if not conn:
        return
    
    try:
        # Ładowanie danych CSV
        customers, orders, order_items, sellers, products, cities = load_csv_data()
        
        # Tworzenie schematu bazy danych
        create_database_schema(conn)
        
        # Wypełnianie wymiarów
        populate_dim_time(conn)
        populate_location_dimensions(conn, customers, sellers, cities)
        populate_dim_product(conn, products)
        populate_dim_order_status(conn, orders)
        
        # Wypełnianie tabeli faktów
        populate_fact_table(conn, orders, order_items, customers, sellers, products)
        
        # Sprawdzenie liczby rekordów
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM FactOrders")
        fact_count = cursor.fetchone()[0]
        print(f"\nTabela faktów zawiera {fact_count} rekordów")
        
        if fact_count >= 10000:
            print("✓ Wymaganie minimum 10,000 rekordów zostało spełnione!")
        else:
            print("⚠ Uwaga: Tabela faktów zawiera mniej niż 10,000 rekordów")
        
        # Przykładowe zapytania analityczne
        create_sample_queries(conn)
        
        print("\n=== HURTOWNIA DANYCH ZOSTAŁA POMYŚLNIE UTWORZONA ===")
        print("\nStruktura hurtowni:")
        print("• 5 wymiarów (w tym 3 hierarchiczne: czas, lokalizacja klienta, lokalizacja sprzedawcy)")
        print("• 3 miary (2 addytywne: TotalPrice, FreightValue; 1 nieaddytywna: średni czas dostawy)")
        print("• Zdenormalizowane wymiary lokalizacji z danymi demograficznymi")
        print("• Połączone dane Olist z danymi o miastach brazylijskich")
        
    except Exception as e:
        print(f"Błąd podczas tworzenia hurtowni: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
