"""
INSTRUKCJA URUCHOMIENIA HURTOWNI DANYCH OLIST
=============================================

KROK 1: Przygotowanie środowiska
---------------------------------
1. Upewnij się, że masz zainstalowane biblioteki Python:
   pip install pandas numpy pyodbc

2. Sprawdź połączenie z SQL Server (192.168.1.204)
   - Użytkownik: sa
   - Hasło: password
   - Baza danych zostanie utworzona automatycznie: OlistDW

KROK 2: Uruchomienie procesu ETL
---------------------------------
python create_data_warehouse.py

Ten skrypt wykona następujące operacje:
✓ Wczyta dane z plików CSV (data/olist/ i data/cities/)
✓ Utworzy schemat hurtowni danych (tabele wymiarów i faktów)
✓ Wypełni wymiary danymi z transformacją i wzbogaceniem
✓ Utworzy tabelę faktów z >10,000 rekordów
✓ Wygeneruje widoki z miarami kalkulowanymi
✓ Zapisze przykładowe zapytania OLAP

KROK 3: Dodatkowe analizy
-------------------------
python olap_cube_definition.py

Wygeneruje zaawansowane zapytania analityczne.

STRUKTURA HURTOWNI:
==================

WYMIARY (5):
- DIM_Time (hierarchiczny: Rok → Kwartał → Miesiąc → Dzień)
- DIM_Customer (hierarchiczny: Region → Stan → Miasto → Klient + dane ekonomiczne)
- DIM_Seller (hierarchiczny: Region → Stan → Miasto → Sprzedawca + dane ekonomiczne)  
- DIM_Payment (typy płatności, raty, kategorie)
- DIM_Review (oceny, satysfakcja)

FAKTY:
- FACT_Orders (zamówienia z miarami)

MIARY:
- Addytywne: Order_Value, Freight_Value, Items_Count
- Nieaddytywne: Delivery_Days (średnia), Review_Score (średnia)
- Kalkulowane: Total_Revenue, Revenue_Per_Customer, Freight_Percentage

PLIKI WYJŚCIOWE:
===============
- sample_olap_queries.sql - podstawowe zapytania OLAP
- advanced_olap_analysis.sql - zaawansowane analizy wielowymiarowe

PRZYKŁADOWE ANALIZY:
===================
1. Sprzedaż w czasie (roll-up/drill-down)
2. Analiza geograficzna z PKB i HDI miast
3. Segmentacja klientów RFM
4. Korelacje ekonomiczne
5. Analizy sezonowości
6. Efektywność dostaw vs satysfakcja
7. Trendy rok-do-roku
8. Cross-dimensional analysis
"""

import subprocess
import sys
import os

def install_requirements():
    """Instalacja wymaganych bibliotek"""
    print("📦 Instalowanie wymaganych bibliotek...")
    
    packages = ['pandas', 'numpy', 'pyodbc']
    
    for package in packages:
        try:
            __import__(package)
            print(f"  ✓ {package} już zainstalowany")
        except ImportError:
            print(f"  📥 Instalowanie {package}...")
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])

def check_data_files():
    """Sprawdzenie dostępności plików danych"""
    print("📁 Sprawdzanie plików danych...")
    
    data_path = r'c:\Users\Kuba\PycharmProjects\hurtownie\data'
    
    required_files = [
        'olist/olist_orders_dataset.csv',
        'olist/olist_order_items_dataset.csv', 
        'olist/olist_customers_dataset.csv',
        'olist/olist_sellers_dataset.csv',
        'olist/olist_order_payments_dataset.csv',
        'olist/olist_order_reviews_dataset.csv',
        'cities/BRAZIL_CITIES_REV2022.CSV'
    ]
    
    missing_files = []
    
    for file_path in required_files:
        full_path = os.path.join(data_path, file_path)
        if os.path.exists(full_path):
            print(f"  ✓ {file_path}")
        else:
            print(f"  ✗ {file_path} - BRAK PLIKU")
            missing_files.append(file_path)
    
    if missing_files:
        print(f"\n❌ Brakuje {len(missing_files)} plików danych!")
        return False
    
    print("  ✓ Wszystkie pliki danych dostępne")
    return True

def run_etl_pipeline():
    """Uruchomienie pełnego pipeline ETL"""
    print("🚀 URUCHAMIANIE PIPELINE HURTOWNI DANYCH OLIST")
    print("=" * 60)
    
    # Krok 1: Sprawdzenie wymagań
    if not check_data_files():
        print("❌ Zatrzymano - brakuje plików danych")
        return False
    
    # Krok 2: Instalacja bibliotek
    try:
        install_requirements()
    except Exception as e:
        print(f"❌ Błąd instalacji bibliotek: {e}")
        return False
    
    # Krok 3: Import i uruchomienie ETL
    try:
        print("\n🔧 Uruchamianie głównego procesu ETL...")
        from create_data_warehouse import OlistDataWarehouse
        
        dw = OlistDataWarehouse()
        success = dw.run_etl_process()
        
        if success:
            print("\n🎯 Generowanie dodatkowych analiz...")
            from olap_cube_definition import save_advanced_queries
            save_advanced_queries()
            
            print("\n" + "=" * 60)
            print("🎉 PIPELINE ZAKOŃCZONY POMYŚLNIE!")
            print("\n📊 NASTĘPNE KROKI:")
            print("   1. Połącz się z bazą OlistDW na SQL Server")
            print("   2. Uruchom zapytania z sample_olap_queries.sql")  
            print("   3. Eksploruj zaawansowane analizy z advanced_olap_analysis.sql")
            print("   4. Utwórz raporty w Power BI/Tableau")
            
            return True
        else:
            print("❌ ETL zakończony błędem")
            return False
            
    except Exception as e:
        print(f"❌ Błąd podczas ETL: {e}")
        return False

if __name__ == "__main__":
    run_etl_pipeline()
