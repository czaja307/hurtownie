"""
SZCZEGÓŁOWA DEFINICJA MIAR HURTOWNI DANYCH OLIST
================================================

MIARY PODSTAWOWE (3):
====================

1. ORDER_VALUE (Addytywna) 💰
   - Definicja: Suma wartości produktów w zamówieniu (bez kosztów dostawy)
   - Typ: Addytywna (można sumować across wszystkie wymiary)
   - Jednostka: BRL (Real brazylijski)
   - Źródło: olist_order_items_dataset.price
   - Agregacje: SUM, AVG, MIN, MAX
   - Przykład: SUM(Order_Value) BY Region, Year

2. FREIGHT_VALUE (Addytywna) 🚚  
   - Definicja: Koszt dostawy zamówienia
   - Typ: Addytywna
   - Jednostka: BRL
   - Źródło: olist_order_items_dataset.freight_value
   - Agregacje: SUM, AVG, MIN, MAX
   - Przykład: SUM(Freight_Value) BY Seller_Region, Payment_Type

3. ITEMS_COUNT (Addytywna) 📦
   - Definicja: Liczba pozycji (produktów) w zamówieniu
   - Typ: Addytywna
   - Jednostka: sztuki
   - Źródło: COUNT(olist_order_items_dataset.order_item_id)
   - Agregacje: SUM, AVG, MIN, MAX
   - Przykład: SUM(Items_Count) BY Customer_State, Month


MIARY NIEADDYTYWNE (2): ⚠️
=========================

4. DELIVERY_DAYS (Nieaddytywna - Średnia) 📅
   - Definicja: Liczba dni od zamówienia do dostawy
   - Typ: Nieaddytywna (meaningful only as average)
   - Jednostka: dni
   - Kalkulacja: order_delivered_customer_date - order_purchase_timestamp
   - Agregacje: AVG (głównie), MIN, MAX
   - BŁĘDNE: SUM(Delivery_Days) - nie ma sensu biznesowego
   - POPRAWNE: AVG(Delivery_Days) BY Region, Payment_Type

5. REVIEW_SCORE (Nieaddytywna - Średnia) ⭐
   - Definicja: Ocena zamówienia przez klienta (1-5)
   - Typ: Nieaddytywna (meaningful only as average) 
   - Jednostka: punkty (1-5)
   - Źródło: olist_order_reviews_dataset.review_score
   - Agregacje: AVG (głównie), MIN, MAX, DISTRIBUTION
   - BŁĘDNE: SUM(Review_Score) - nie ma sensu biznesowego
   - POPRAWNE: AVG(Review_Score) BY Seller_City, Year


MIARY KALKULOWANE (6): 🧮
=========================

6. TOTAL_REVENUE (Semi-addytywna)
   - Definicja: Order_Value + Freight_Value
   - Kalkulacja: SUM(Order_Value + Freight_Value)
   - Typ: Addytywna (suma dwóch miar addytywnych)
   - Zastosowanie: Całkowite przychody ze sprzedaży

7. AVG_REVIEW_SCORE (Nieaddytywna)
   - Definicja: Średnia ocena w danym przekroju
   - Kalkulacja: AVG(Review_Score) WHERE Review_Score > 0
   - Typ: Nieaddytywna
   - Zastosowanie: Analiza satysfakcji klientów

8. REVENUE_PER_CUSTOMER (Kalkulowana)
   - Definicja: Średni przychód na klienta
   - Kalkulacja: SUM(Total_Revenue) / COUNT(DISTINCT Customer_ID)
   - Typ: Ratio (stosunek)
   - Zastosowanie: Analiza wartości klienta

9. FREIGHT_PERCENTAGE (Kalkulowana)
   - Definicja: Udział kosztów dostawy w całkowitej wartości
   - Kalkulacja: (Freight_Value / Order_Value) * 100
   - Typ: Percentage/Ratio
   - Zastosowanie: Analiza struktury kosztów

10. REVENUE_GDP_RATIO (Kalkulowana)
    - Definicja: Stosunek przychodu do PKB per capita miasta
    - Kalkulacja: SUM(Total_Revenue) / AVG(City_GDP_Per_Capita)
    - Typ: Economic Indicator
    - Zastosowanie: Analiza korelacji ekonomicznych

11. YOY_GROWTH_PERCENT (Kalkulowana czasowo)
    - Definicja: Wzrost rok-do-roku
    - Kalkulacja: ((Current_Year - Previous_Year) / Previous_Year) * 100
    - Typ: Time Intelligence
    - Zastosowanie: Analiza trendów czasowych


PRZYKŁADY UŻYCIA W ZAPYTANIACH OLAP:
===================================

1. POPRAWNE AGREGACJE:
   ✅ SELECT Region, SUM(Order_Value), AVG(Delivery_Days), AVG(Review_Score)
   ✅ SELECT Year, SUM(Total_Revenue), COUNT(Orders), AVG(Revenue_Per_Customer)

2. BŁĘDNE AGREGACJE:
   ❌ SELECT Region, SUM(Delivery_Days) -- Nie ma sensu!
   ❌ SELECT Year, SUM(Review_Score) -- Nie ma sensu!

3. ANALIZA DRILL-DOWN Z MIARAMI:
   SELECT 
       Customer_Region,
       Customer_State, 
       Customer_City,
       COUNT(*) as Orders_Count,           -- Addytywna
       SUM(Order_Value) as Total_Sales,    -- Addytywna  
       AVG(Delivery_Days) as Avg_Delivery, -- Nieaddytywna
       AVG(Review_Score) as Satisfaction   -- Nieaddytywna
   FROM Sales_Cube
   WHERE Year = 2018
   GROUP BY Customer_Region, Customer_State, Customer_City
   ORDER BY Total_Sales DESC

4. ANALIZA Z MIARAMI KALKULOWANYMI:
   SELECT
       Payment_Type,
       SUM(Total_Revenue) as Revenue,
       AVG(Freight_Percentage) as Avg_Freight_Pct,
       AVG(Revenue_Per_Customer) as Customer_Value
   FROM Sales_Cube  
   GROUP BY Payment_Type

5. TIME INTELLIGENCE:
   SELECT 
       Year,
       Month, 
       SUM(Total_Revenue) as Current_Revenue,
       LAG(SUM(Total_Revenue), 12) OVER (ORDER BY Year, Month) as Previous_Year,
       YoY_Growth_Percent
   FROM Sales_Cube
   GROUP BY Year, Month


ZALECENIA ANALITYCZNE:
=====================

🎯 DLA MIAR ADDYTYWNYCH (Order_Value, Freight_Value, Items_Count):
   - Używaj SUM() dla agregacji
   - Bezpieczne drill-down/roll-up
   - Można porównywać across wszystkie wymiary

⚠️ DLA MIAR NIEADDYTYWNYCH (Delivery_Days, Review_Score):
   - Używaj głównie AVG()
   - Ostrożnie z agregacjami wielopoziomowymi
   - Zawsze sprawdzaj sensowność biznesową

🧮 DLA MIAR KALKULOWANYCH:
   - Definiuj na poziomie widoków/zapytań
   - Testuj poprawność formuł
   - Dokumentuj logikę biznesową

📊 BEST PRACTICES:
   - Zawsze group by odpowiednie wymiary
   - Używaj HAVING dla filtrowania agregatów
   - Implementuj NULL handling
   - Testuj z rzeczywistymi danymi
"""

def generate_measure_validation_queries():
    """Generuje zapytania walidacyjne miar"""
    validation_sql = """
-- ====== WALIDACJA MIAR HURTOWNI DANYCH ======

-- 1. Sprawdzenie poprawności miar addytywnych
SELECT 
    'Miary addytywne' as Test_Category,
    COUNT(*) as Total_Records,
    SUM(Order_Value) as Total_Order_Value,
    SUM(Freight_Value) as Total_Freight,
    SUM(Items_Count) as Total_Items,
    AVG(Order_Value) as Avg_Order_Value,
    MIN(Order_Value) as Min_Order_Value,
    MAX(Order_Value) as Max_Order_Value
FROM FACT_Orders
WHERE Order_Value > 0;

-- 2. Sprawdzenie miar nieaddytywnych  
SELECT
    'Miary nieaddytywne' as Test_Category,
    COUNT(*) as Total_Records_With_Delivery,
    AVG(CAST(Delivery_Days as FLOAT)) as Avg_Delivery_Days,
    MIN(Delivery_Days) as Min_Delivery_Days,
    MAX(Delivery_Days) as Max_Delivery_Days,
    COUNT(*) as Total_Records_With_Review,
    AVG(CAST(Review_Score as FLOAT)) as Avg_Review_Score,
    MIN(Review_Score) as Min_Review_Score,
    MAX(Review_Score) as Max_Review_Score
FROM FACT_Orders  
WHERE Delivery_Days IS NOT NULL AND Review_Score > 0;

-- 3. Walidacja miar kalkulowanych
SELECT
    'Miary kalkulowane' as Test_Category,
    SUM(Order_Value + Freight_Value) as Total_Revenue_Calc,
    AVG((Freight_Value / NULLIF(Order_Value, 0)) * 100) as Avg_Freight_Percentage,
    COUNT(DISTINCT Customer_Key) as Unique_Customers,
    SUM(Order_Value + Freight_Value) / COUNT(DISTINCT Customer_Key) as Revenue_Per_Customer
FROM FACT_Orders
WHERE Order_Value > 0;

-- 4. Test addytywności - roll-up validation
-- Suma na poziomie miesięcy powinna równać się sumie rocznej
WITH Monthly_Totals AS (
    SELECT 
        t.Year_Number,
        t.Month_Number,
        SUM(f.Order_Value) as Monthly_Order_Value
    FROM FACT_Orders f
    JOIN DIM_Time t ON f.Time_Key = t.Time_Key
    GROUP BY t.Year_Number, t.Month_Number
),
Yearly_Totals AS (
    SELECT 
        Year_Number,
        SUM(Monthly_Order_Value) as Yearly_From_Months
    FROM Monthly_Totals
    GROUP BY Year_Number
),
Direct_Yearly AS (
    SELECT 
        t.Year_Number,
        SUM(f.Order_Value) as Yearly_Direct
    FROM FACT_Orders f  
    JOIN DIM_Time t ON f.Time_Key = t.Time_Key
    GROUP BY t.Year_Number
)
SELECT 
    y.Year_Number,
    y.Yearly_From_Months,
    d.Yearly_Direct,
    CASE 
        WHEN ABS(y.Yearly_From_Months - d.Yearly_Direct) < 0.01 
        THEN 'PASS' 
        ELSE 'FAIL' 
    END as Additivity_Test
FROM Yearly_Totals y
JOIN Direct_Yearly d ON y.Year_Number = d.Year_Number
ORDER BY y.Year_Number;

-- 5. Test nieaddytywności - średnie nie mogą być sumowane
SELECT
    'Test nieaddytywności' as Test_Description,
    'Średnia z średnich != średnia całościowa' as Expected_Result,
    AVG(Regional_Avg_Review) as Avg_Of_Regional_Averages,
    (SELECT AVG(CAST(Review_Score as FLOAT)) FROM FACT_Orders WHERE Review_Score > 0) as Overall_Average,
    CASE 
        WHEN ABS(AVG(Regional_Avg_Review) - (SELECT AVG(CAST(Review_Score as FLOAT)) FROM FACT_Orders WHERE Review_Score > 0)) > 0.1 
        THEN 'PASS - Miary są nieaddytywne'
        ELSE 'UWAGA - Sprawdź dane'
    END as Test_Result
FROM (
    SELECT 
        c.Customer_Region,
        AVG(CAST(f.Review_Score as FLOAT)) as Regional_Avg_Review
    FROM FACT_Orders f
    JOIN DIM_Customer c ON f.Customer_Key = c.Customer_Key
    WHERE f.Review_Score > 0
    GROUP BY c.Customer_Region
) regional_avgs;
"""
    
    return validation_sql

# Zapisanie walidacji
def save_measure_validation():
    validation_file = r'c:\Users\Kuba\PycharmProjects\hurtownie\measure_validation.sql'
    with open(validation_file, 'w', encoding='utf-8') as f:
        f.write(generate_measure_validation_queries())
    print(f"Zapytania walidacyjne miar zapisane w: {validation_file}")

if __name__ == "__main__":
    save_measure_validation()
