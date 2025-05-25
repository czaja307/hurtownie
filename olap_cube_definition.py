"""
DEFINICJA KOSTKI OLAP OLIST - MIARY I WYMIARY
==============================================

KOSTKA: SALES_CUBE (Sprzedaż E-commerce)
FAKT: Zamówienie (Order) - ziarnistość: jedno zamówienie

WYMIARY (5):
============

1. DIM_TIME (HIERARCHICZNY) ⭐
   - Poziom 4 (najniższy): Dzień
   - Poziom 3: Miesiąc  
   - Poziom 2: Kwartał
   - Poziom 1 (najwyższy): Rok
   
   Atrybuty: Is_Weekend, Is_Holiday, Day_Name

2. DIM_CUSTOMER (HIERARCHICZNY) ⭐
   - Poziom 4 (najniższy): Klient
   - Poziom 3: Miasto + dane ekonomiczne
   - Poziom 2: Stan
   - Poziom 1 (najwyższy): Region
   
   Atrybuty ekonomiczne: GDP_Per_Capita, HDI, Population, Is_Capital

3. DIM_SELLER (HIERARCHICZNY) ⭐
   - Poziom 4 (najniższy): Sprzedawca
   - Poziom 3: Miasto + dane ekonomiczne  
   - Poziom 2: Stan
   - Poziom 1 (najwyższy): Region
   
   Atrybuty ekonomiczne: GDP_Per_Capita, HDI, Population, Is_Capital

4. DIM_PAYMENT
   - Payment_Type (credit_card, boleto, voucher, debit_card)
   - Payment_Category (kategorie płatności)
   - Installments_Range (zakresy rat)
   - Is_Credit, Is_Installment

5. DIM_REVIEW  
   - Review_Score (1-5, 0=brak)
   - Review_Category (Pozytywna, Neutralna, Negatywna, Brak)
   - Satisfaction_Level (Zadowolony, Neutralny, Niezadowolony)


MIARY (3 podstawowe + kalkulowane):
===================================

MIARY ADDYTYWNE:
- Order_Value: Wartość zamówienia (suma produktów)
- Freight_Value: Koszt dostawy  
- Items_Count: Liczba pozycji w zamówieniu

MIARY NIEADDYTYWNE: ⭐
- Delivery_Days: Dni dostawy (średnia, nie można sumować)
- Review_Score: Ocena (średnia, nie można sumować)

MIARY KALKULOWANE: ⭐
- Total_Revenue = Order_Value + Freight_Value
- Avg_Review_Score = AVG(Review_Score) 
- Revenue_Per_Customer = SUM(Total_Revenue) / COUNT(DISTINCT Customer)
- Freight_Percentage = (Freight_Value / Order_Value) * 100
- Revenue_GDP_Ratio = SUM(Total_Revenue) / AVG(City_GDP_Per_Capita)
- YoY_Growth = ((Current_Period - Previous_Period) / Previous_Period) * 100


OPERACJE OLAP:
==============

1. ROLL-UP (Agregacja w górę hierarchii):
   - Dzień → Miesiąc → Kwartał → Rok
   - Klient → Miasto → Stan → Region

2. DRILL-DOWN (Szczegółowość w dół hierarchii):
   - Rok → Kwartał → Miesiąc → Dzień
   - Region → Stan → Miasto → Klient

3. SLICE (Przekrój kostki):
   - Tylko rok 2018
   - Tylko region Sudeste
   - Tylko płatności kartą kredytową

4. DICE (Kostka z ograniczeniami):
   - Rok 2017-2018 AND Region Sudeste AND Review_Score >= 4

5. PIVOT (Obrót wymiarów):
   - Zmiana osi: Czas vs Geografia → Geografia vs Payment_Type


PRZYKŁADY ANALIZ:
=================

1. Analiza sprzedaży w czasie (drill-down czasowy):
   SELECT Year, Quarter, Month, SUM(Order_Value), AVG(Review_Score)
   FROM SALES_CUBE

2. Analiza geograficzna z danymi ekonomicznymi:
   SELECT Region, State, City_GDP_Per_Capita, SUM(Total_Revenue)
   FROM SALES_CUBE

3. Analiza korelacji PKB-Sprzedaż:
   SELECT GDP_Category, AVG(Order_Value), COUNT(Orders)
   FROM SALES_CUBE

4. Analiza efektywności dostawy (miara nieaddytywna):
   SELECT Payment_Type, AVG(Delivery_Days), AVG(Review_Score)
   FROM SALES_CUBE

5. Trend czasowy z YoY growth (miara kalkulowana):
   SELECT Month, Total_Revenue, YoY_Growth_Percent
   FROM SALES_CUBE


ZALETY ZAPROJEKTOWANEJ KOSTKI:
===============================

✓ Spełnia wymagania (5 wymiarów, 2 hierarchiczne, 3 miary, >10k rekordów)
✓ Integruje dane ekonomiczne miast (GDP, HDI) z danymi biznesowymi
✓ Umożliwia analizy wielopoziomowe (drill-down/roll-up)
✓ Zawiera miary nieaddytywne (średnie) i kalkulowane
✓ Obsługuje wszystkie operacje OLAP
✓ Dane rzeczywiste z systemu e-commerce (99k+ zamówień)
✓ Możliwość analiz korelacji społeczno-ekonomicznych
"""

# Dodatkowe funkcje analityczne

def create_olap_analysis_queries():
    """Zaawansowane zapytania analityczne OLAP"""
    
    advanced_queries = """
-- ===== ZAAWANSOWANE ANALIZY OLAP =====

-- 1. ANALIZA KOHORT CZASOWYCH (drill-down + roll-up)
WITH Customer_Cohorts AS (
    SELECT 
        c.Customer_ID,
        MIN(t.Year_Number) as First_Purchase_Year,
        MIN(t.Month_Number) as First_Purchase_Month
    FROM FACT_Orders f
    JOIN DIM_Time t ON f.Time_Key = t.Time_Key  
    JOIN DIM_Customer c ON f.Customer_Key = c.Customer_Key
    GROUP BY c.Customer_ID
),
Cohort_Analysis AS (
    SELECT 
        cc.First_Purchase_Year,
        cc.First_Purchase_Month,
        t.Year_Number as Purchase_Year,
        t.Month_Number as Purchase_Month,
        COUNT(DISTINCT f.Customer_Key) as Active_Customers,
        SUM(f.Order_Value + f.Freight_Value) as Cohort_Revenue
    FROM Customer_Cohorts cc
    JOIN FACT_Orders f ON cc.Customer_ID = (SELECT Customer_ID FROM DIM_Customer WHERE Customer_Key = f.Customer_Key)
    JOIN DIM_Time t ON f.Time_Key = t.Time_Key
    WHERE t.Year_Number >= cc.First_Purchase_Year
    GROUP BY cc.First_Purchase_Year, cc.First_Purchase_Month, t.Year_Number, t.Month_Number
)
SELECT * FROM Cohort_Analysis
ORDER BY First_Purchase_Year, First_Purchase_Month, Purchase_Year, Purchase_Month;

-- 2. ANALIZA CROSS-DIMENSIONAL (slice & dice)
-- Wpływ poziomu ekonomicznego na zachowania zakupowe
SELECT 
    CASE 
        WHEN c.City_HDI >= 0.8 THEN 'Wysokie HDI (≥0.8)'
        WHEN c.City_HDI >= 0.7 THEN 'Średnie HDI (0.7-0.8)'
        WHEN c.City_HDI >= 0.6 THEN 'Niskie HDI (0.6-0.7)'
        ELSE 'Bardzo niskie HDI (<0.6)'
    END as HDI_Category,
    p.Payment_Category,
    COUNT(*) as Orders_Count,
    AVG(f.Order_Value) as Avg_Order_Value,
    AVG(CAST(f.Review_Score as FLOAT)) as Avg_Satisfaction,
    SUM(f.Order_Value + f.Freight_Value) as Total_Revenue
FROM FACT_Orders f
JOIN DIM_Customer c ON f.Customer_Key = c.Customer_Key
JOIN DIM_Payment p ON f.Payment_Key = p.Payment_Key
WHERE c.City_HDI > 0 AND f.Review_Score > 0
GROUP BY 
    CASE 
        WHEN c.City_HDI >= 0.8 THEN 'Wysokie HDI (≥0.8)'
        WHEN c.City_HDI >= 0.7 THEN 'Średnie HDI (0.7-0.8)'
        WHEN c.City_HDI >= 0.6 THEN 'Niskie HDI (0.6-0.7)'
        ELSE 'Bardzo niskie HDI (<0.6)'
    END,
    p.Payment_Category
ORDER BY HDI_Category, Total_Revenue DESC;

-- 3. ANALIZA WIELOWYMIAROWA Z RANKING (advanced pivot)
WITH Regional_Performance AS (
    SELECT 
        c.Customer_Region,
        s.Seller_Region,
        t.Year_Number,
        COUNT(*) as Orders_Count,
        SUM(f.Order_Value + f.Freight_Value) as Total_Revenue,
        AVG(f.Order_Value) as Avg_Order_Value,
        AVG(CAST(f.Delivery_Days as FLOAT)) as Avg_Delivery_Days
    FROM FACT_Orders f
    JOIN DIM_Customer c ON f.Customer_Key = c.Customer_Key
    JOIN DIM_Seller s ON f.Seller_Key = s.Seller_Key  
    JOIN DIM_Time t ON f.Time_Key = t.Time_Key
    WHERE f.Delivery_Days IS NOT NULL
    GROUP BY c.Customer_Region, s.Seller_Region, t.Year_Number
)
SELECT 
    *,
    RANK() OVER (PARTITION BY Year_Number ORDER BY Total_Revenue DESC) as Revenue_Rank,
    NTILE(4) OVER (PARTITION BY Year_Number ORDER BY Avg_Delivery_Days) as Delivery_Quartile
FROM Regional_Performance
WHERE Orders_Count >= 10
ORDER BY Year_Number, Revenue_Rank;

-- 4. ANALIZA SEASONALITY Z MOVING AVERAGES (time series)
WITH Monthly_Sales AS (
    SELECT 
        t.Year_Number,
        t.Month_Number,
        t.Month_Name,
        SUM(f.Order_Value + f.Freight_Value) as Monthly_Revenue,
        COUNT(*) as Monthly_Orders,
        AVG(f.Review_Score) as Monthly_Satisfaction
    FROM FACT_Orders f
    JOIN DIM_Time t ON f.Time_Key = t.Time_Key
    WHERE f.Review_Score > 0
    GROUP BY t.Year_Number, t.Month_Number, t.Month_Name
)
SELECT 
    *,
    AVG(Monthly_Revenue) OVER (
        ORDER BY Year_Number, Month_Number 
        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
    ) as Moving_Avg_5M,
    LAG(Monthly_Revenue, 12) OVER (ORDER BY Year_Number, Month_Number) as Same_Month_Last_Year,
    CASE 
        WHEN LAG(Monthly_Revenue, 12) OVER (ORDER BY Year_Number, Month_Number) IS NOT NULL
        THEN ((Monthly_Revenue - LAG(Monthly_Revenue, 12) OVER (ORDER BY Year_Number, Month_Number)) 
              / LAG(Monthly_Revenue, 12) OVER (ORDER BY Year_Number, Month_Number) * 100)
        ELSE NULL
    END as YoY_Growth_Percent
FROM Monthly_Sales
ORDER BY Year_Number, Month_Number;

-- 5. ANALIZA PROFITABILITY BY SEGMENT (advanced calculations)
WITH Customer_Segments AS (
    SELECT 
        c.Customer_Key,
        c.Customer_Region,
        c.City_GDP_Per_Capita,
        COUNT(*) as Order_Frequency,
        SUM(f.Order_Value + f.Freight_Value) as Customer_Lifetime_Value,
        AVG(f.Order_Value) as Avg_Order_Value,
        AVG(CAST(f.Review_Score as FLOAT)) as Avg_Satisfaction,
        
        -- Segmentacja RFM (Recency, Frequency, Monetary)
        NTILE(5) OVER (ORDER BY COUNT(*)) as Frequency_Score,
        NTILE(5) OVER (ORDER BY SUM(f.Order_Value + f.Freight_Value)) as Monetary_Score
        
    FROM FACT_Orders f
    JOIN DIM_Customer c ON f.Customer_Key = c.Customer_Key
    WHERE f.Review_Score > 0
    GROUP BY c.Customer_Key, c.Customer_Region, c.City_GDP_Per_Capita
)
SELECT 
    Customer_Region,
    CASE 
        WHEN Frequency_Score >= 4 AND Monetary_Score >= 4 THEN 'Champions'
        WHEN Frequency_Score >= 3 AND Monetary_Score >= 3 THEN 'Loyal Customers'
        WHEN Monetary_Score >= 4 THEN 'Big Spenders'
        WHEN Frequency_Score >= 4 THEN 'Frequent Buyers'
        ELSE 'Regular Customers'
    END as Customer_Segment,
    COUNT(*) as Customers_Count,
    AVG(Customer_Lifetime_Value) as Avg_CLV,
    AVG(Avg_Order_Value) as Avg_AOV,
    AVG(Avg_Satisfaction) as Avg_Satisfaction,
    AVG(City_GDP_Per_Capita) as Avg_GDP_Per_Capita
FROM Customer_Segments
GROUP BY 
    Customer_Region,
    CASE 
        WHEN Frequency_Score >= 4 AND Monetary_Score >= 4 THEN 'Champions'
        WHEN Frequency_Score >= 3 AND Monetary_Score >= 3 THEN 'Loyal Customers'
        WHEN Monetary_Score >= 4 THEN 'Big Spenders'
        WHEN Frequency_Score >= 4 THEN 'Frequent Buyers'
        ELSE 'Regular Customers'
    END
ORDER BY Customer_Region, Avg_CLV DESC;
"""
    
    return advanced_queries

# Zapisanie zaawansowanych zapytań
def save_advanced_queries():
    queries_file = r'c:\Users\Kuba\PycharmProjects\hurtownie\advanced_olap_analysis.sql'
    with open(queries_file, 'w', encoding='utf-8') as f:
        f.write(create_olap_analysis_queries())
    print(f"Zaawansowane analizy OLAP zapisane w: {queries_file}")

if __name__ == "__main__":
    save_advanced_queries()
