from enhanced_etl_system import *


class T3_DimensionBuilder:
    """Task 3: Build all dimension tables with data cleansing and enrichment"""
    
    def __init__(self, config: ETLConfig, logger: ETLLogger, 
                 db_manager: DatabaseManager, data_extractor: T1_DataExtractor,
                 quality_manager: DataQualityManager):
        self.config = config
        self.logger = logger
        self.db_manager = db_manager
        self.data_extractor = data_extractor
        self.quality_manager = quality_manager
        self.metrics = {}
    
    def execute(self) -> bool:
        """Execute all dimension building tasks"""
        self.logger.info("=== T3: Starting Dimension Building ===")
        
        dimension_tasks = [
            ("Time", self._build_time_dimension),
            ("Customer", self._build_customer_dimension),
            ("Seller", self._build_seller_dimension),
            ("Payment", self._build_payment_dimension),
            ("Review", self._build_review_dimension)
        ]
        
        for dim_name, task_func in dimension_tasks:
            self.logger.info(f"T3: Building {dim_name} dimension...")
            if not task_func():
                self.logger.error(f"T3: Failed to build {dim_name} dimension")
                return False
        
        self.logger.info("T3: All dimensions built successfully")
        self._log_dimension_metrics()
        return True
    
    def _build_time_dimension(self) -> bool:
        """T3.1: Build time dimension with hierarchical date attributes"""
        conn = self.db_manager.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            start_date = datetime.strptime(self.config.start_date, '%Y-%m-%d')
            end_date = datetime.strptime(self.config.end_date, '%Y-%m-%d')
            current_date = start_date
            
            inserted_count = 0
            
            while current_date <= end_date:
                # Calculate hierarchical attributes
                day_name = current_date.strftime('%A')
                day_number = current_date.day
                week_number = current_date.isocalendar()[1]
                month_number = current_date.month
                month_name = current_date.strftime('%B')
                quarter_number = (current_date.month - 1) // 3 + 1
                quarter_name = f"Q{quarter_number}"
                year_number = current_date.year
                is_weekend = 1 if current_date.weekday() >= 5 else 0
                is_holiday = 0  # Can be extended with Brazilian holidays
                date_string = current_date.strftime('%Y-%m-%d')
                
                insert_sql = """
                INSERT INTO DIM_Time 
                (Date_Value, Day_Name, Day_Number, Week_Number, Month_Number, Month_Name,
                 Quarter_Number, Quarter_Name, Year_Number, Is_Weekend, Is_Holiday, Date_String)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                cursor.execute(insert_sql, (
                    current_date.date(), day_name, day_number, week_number, 
                    month_number, month_name, quarter_number, quarter_name, 
                    year_number, is_weekend, is_holiday, date_string
                ))
                
                inserted_count += 1
                current_date += timedelta(days=1)
            
            conn.commit()
            self.metrics['time'] = {'records': inserted_count}
            return True
            
        except Exception as e:
            self.logger.error(f"T3.1: Time dimension creation failed: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def _build_customer_dimension(self) -> bool:
        """T3.2: Build customer dimension with fuzzy city matching"""
        return self._build_geographic_dimension(
            'customer', 'DIM_Customer', 
            self.data_extractor.get_dataframe('customers'),
            'customer_id', 'customer_city', 'customer_state'
        )
    
    def _build_seller_dimension(self) -> bool:
        """T3.3: Build seller dimension with fuzzy city matching"""
        return self._build_geographic_dimension(
            'seller', 'DIM_Seller',
            self.data_extractor.get_dataframe('sellers'),
            'seller_id', 'seller_city', 'seller_state'
        )
    
    def _build_geographic_dimension(self, dim_type: str, table_name: str, 
                                  source_df: pd.DataFrame, id_col: str, 
                                  city_col: str, state_col: str) -> bool:
        """Generic method for building geographic dimensions with fuzzy matching"""
        conn = self.db_manager.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            cities_df = self.data_extractor.get_dataframe('cities')
            
            # Brazil regions mapping
            region_map = {
                'AC': 'North', 'AL': 'Northeast', 'AP': 'North', 'AM': 'North', 'BA': 'Northeast',
                'CE': 'Northeast', 'DF': 'Central-West', 'ES': 'Southeast', 'GO': 'Central-West',
                'MA': 'Northeast', 'MT': 'Central-West', 'MS': 'Central-West', 'MG': 'Southeast',
                'PA': 'North', 'PB': 'Northeast', 'PR': 'South', 'PE': 'Northeast', 'PI': 'Northeast',
                'RJ': 'Southeast', 'RN': 'Northeast', 'RS': 'South', 'RO': 'North', 'RR': 'North',
                'SC': 'South', 'SP': 'Southeast', 'SE': 'Northeast', 'TO': 'North'
            }
            
            # Prepare cities data for fuzzy matching
            cities_by_state = self._prepare_cities_data(cities_df)
            
            # Track matching statistics
            stats = {'exact_matches': 0, 'fuzzy_matches': 0, 'no_matches': 0}
            processed = 0
            
            for _, row in source_df.iterrows():
                entity_city = str(row[city_col]).strip()
                entity_state = str(row[state_col]).upper().strip()
                
                # Find matching city using fuzzy logic
                city_data = self.quality_manager.fuzzy_match_city(
                    entity_city, entity_state, cities_by_state, self.config.fuzzy_threshold
                )
                
                # Update matching statistics
                if city_data:
                    normalized_entity = self.quality_manager.normalize_city_name(entity_city)
                    if normalized_entity == city_data['normalized_name']:
                        stats['exact_matches'] += 1
                    else:
                        stats['fuzzy_matches'] += 1
                else:
                    stats['no_matches'] += 1
                
                # Assign region
                region = region_map.get(entity_state, 'Unknown')
                
                # Insert record
                if dim_type == 'customer':
                    self._insert_customer_record(cursor, row, region, city_data)
                else:  # seller
                    self._insert_seller_record(cursor, row, region, city_data)
                
                processed += 1
                if processed % self.config.batch_size == 0:
                    self.logger.info(f"T3: Processed {processed}/{len(source_df)} {dim_type}s...")
                    conn.commit()
            
            conn.commit()
            
            # Store metrics
            total_records = len(source_df)
            stats['total_records'] = total_records
            stats['match_rate'] = (stats['exact_matches'] + stats['fuzzy_matches']) / total_records * 100
            self.metrics[dim_type] = stats
            
            return True
            
        except Exception as e:
            self.logger.error(f"T3: {dim_type.title()} dimension creation failed: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def _prepare_cities_data(self, cities_df: pd.DataFrame) -> Dict[str, List[Dict]]:
        """Prepare cities data grouped by state for fuzzy matching"""
        cities_by_state = {}
        
        for _, row in cities_df.iterrows():
            state = str(row['STATE']).upper().strip()
            if state not in cities_by_state:
                cities_by_state[state] = []
            
            city_data = {
                'original_name': row['CITY'],
                'normalized_name': self.quality_manager.normalize_city_name(row['CITY']),
                'state': state,
                'population': row.get('IBGE_POP', 0) if pd.notna(row.get('IBGE_POP', 0)) else 0,
                'gdp_capita': row.get('GDP_CAPITA', 0) if pd.notna(row.get('GDP_CAPITA', 0)) else 0,
                'hdi': row.get('IDHM', 0) if pd.notna(row.get('IDHM', 0)) else 0,
                'hdi_income': row.get('IDHM_Renda', 0) if pd.notna(row.get('IDHM_Renda', 0)) else 0,
                'hdi_education': row.get('IDHM_Educacao', 0) if pd.notna(row.get('IDHM_Educacao', 0)) else 0,
                'hdi_longevity': row.get('IDHM_Longevidade', 0) if pd.notna(row.get('IDHM_Longevidade', 0)) else 0,
                'is_capital': 1 if row.get('CAPITAL', 0) == 1 else 0,
                'category': str(row.get('CATEGORIA_TUR', 'None')) if pd.notna(row.get('CATEGORIA_TUR')) else 'None'
            }
            cities_by_state[state].append(city_data)
        
        return cities_by_state
    
    def _insert_customer_record(self, cursor, customer, region: str, city_data: Dict):
        """Insert customer record with city data"""
        insert_sql = """
        INSERT INTO DIM_Customer 
        (Customer_ID, Customer_Unique_ID, Customer_Zip_Code, Customer_City, Customer_State, Customer_Region,
         City_Population, City_GDP_Per_Capita, City_HDI, City_HDI_Income, City_HDI_Education, 
         City_HDI_Longevity, City_Is_Capital, City_Category)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        cursor.execute(insert_sql, (
            customer['customer_id'],
            customer.get('customer_unique_id', ''),
            customer.get('customer_zip_code_prefix', ''),
            customer['customer_city'],
            customer['customer_state'],
            region,
            city_data.get('population', 0),
            city_data.get('gdp_capita', 0),
            city_data.get('hdi', 0),
            city_data.get('hdi_income', 0),
            city_data.get('hdi_education', 0),
            city_data.get('hdi_longevity', 0),
            city_data.get('is_capital', 0),
            city_data.get('category', 'None')
        ))
    
    def _insert_seller_record(self, cursor, seller, region: str, city_data: Dict):
        """Insert seller record with city data"""
        insert_sql = """
        INSERT INTO DIM_Seller 
        (Seller_ID, Seller_Zip_Code, Seller_City, Seller_State, Seller_Region,
         City_Population, City_GDP_Per_Capita, City_HDI, City_HDI_Income, City_HDI_Education, 
         City_HDI_Longevity, City_Is_Capital, City_Category)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        cursor.execute(insert_sql, (
            seller['seller_id'],
            seller.get('seller_zip_code_prefix', ''),
            seller['seller_city'],
            seller['seller_state'],
            region,
            city_data.get('population', 0),
            city_data.get('gdp_capita', 0),
            city_data.get('hdi', 0),
            city_data.get('hdi_income', 0),
            city_data.get('hdi_education', 0),
            city_data.get('hdi_longevity', 0),
            city_data.get('is_capital', 0),
            city_data.get('category', 'None')
        ))
    
    def _build_payment_dimension(self) -> bool:
        """T3.4: Build payment dimension with categorization"""
        conn = self.db_manager.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            payments_df = self.data_extractor.get_dataframe('payments')
            
            # Get unique payment combinations
            unique_payments = payments_df[['payment_type', 'payment_installments']].drop_duplicates()
            
            for _, payment in unique_payments.iterrows():
                payment_type = payment['payment_type']
                installments = payment['payment_installments']
                
                # Categorize payment type
                payment_category, is_credit = self._categorize_payment_type(payment_type)
                
                # Categorize installments
                installments_range = self._categorize_installments(installments)
                is_installment = 1 if installments > 1 else 0
                
                insert_sql = """
                INSERT INTO DIM_Payment 
                (Payment_Type, Payment_Category, Installments_Range, Is_Credit, Is_Installment)
                VALUES (?, ?, ?, ?, ?)
                """
                
                cursor.execute(insert_sql, (
                    payment_type, payment_category, installments_range, 
                    is_credit, is_installment
                ))
            
            conn.commit()
            self.metrics['payment'] = {'records': len(unique_payments)}
            return True
            
        except Exception as e:
            self.logger.error(f"T3.4: Payment dimension creation failed: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def _categorize_payment_type(self, payment_type: str) -> Tuple[str, int]:
        """Categorize payment type and determine if it's credit"""
        if payment_type == 'credit_card':
            return 'Credit Card', 1
        elif payment_type == 'boleto':
            return 'Boleto', 0
        elif payment_type == 'voucher':
            return 'Voucher', 0
        elif payment_type == 'debit_card':
            return 'Debit Card', 0
        else:
            return 'Other', 0
    
    def _categorize_installments(self, installments: int) -> str:
        """Categorize installment ranges"""
        if installments == 1:
            return '1 installment'
        elif installments <= 3:
            return '2-3 installments'
        elif installments <= 6:
            return '4-6 installments'
        elif installments <= 12:
            return '7-12 installments'
        else:
            return '13+ installments'
    
    def _build_review_dimension(self) -> bool:
        """T3.5: Build review dimension with satisfaction categorization"""
        conn = self.db_manager.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            reviews_df = self.data_extractor.get_dataframe('reviews')
            
            # Get unique review combinations
            review_pairs = set()
            for _, row in reviews_df.iterrows():
                score = row.get('review_score', 0)
                comment = row.get('review_comment_message', '')
                comment_category = self._categorize_comment_length(comment)
                review_pairs.add((score, comment_category))
            
            # Insert each unique combination
            for score, comment_category in review_pairs:
                review_category, satisfaction_level = self._categorize_review_score(score)
                has_comment = 0 if comment_category == 'No Comment' else 1
                
                insert_sql = """
                INSERT INTO DIM_Review 
                (Review_Score, Review_Category, Satisfaction_Level, Has_Comment, Comment_Length_Category)
                VALUES (?, ?, ?, ?, ?)
                """
                
                cursor.execute(insert_sql, (
                    score, review_category, satisfaction_level, 
                    has_comment, comment_category
                ))
            
            # Add record for no review
            cursor.execute(insert_sql, (0, 'No review', 'Unknown', 0, 'No Comment'))
            
            conn.commit()
            self.metrics['review'] = {'records': len(review_pairs) + 1}
            return True
            
        except Exception as e:
            self.logger.error(f"T3.5: Review dimension creation failed: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def _categorize_comment_length(self, comment: str) -> str:
        """Categorize comment by length"""
        if pd.isna(comment) or not comment or str(comment).strip() == '':
            return 'No Comment'
        
        length = len(str(comment))
        if length < 30:
            return 'Short (<30)'
        elif length < 50:
            return 'Medium (30-49)'
        elif length < 100:
            return 'Long (50-99)'
        elif length < 200:
            return 'Very Long (100-199)'
        else:
            return 'Extremely Long (200+)'
    
    def _categorize_review_score(self, score: int) -> Tuple[str, str]:
        """Categorize review score into satisfaction levels"""
        if score <= 2:
            return 'Negative', 'Unsatisfied'
        elif score == 3:
            return 'Neutral', 'Neutral'
        elif score >= 4:
            return 'Positive', 'Satisfied'
        else:
            return 'No review', 'Unknown'
    
    def _log_dimension_metrics(self):
        """Log dimension building metrics"""
        self.logger.info("T3: Dimension Building Metrics:")
        for dim_name, metrics in self.metrics.items():
            if dim_name in ['customer', 'seller']:
                self.logger.info(f"  {dim_name.title()}: {metrics['total_records']} records")
                self.logger.info(f"    Exact matches: {metrics['exact_matches']} ({metrics['exact_matches']/metrics['total_records']*100:.1f}%)")
                self.logger.info(f"    Fuzzy matches: {metrics['fuzzy_matches']} ({metrics['fuzzy_matches']/metrics['total_records']*100:.1f}%)")
                self.logger.info(f"    No matches: {metrics['no_matches']} ({metrics['no_matches']/metrics['total_records']*100:.1f}%)")
                self.logger.info(f"    Total match rate: {metrics['match_rate']:.1f}%")
            else:
                self.logger.info(f"  {dim_name.title()}: {metrics['records']} records")


class T4_FactBuilder:
    """Task 4: Build fact table with denormalized measures"""
    
    def __init__(self, config: ETLConfig, logger: ETLLogger, 
                 db_manager: DatabaseManager, data_extractor: T1_DataExtractor):
        self.config = config
        self.logger = logger
        self.db_manager = db_manager
        self.data_extractor = data_extractor
    
    def execute(self) -> bool:
        """Execute fact table building process"""
        self.logger.info("=== T4: Starting Fact Table Building ===")
        
        try:
            # Prepare fact data through joins
            fact_data = self._prepare_fact_data()
            if fact_data is None or fact_data.empty:
                self.logger.error("T4: No fact data prepared")
                return False
            
            # Get dimension lookup tables
            dim_keys = self._get_dimension_keys()
            if not dim_keys:
                self.logger.error("T4: Failed to retrieve dimension keys")
                return False
            
            # Load fact records
            success = self._load_fact_records(fact_data, dim_keys)
            
            if success:
                self.logger.info("T4: Fact table building completed successfully")
            
            return success
            
        except Exception as e:
            self.logger.error(f"T4: Fact table building failed: {e}")
            return False
    
    def _prepare_fact_data(self) -> Optional[pd.DataFrame]:
        """Prepare fact data by joining source tables"""
        try:
            self.logger.info("T4: Joining source data for fact table...")
            
            orders_df = self.data_extractor.get_dataframe('orders')
            order_items_df = self.data_extractor.get_dataframe('order_items')
            payments_df = self.data_extractor.get_dataframe('payments')
            reviews_df = self.data_extractor.get_dataframe('reviews')
            
            # Aggregate order items by order
            items_agg = order_items_df.groupby('order_id').agg({
                'price': 'sum',
                'freight_value': 'sum',
                'order_item_id': 'count',
                'seller_id': 'first'  # Take first seller for the order
            }).reset_index()
            
            # Join orders with items
            fact_data = orders_df.merge(items_agg, on='order_id', how='inner')
            
            # Aggregate payments by order
            payments_agg = payments_df.groupby('order_id').agg({
                'payment_type': 'first',
                'payment_installments': 'first',
                'payment_value': 'sum'
            }).reset_index()
            
            # Join with payments
            fact_data = fact_data.merge(payments_agg, on='order_id', how='left')
            
            # Join with reviews
            fact_data = fact_data.merge(
                reviews_df[['order_id', 'review_score']], 
                on='order_id', how='left'
            )
            
            self.logger.info(f"T4: Prepared {len(fact_data)} fact records")
            return fact_data
            
        except Exception as e:
            self.logger.error(f"T4: Failed to prepare fact data: {e}")
            return None
    
    def _get_dimension_keys(self) -> Dict[str, Dict]:
        """Retrieve dimension key mappings"""
        conn = self.db_manager.get_connection()
        if not conn:
            return {}
        
        try:
            cursor = conn.cursor()
            dim_keys = {}
            
            # Time keys
            cursor.execute("SELECT Time_Key, Date_Value FROM DIM_Time")
            dim_keys['time'] = {row[1]: row[0] for row in cursor.fetchall()}
            
            # Customer keys
            cursor.execute("SELECT Customer_Key, Customer_ID FROM DIM_Customer")
            dim_keys['customer'] = {row[1]: row[0] for row in cursor.fetchall()}
            
            # Seller keys
            cursor.execute("SELECT Seller_Key, Seller_ID FROM DIM_Seller")
            dim_keys['seller'] = {row[1]: row[0] for row in cursor.fetchall()}
            
            # Payment keys
            cursor.execute("SELECT Payment_Key, Payment_Type, Installments_Range FROM DIM_Payment")
            payment_keys = {}
            for row in cursor.fetchall():
                payment_keys[f"{row[1]}_{row[2]}"] = row[0]
            dim_keys['payment'] = payment_keys
            
            # Review keys
            cursor.execute("SELECT Review_Key, Review_Score FROM DIM_Review")
            dim_keys['review'] = {row[1]: row[0] for row in cursor.fetchall()}
            
            return dim_keys
            
        except Exception as e:
            self.logger.error(f"T4: Failed to retrieve dimension keys: {e}")
            return {}
        finally:
            conn.close()
    
    def _load_fact_records(self, fact_data: pd.DataFrame, dim_keys: Dict) -> bool:
        """Load fact records with dimension key lookups"""
        conn = self.db_manager.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            inserted_count = 0
            error_count = 0
            
            for _, row in fact_data.iterrows():
                try:
                    # Process dates and calculate measures
                    purchase_date = pd.to_datetime(row['order_purchase_timestamp']).date()
                    delivery_date = pd.to_datetime(row.get('order_delivered_customer_date', None), errors='coerce')
                    estimated_delivery = pd.to_datetime(row.get('order_estimated_delivery_date', None), errors='coerce')
                    
                    # Calculate delivery days
                    delivery_days = None
                    if delivery_date is not None and not pd.isna(delivery_date):
                        delivery_days = (delivery_date.date() - purchase_date).days
                        delivery_date = delivery_date.date()
                    else:
                        delivery_date = None
                    
                    if estimated_delivery is not None and not pd.isna(estimated_delivery):
                        estimated_delivery = estimated_delivery.date()
                    else:
                        estimated_delivery = None
                    
                    # Lookup dimension keys
                    time_key = dim_keys['time'].get(purchase_date)
                    customer_key = dim_keys['customer'].get(row['customer_id'])
                    seller_key = dim_keys['seller'].get(row.get('seller_id'))
                    
                    # Get payment key
                    payment_type = row.get('payment_type', 'unknown')
                    installments = row.get('payment_installments', 1)
                    installments_range = self._get_installments_range(installments)
                    payment_key = dim_keys['payment'].get(f"{payment_type}_{installments_range}")
                    
                    # Get review key
                    review_score = row.get('review_score', 0)
                    if pd.isna(review_score):
                        review_score = 0
                    review_key = dim_keys['review'].get(int(review_score))
                    
                    # Only insert if all keys are found
                    if all([time_key, customer_key, seller_key, payment_key, review_key]):
                        self._insert_fact_record(
                            cursor, row, time_key, customer_key, seller_key,
                            payment_key, review_key, delivery_days, review_score,
                            purchase_date, delivery_date, estimated_delivery
                        )
                        inserted_count += 1
                        
                        if inserted_count % self.config.batch_size == 0:
                            self.logger.info(f"T4: Inserted {inserted_count} records...")
                            conn.commit()
                    else:
                        error_count += 1
                        if error_count <= 5:  # Log first few errors
                            self.logger.warning(f"T4: Missing keys for order {row['order_id']}")
                
                except Exception as e:
                    error_count += 1
                    if error_count <= 5:
                        self.logger.warning(f"T4: Error processing order {row['order_id']}: {e}")
                    continue
            
            conn.commit()
            self.logger.info(f"T4: Fact table loaded: {inserted_count} records, {error_count} errors")
            
            # Log success rate
            total_records = len(fact_data)
            success_rate = (inserted_count / total_records) * 100 if total_records > 0 else 0
            self.logger.info(f"T4: Load success rate: {success_rate:.1f}%")
            
            return True
            
        except Exception as e:
            self.logger.error(f"T4: Fact loading failed: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def _get_installments_range(self, installments: int) -> str:
        """Get installments range category"""
        if installments == 1:
            return '1 installment'
        elif installments <= 3:
            return '2-3 installments'
        elif installments <= 6:
            return '4-6 installments'
        elif installments <= 12:
            return '7-12 installments'
        else:
            return '13+ installments'
    
    def _insert_fact_record(self, cursor, row, time_key, customer_key, seller_key,
                          payment_key, review_key, delivery_days, review_score,
                          purchase_date, delivery_date, estimated_delivery):
        """Insert single fact record"""
        insert_sql = """
        INSERT INTO FACT_Orders 
        (Order_ID, Time_Key, Customer_Key, Seller_Key, Payment_Key, Review_Key,
         Order_Value, Freight_Value, Items_Count, Delivery_Days, Review_Score,
         Purchase_Date, Delivery_Date, Estimated_Delivery_Date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        cursor.execute(insert_sql, (
            row['order_id'],
            time_key,
            customer_key,
            seller_key,
            payment_key,
            review_key,
            float(row.get('price', 0)),
            float(row.get('freight_value', 0)),
            int(row.get('order_item_id', 1)),
            delivery_days,
            int(review_score) if review_score > 0 else None,
            purchase_date,
            delivery_date,
            estimated_delivery
        ))
