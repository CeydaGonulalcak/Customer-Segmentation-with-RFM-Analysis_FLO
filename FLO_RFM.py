###############################################################
# Customer Segmentation with RFM
###############################################################


###############################################################
# TASKS
###############################################################

# TASK 1: Data Understanding and Preparation
           #1. Read the flo_data_20K.csv data.
           #2. In the dataset
                     # a. top 10 observations,
                     # b. variable names,
                     # c. descriptive statistics,
                     # D. null value,
                     # e. Variable types, review.
           # 3. Omnichannel means that customers shop from both online and offline platforms. Total for each customer create new variables for number of purchases and spend.
           # 4. Examine the variable types. Change the type of variables that express date to date.
           # 5. Look at the breakdown of the number of customers, average number of products purchased, and average spend in shopping channels.
           # 6. Rank the top 10 customers with the most revenue.
           # 7. Rank the top 10 customers with the most orders.
           # 8. Functionalize the data provisioning process.

# TASK 2: Calculating RFM Metrics

# TASK 3: Calculating RF and RFM Scores

# TASK 4: Defining RF Scores as Segments

# TASK 5: Time for action!
           # 1. Examine the recency, frequency and monetary averages of the segments.
           # 2. With the help of RFM analysis, find the customers in the relevant profile for 2 cases and save the customer IDs to the csv.
                   # a. FLO includes a new women's shoe brand. The product prices of the brand it includes are above the general customer preferences. Therefore, the brand
                   # It is desired to contact the customers in the profile that will be interested in promotion and product sales. From their loyal customers(champions,loyal_customers),
                   # People who shop from the women category with an average of 250 TL or more are the customers who will be contacted privately. Id numbers of these customers to csv file
                   # save as new_brand_target_customer_id.cvs.
                   # b. Up to 40% discount is planned for Men's and Children's products. Good past customer but long-standing customer interested in categories related to this sale
                   # customers who should not be lost who do not shop, those who are asleep and new customers are specifically targeted. Enter the ids of the customers in the appropriate profile into the csv file discount_target_customer_ids.csv, save it as.
                   
# TASK 6: Functionalize the whole process.


###############################################################
# TASK 1: Data Understanding
###############################################################

import pandas as pd
import datetime as dt
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x)
pd.set_option('display.width',1000)

df_ = pd.read_csv("CRM Analytics/Proje 1/FLOMusteriSegmentasyonu-221114-233246/FLOMusteriSegmentasyonu/flo_data_20k.csv")
df = df_.copy()
df.head()

df.head(10)
df.columns
df.shape
df.describe().T
df.isnull().sum()
df.info()

df["order_num_total"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
df["customer_value_total"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_online"]

date_columns = df.columns[df.columns.str.contains("date")]
df[date_columns] = df[date_columns].apply(pd.to_datetime)
df.info()

df.groupby("order_channel").agg({"master_id":"count",
                                 "order_num_total":"sum",
                                 "customer_value_total":"sum"})

df.sort_values("customer_value_total", ascending=False)[:10]

df.sort_values("order_num_total", ascending=False)[:10]

def data_prep(dataframe):
    dataframe["order_num_total"] = dataframe["order_num_total_ever_online"] + dataframe["order_num_total_ever_offline"]
    dataframe["customer_value_total"] = dataframe["customer_value_total_ever_offline"] + dataframe["customer_value_total_ever_online"]
    date_columns = dataframe.columns[dataframe.columns.str.contains("date")]
    dataframe[date_columns] = dataframe[date_columns].apply(pd.to_datetime)
    return df


###############################################################
# TASK 2: Calculating RFM Metrics
###############################################################

df["last_order_date"].max() # 2021-05-30
analysis_date = dt.datetime(2021,6,1)

rfm = pd.DataFrame()
rfm["customer_id"] = df["master_id"]
rfm["recency"] = (analysis_date - df["last_order_date"]).astype('timedelta64[D]')
rfm["frequency"] = df["order_num_total"]
rfm["monetary"] = df["customer_value_total"]

rfm.head()


###############################################################
# TASK 3: Calculating RF and RFM Scores
###############################################################

rfm["recency_score"] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1])
rfm["frequency_score"] = pd.qcut(rfm['frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
rfm["monetary_score"] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])

rfm.head()

rfm["RF_SCORE"] = (rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str))

rfm["RFM_SCORE"] = (rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str) + rfm['monetary_score'].astype(str))

rfm.head()


###############################################################
# TASK 4: Segment Definition of RF Scores
###############################################################

seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}

rfm['segment'] = rfm['RF_SCORE'].replace(seg_map, regex=True)

rfm.head()


###############################################################
# TASK 5: Time for action!
###############################################################

rfm[["segment", "recency", "frequency", "monetary"]].groupby("segment").agg(["mean", "count"])

target_segments_customer_ids = rfm[rfm["segment"].isin(["champions","loyal_customers"])]["customer_id"]
cust_ids = df[(df["master_id"].isin(target_segments_customer_ids)) &(df["interested_in_categories_12"].str.contains("KADIN"))]["master_id"]
cust_ids.to_csv("yeni_marka_hedef_müşteri_id.csv", index=False)
cust_ids.shape

rfm.head()

target_segments_customer_ids = rfm[rfm["segment"].isin(["cant_loose","hibernating","new_customers"])]["customer_id"]
cust_ids = df[(df["master_id"].isin(target_segments_customer_ids)) & ((df["interested_in_categories_12"].str.contains("ERKEK"))|(df["interested_in_categories_12"].str.contains("COCUK")))]["master_id"]
cust_ids.to_csv("indirim_hedef_müşteri_ids.csv", index=False)


###############################################################
# TASK 6: Functionalize the whole process.
###############################################################

def create_rfm(dataframe):
    # Preparing the Data
    dataframe["order_num_total"] = dataframe["order_num_total_ever_online"] + dataframe["order_num_total_ever_offline"]
    dataframe["customer_value_total"] = dataframe["customer_value_total_ever_offline"] + dataframe["customer_value_total_ever_online"]
    date_columns = dataframe.columns[dataframe.columns.str.contains("date")]
    dataframe[date_columns] = dataframe[date_columns].apply(pd.to_datetime)

    # Calculation of RFM Metrics
    dataframe["last_order_date"].max()  # 2021-05-30
    analysis_date = dt.datetime(2021, 6, 1)
    rfm = pd.DataFrame()
    rfm["customer_id"] = dataframe["master_id"]
    rfm["recency"] = (analysis_date - dataframe["last_order_date"]).astype('timedelta64[D]')
    rfm["frequency"] = dataframe["order_num_total"]
    rfm["monetary"] = dataframe["customer_value_total"]

    # Calculation of RF and RFM Scores
    rfm["recency_score"] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1])
    rfm["frequency_score"] = pd.qcut(rfm['frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
    rfm["monetary_score"] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])
    rfm["RF_SCORE"] = (rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str))
    rfm["RFM_SCORE"] = (rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str) + rfm['monetary_score'].astype(str))

    # Naming of Segments
    seg_map = {
        r'[1-2][1-2]': 'hibernating',
        r'[1-2][3-4]': 'at_Risk',
        r'[1-2]5': 'cant_loose',
        r'3[1-2]': 'about_to_sleep',
        r'33': 'need_attention',
        r'[3-4][4-5]': 'loyal_customers',
        r'41': 'promising',
        r'51': 'new_customers',
        r'[4-5][2-3]': 'potential_loyalists',
        r'5[4-5]': 'champions'
    }
    rfm['segment'] = rfm['RF_SCORE'].replace(seg_map, regex=True)

    return rfm[["customer_id", "recency","frequency","monetary","RF_SCORE","RFM_SCORE","segment"]]


rfm_df = create_rfm(df)

