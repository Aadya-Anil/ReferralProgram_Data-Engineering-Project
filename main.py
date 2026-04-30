import pandas as pd
import pytz

# ============================================================
#                        DATA LOADING  
# ============================================================

user_referrals = pd.read_csv("data/user_referrals.csv")
user_referral_statuses = pd.read_csv("data/user_referral_statuses.csv")
user_referral_logs = pd.read_csv("data/user_referral_logs.csv")
user_logs = pd.read_csv("data/user_logs.csv")
referral_rewards = pd.read_csv("data/referral_rewards.csv")
paid_transactions = pd.read_csv("data/paid_transactions.csv")
lead_log = pd.read_csv("data/lead_log.csv")


"""
# ============================================================
# DATA PROFILING 
# ============================================================
def profile_table(df, table_name):
    profile_data = []
    for col in df.columns:
        null_count = df[col].isnull().sum()
        distinct_count = df[col].nunique()
        profile_data.append({
            "Column Name": col,
            "Data Type": str(df[col].dtype),
            "Null Count": null_count,
            "Distinct Value Count": distinct_count
        })
    
    profile_df = pd.DataFrame(profile_data)
    profile_df.to_csv(f"profiled_{table_name}.csv", index=False)
    print(f"Profiled {table_name} — saved to profiled_{table_name}.csv")
    return profile_df

# Run profiling on all tables
profile_table(lead_log, "lead_log")
profile_table(user_referrals, "user_referrals")
profile_table(user_referral_logs, "user_referral_logs")
profile_table(user_logs, "user_logs")
profile_table(user_referral_statuses, "user_referral_statuses")
profile_table(referral_rewards, "referral_rewards")
profile_table(paid_transactions, "paid_transactions")
"""
# ============================================================
#                      DATA CLEANING 
# ============================================================

# Converting timestamps from string to datetime
user_referrals['referral_at'] = pd.to_datetime(user_referrals['referral_at'])
user_referrals['updated_at'] = pd.to_datetime(user_referrals['updated_at'])
user_referral_logs['created_at'] = pd.to_datetime(user_referral_logs['created_at'])
user_logs['membership_expired_date'] = pd.to_datetime(user_logs['membership_expired_date'])
user_referral_statuses['created_at'] = pd.to_datetime(user_referral_statuses['created_at'])
referral_rewards['created_at'] = pd.to_datetime(referral_rewards['created_at'])
paid_transactions['transaction_at'] = pd.to_datetime(paid_transactions['transaction_at'])
lead_log['created_at'] = pd.to_datetime(lead_log['created_at'])

#converting all string values EXCEPT club names to title case(initcapping)
user_logs['name'] = user_logs['name'].str.title()
user_referrals['referee_name'] = user_referrals['referee_name'].str.title()
paid_transactions['transaction_status'] = paid_transactions['transaction_status'].str.title()
paid_transactions['transaction_type'] = paid_transactions['transaction_type'].str.title()

# Removed duplicate rows from all tables
user_referrals = user_referrals.drop_duplicates()
user_referral_logs = user_referral_logs.drop_duplicates()
user_logs = user_logs.drop_duplicates()
user_referral_statuses = user_referral_statuses.drop_duplicates()
referral_rewards = referral_rewards.drop_duplicates()
paid_transactions = paid_transactions.drop_duplicates()
lead_log = lead_log.drop_duplicates()

#function to convert a UTC timestamp column to local time based on a timezone column
def convert_timezone(df, timestamp_col, timezone_col):
    def convert_row(row):
        if pd.isnull(row[timestamp_col]) or pd.isnull(row[timezone_col]):
            return row[timestamp_col]  # return as-is if null
        tz = pytz.timezone(row[timezone_col])
        utc_time = row[timestamp_col].replace(tzinfo=pytz.utc)
        return utc_time.astimezone(tz).replace(tzinfo=None)
    
    df[timestamp_col] = df.apply(convert_row, axis=1)
    return df

# Convert each table
user_logs = convert_timezone(user_logs, 'membership_expired_date', 'timezone_homeclub')
paid_transactions = convert_timezone(paid_transactions, 'transaction_at', 'timezone_transaction')
lead_log = convert_timezone(lead_log, 'created_at', 'timezone_location')

#for the latest logs in user_logs
user_logs_latest = user_logs.sort_values('membership_expired_date').groupby('user_id').last().reset_index()
logs_latest = user_referral_logs.groupby('user_referral_id').agg(
    created_at=('created_at', 'last'),
    is_reward_granted=('is_reward_granted', 'any')
).reset_index()


# for  lead_log to keep only latest record per lead
lead_logs_latest = lead_log.sort_values('created_at').groupby('lead_id').last().reset_index()

# Joining user_referrals with user_referral_statuses to get status description
df1 = user_referrals.merge(
    user_referral_statuses[['id', 'description']],
    left_on='user_referral_status_id',
    right_on='id',
    how='left'
).drop(columns=['id'])


# Joining with referral_rewards to get reward value
df2 = df1.merge(
    referral_rewards[['id', 'reward_value', 'reward_type']],
    left_on='referral_reward_id',
    right_on='id',
    how='left'
).drop(columns=['id'])

# Join with user_logs to get REFERRER details
df3 = df2.merge(
    user_logs_latest[['user_id', 'name', 'phone_number', 'homeclub', 'timezone_homeclub', 'membership_expired_date', 'is_deleted']],
    left_on='referrer_id',
    right_on='user_id',
    how='left'
).drop(columns=['user_id'])


# Join with paid_transactions
df4 = df3.merge(
    paid_transactions[['transaction_id', 'transaction_status', 'transaction_at', 'transaction_location', 'transaction_type', 'timezone_transaction']],
    on='transaction_id',
    how='left'
)

# Join with user_referral_logs (only latest log per referral to avoid duplicates)
df5 = df4.merge(
    logs_latest[['user_referral_id', 'created_at', 'is_reward_granted']],
    left_on='referral_id',
    right_on='user_referral_id',
    how='left'
).drop(columns=['user_referral_id'])

# Join with lead_logs for referrals where source is "Lead"
df = df5.merge(
    lead_logs_latest[['lead_id', 'source_category']],
    left_on='referee_id',
    right_on='lead_id',
    how='left'
).drop(columns=['lead_id'])

# filling null columns
df['timezone_homeclub'] = df['timezone_homeclub'].fillna('Asia/Jakarta')
df['timezone_transaction'] = df['timezone_transaction'].fillna('Asia/Jakarta')

#time conversion  of joined tables
df = convert_timezone(df, 'referral_at', 'timezone_homeclub')
df = convert_timezone(df, 'updated_at', 'timezone_homeclub')
df = convert_timezone(df, 'created_at', 'timezone_homeclub')
df = convert_timezone(df, 'transaction_at', 'timezone_transaction')

#Determine referral_source_category
df['referral_source_category'] = df.apply(
    lambda row: 
        'Online' if row['referral_source'] == 'User Sign Up'
        else 'Offline' if row['referral_source'] == 'Draft Transaction'
        else row['source_category'] if row['referral_source'] == 'Lead'
        else None,
    axis=1
)
df['referral_source_category'] = df['referral_source_category'].fillna('Unknown')

# ============================================================
#            BUSINESS LOGIC for FRAUD DETECTION
# ============================================================

# Extract numeric value from reward_value (e.g. "20 days" -> 20)
df['reward_value'] = df['reward_value'].str.extract(r'(\d+)').astype(float)

# Fill NaN in is_deleted and is_reward_granted with False
df['is_deleted'] = df['is_deleted'].fillna(False)
df['is_reward_granted'] = df['is_reward_granted'].fillna(False)

#Striping timezone info from all timestamp columns
def strip_timezone(col):
    return pd.to_datetime(col, utc=True).dt.tz_localize(None)

df['referral_at'] = strip_timezone(df['referral_at'])
df['updated_at'] = strip_timezone(df['updated_at'])
df['transaction_at'] = strip_timezone(df['transaction_at'])
df['created_at'] = strip_timezone(df['created_at'])
df['membership_expired_date'] = strip_timezone(df['membership_expired_date'])

def check_business_logic(row):
    
    has_reward = pd.notnull(row['reward_value']) and row['reward_value'] > 0
    is_successful = row['description'] == 'Berhasil'
    is_pending_or_failed = row['description'] in ['Menunggu', 'Tidak Berhasil']
    has_transaction = pd.notnull(row['transaction_id'])
    is_paid = row['transaction_status'] == 'Paid'
    is_new = row['transaction_type'] == 'New'
    is_not_deleted = row['is_deleted'] == False
    is_reward_granted = row['is_reward_granted'] == True
    
    # Check membership not expired
    membership_valid = False
    if pd.notnull(row['membership_expired_date']) and pd.notnull(row['referral_at']):
        membership_valid = row['membership_expired_date'] > row['referral_at']
    
    # Check transaction after referral
    transaction_after_referral = False
    if pd.notnull(row['transaction_at']) and pd.notnull(row['referral_at']):
        transaction_after_referral = row['transaction_at'] > row['referral_at']
    
    # Check same month
    same_month = False
    if pd.notnull(row['transaction_at']) and pd.notnull(row['referral_at']):
        same_month = (row['transaction_at'].month == row['referral_at'].month and
                     row['transaction_at'].year == row['referral_at'].year)

    # VALID Condition 1
    if (has_reward and is_successful and has_transaction and is_paid and
    is_new and transaction_after_referral and same_month and
    membership_valid and is_not_deleted):
        return True

    # VALID Condition 2
    if is_pending_or_failed and not has_reward:
        return True

    # INVALID Condition 1
    if has_reward and not is_successful:
        return False

    # INVALID Condition 2
    if has_reward and not has_transaction:
        return False

    # INVALID Condition 3
    if not has_reward and has_transaction and is_paid and transaction_after_referral:
        return False

    # INVALID Condition 4
    if is_successful and not has_reward:
        return False

    # INVALID Condition 5
    if has_transaction and pd.notnull(row['transaction_at']) and pd.notnull(row['referral_at']):
        if row['transaction_at'] < row['referral_at']:
            return False

    return False

df['is_business_logic_valid'] = df.apply(check_business_logic, axis=1)

# ============================================================
#                         OUTPUT
# ============================================================

final_report = df[[
    'referral_id',
    'referral_source',
    'referral_source_category',
    'referral_at',
    'referrer_id',
    'name',
    'phone_number',
    'homeclub',
    'referee_id',
    'referee_name',
    'referee_phone',
    'description',
    'reward_value',
    'transaction_id',
    'transaction_status',
    'transaction_at',
    'transaction_location',
    'transaction_type',
    'updated_at',
    'created_at',
    'is_reward_granted',
    'is_business_logic_valid'
]].copy()

# Rename columns to match expected output
final_report = final_report.rename(columns={
    'name': 'referrer_name',
    'phone_number': 'referrer_phone_number',
    'homeclub': 'referrer_homeclub',
    'description': 'referral_status',
    'reward_value': 'num_reward_days',
    'created_at': 'reward_granted_at'
})

# Add referral_details_id
final_report.insert(0, 'referral_details_id', range(101, 101 + len(final_report)))

# Save to CSV
final_report.to_csv('referral_report.csv', index=False)

