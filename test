"""
VVD Vintage Curve Analysis - All 6 Campaigns
=============================================

UNIVERSAL STANDARDS:
- Aggregation: MONTH level (yyyy-MM) - never weekly/daily
- Years: 2025 and 2026 only (exclude older data)
- Test (TG4) vs Control on same plot
- Same color per cohort: solid = Test, dashed = Control
- One plot per campaign
- Measurement window = TREATMT_END_DT - TREATMT_STRT_DT

Campaigns:
- VCN, VDA: Acquisition (ISS_DT)
- VDT: Activation (ACTV_DT)
- VUI: Usage (TXN_DT from POS transactions)
- VUT, VAW: Tokenization (TXN_DT from EDW token query)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark import StorageLevel
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from scipy import stats

# =============================================================================
# CONFIGURATION - ALL 6 CAMPAIGNS
# =============================================================================

CAMPAIGN_CONFIG = {
    "VCN": {
        "success_type": "ACQUISITION",
        "success_source": "HIVE",
        "success_table_path": "/prod/sz/tsz/00050/data/DDWTA_VISA_DR_CRD/PartitionColumn=Latest/CAPTR_DT=",
        "success_date_field": "ISS_DT",
        "filters": {
            "STS_CD": ["06", "08"],      # Status codes
            "SRVC_ID": 36,                # Service ID
            "ISS_DT_NOT_NULL": True       # Issue date not null
        },
        "description": "VVD Contextual Notification"
    },
    "VDA": {
        "success_type": "ACQUISITION",
        "success_source": "HIVE",
        "success_table_path": "/prod/sz/tsz/00050/data/DDWTA_VISA_DR_CRD/PartitionColumn=Latest/CAPTR_DT=",
        "success_date_field": "ISS_DT",
        "filters": {
            "STS_CD": ["06", "08"],
            "SRVC_ID": 36,
            "ISS_DT_NOT_NULL": True
        },
        "description": "VVD Black Friday Cyber Monday Targeted"
    },
    "VDT": {
        "success_type": "ACTIVATION",
        "success_source": "HIVE",
        "success_table_path": "/prod/sz/tsz/00050/data/DDWTA_VISA_DR_CRD/PartitionColumn=Latest/CAPTR_DT=",
        "success_date_field": "ACTV_DT",
        "filters": {
            "STS_CD": ["06", "08"],
            "SRVC_ID": 36,
            "ISS_DT_NOT_NULL": True
        },
        "description": "VVD Activation Trigger"
    },
    "VUI": {
        "success_type": "USAGE",
        "success_source": "HIVE",
        "success_table_path": "/prod/sz/tsz/00050/data/DDWTA_T_PT_OF_SALE_TXN/SNAP_DT=",
        "success_date_field": "TXN_DT",
        "filters": {
            "SRVC_CD": 36,
            "TXN_TYPES": [
                {"TXN_TP": 10, "MSG_TP": "0210"},
                {"TXN_TP": 13, "MSG_TP": "0210"},
                {"TXN_TP": 12, "MSG_TP": "0220"}
            ],
            "AMT1_GT": 0,
            "EXTRACT_CLNT_NO": True  # Extract CLNT_NO from CLNT_CRD_NO
        },
        "description": "VVD Usage Trigger"
    },
    "VUT": {
        "success_type": "TOKENIZATION",
        "success_source": "EDW",
        "success_table_path": "/user/427966379/token.parquet",  # Pre-pulled from EDW
        "success_date_field": "TXN_DT",
        "filters": None,
        "description": "VVD Tokenization Usage Campaign"
    },
    "VAW": {
        "success_type": "TOKENIZATION",
        "success_source": "EDW",
        "success_table_path": "/user/427966379/token.parquet",  # Pre-pulled from EDW
        "success_date_field": "TXN_DT",
        "filters": None,
        "description": "VVD Add To Wallet Contextual Notification"
    },
}

# =============================================================================
# INITIALIZE SPARK
# =============================================================================

spark = SparkSession.builder \
    .appName("VVD Vintage Curves - All Campaigns") \
    .getOrCreate()

# =============================================================================
# EDW TOKEN QUERY (Run once, save to parquet)
# =============================================================================

def pull_token_data_from_edw():
    """
    Pull token provisioning data from EDW.
    Run this once and save to parquet for reuse.
    Assumes EDW connection is already established.
    """
    cursor = EDW.cursor()
    
    query = """
    SELECT DISTINCT
        SUBSTR(B.CLNT_CRD_NO, 7, 9) AS CLNT_NO,
        B.TXN_DT
    FROM DDWV05.CLNT_CRD_POS_LOG AS B
    INNER JOIN DL_DECMAN.TOKEN_LIST C
        ON B.TOKN_REQSTR_ID = C.TOKEN_ID
    WHERE B.AMT1 = 0
        AND SUBSTR(B.CLNT_CRD_NO, 1, 5) = '45190'
        AND SUBSTR(B.VISA_DR_CRD_NO, 1, 5) = '45199'
        AND SUBSTR(B.TOKN_REQSTR_ID, 1, 1) > '0'
        AND B.POS_ENTR_MODE_CD_NON_EMV = '000'
        AND B.SRVC_CD = 36
        AND C.TOKEN_WALLET_IND = 'Y'
    ORDER BY 1
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    
    token_list_df = pd.DataFrame(results, columns=column_names)
    token_list_df['CLNT_NO'] = token_list_df['CLNT_NO'].astype(int)
    
    cursor.close()
    
    # Convert to Spark and save
    token_spark_df = spark.createDataFrame(token_list_df)
    token_spark_df.write.parquet("/user/427966379/token.parquet", mode="overwrite")
    
    print(f"Token data saved: {token_spark_df.count():,} records")
    return token_spark_df


# =============================================================================
# STANDARD CONFIGURATION
# =============================================================================

# Universal aggregation rule - MONTH level for all campaigns
AGGREGATION_LEVEL = "MONTH"  # Always yyyy-MM, never weekly/daily

# Year filter - only include these years
YEARS_TO_INCLUDE = [2025, 2026]


# =============================================================================
# DATA LOADING FUNCTIONS
# =============================================================================

def load_tactic(mne):
    """Load tactic data and filter for specific campaign and years."""
    tactic = spark.read.parquet("/user/427966379/tactic.parquet")
    
    # Filter by MNE and YEARS (2025, 2026 only)
    tactic = tactic.filter(
        (F.col("MNE") == mne) &
        (F.year(F.col("TREATMT_STRT_DT")).isin(YEARS_TO_INCLUDE))
    )
    
    return tactic


def load_success_table_hive(config, years=["2025", "2026"]):
    """Load success table from Hive with year partitions and apply filters."""
    paths = [f"{config['success_table_path']}{year}*" for year in years]
    df = spark.read.parquet(*paths)
    
    # Apply filters if specified
    if config.get("filters"):
        filters = config["filters"]
        
        # STS_CD filter (for activation/acquisition)
        if "STS_CD" in filters:
            df = df.filter(F.col("STS_CD").isin(filters["STS_CD"]))
        
        # SRVC_ID filter (for activation/acquisition - note: SRVC_ID not SRVC_CD)
        if "SRVC_ID" in filters:
            df = df.filter(F.col("SRVC_ID") == filters["SRVC_ID"])
        
        # SRVC_CD filter (for usage)
        if "SRVC_CD" in filters:
            df = df.filter(F.col("SRVC_CD") == filters["SRVC_CD"])
        
        # Complex TXN_TYPES filter (for usage) - TXN_TP and MSG_TP combinations
        if "TXN_TYPES" in filters:
            txn_conditions = None
            for txn_type in filters["TXN_TYPES"]:
                condition = (F.col("TXN_TP") == txn_type["TXN_TP"]) & (F.col("MSG_TP") == txn_type["MSG_TP"])
                if txn_conditions is None:
                    txn_conditions = condition
                else:
                    txn_conditions = txn_conditions | condition
            df = df.filter(txn_conditions)
        
        # ISS_DT not null filter
        if filters.get("ISS_DT_NOT_NULL"):
            df = df.filter(F.col("ISS_DT").isNotNull())
        
        # AMT1 > 0 filter (for usage)
        if "AMT1_GT" in filters:
            df = df.filter(F.col("AMT1") > filters["AMT1_GT"])
        
        # Extract CLNT_NO from CLNT_CRD_NO (for usage table)
        if filters.get("EXTRACT_CLNT_NO"):
            df = df.withColumn(
                "CLNT_NO",
                F.regexp_replace(F.substring(F.col("CLNT_CRD_NO"), 7, 9), "^0+", "")
            ).withColumn(
                "POS_MODE",
                F.substring(F.col("POS_ENTRY_MODE_CD_NON_EMV"), 1, 2)
            )
    
    # Add Card_Type for activation/acquisition tables
    if config["success_type"] in ["ACQUISITION", "ACTIVATION"]:
        df = df.withColumn(
            "Card_Type",
            F.when(F.col("VISA_DR_CRD_BRND_CD") == "03", "Digital")
            .otherwise("Hybrid/Plastic")
        )
    
    return df


def load_success_table_edw(config):
    """Load token data from pre-pulled parquet (originally from EDW)."""
    return spark.read.parquet(config["success_table_path"])


def load_success_table(config):
    """Load appropriate success table based on source."""
    if config["success_source"] == "HIVE":
        return load_success_table_hive(config)
    elif config["success_source"] == "EDW":
        return load_success_table_edw(config)
    else:
        raise ValueError(f"Unknown success source: {config['success_source']}")


# =============================================================================
# SUCCESS DETECTION
# =============================================================================

def detect_success(tactic_df, success_df, config):
    """
    Join tactic with success table to detect successes.
    Returns standardized DataFrame with success flags.
    Uses TREATMT_END_DT - TREATMT_STRT_DT for measurement window.
    COHORT is always at MONTH level (yyyy-MM) per universal standard.
    """
    # Get all tactic columns
    tactic_columns = tactic_df.columns
    
    # Add measurement window from TREATMT_END_DT and test/control flag
    # COHORT = MONTH level (yyyy-MM) - UNIVERSAL STANDARD
    tactic_df = tactic_df.withColumn(
        "WINDOW_DAYS",
        F.datediff(F.col("TREATMT_END_DT"), F.col("TREATMT_STRT_DT"))
    ).withColumn(
        "GROUP",
        F.when(F.col("TST_GRP_CD") == "TG4", "TEST").otherwise("CONTROL")
    ).withColumn(
        "COHORT",
        F.date_format(F.col("TREATMT_STRT_DT"), "yyyy-MM")  # MONTH level aggregation
    )
    
    # Alias for join
    tactic_alias = tactic_df.alias("t")
    
    # Prepare success table
    success_date_field = config["success_date_field"]
    success_select = success_df.select(
        F.col("CLNT_NO").alias("SUCCESS_CLNT_NO"),
        F.col(success_date_field).alias("SUCCESS_DT")
    ).alias("s")
    
    # Left join - success within measurement window
    joined = tactic_alias.join(
        success_select,
        (F.col("t.CLNT_NO") == F.col("s.SUCCESS_CLNT_NO")) &
        (F.col("s.SUCCESS_DT") >= F.col("t.TREATMT_STRT_DT")) &
        (F.col("s.SUCCESS_DT") <= F.col("t.TREATMT_END_DT")),
        how="left"
    )
    
    # Calculate days to success
    joined = joined.withColumn(
        "DAYS_TO_SUCCESS",
        F.when(
            F.col("s.SUCCESS_DT").isNotNull(),
            F.datediff(F.col("s.SUCCESS_DT"), F.col("t.TREATMT_STRT_DT"))
        ).otherwise(None)
    )
    
    # Aggregate to client-deployment level (keeping COHORT at MONTH level)
    groupby_cols = [f"t.{col}" for col in tactic_columns] + ["WINDOW_DAYS", "GROUP", "COHORT"]
    
    result = joined.groupBy(groupby_cols).agg(
        F.max(F.when(F.col("s.SUCCESS_DT").isNotNull(), 1).otherwise(0)).alias("SUCCESS_FLAG"),
        F.min("s.SUCCESS_DT").alias("FIRST_SUCCESS_DT"),
        F.min("DAYS_TO_SUCCESS").alias("DAYS_TO_FIRST_SUCCESS"),
        F.count("s.SUCCESS_DT").alias("SUCCESS_COUNT")
    )
    
    return result


# =============================================================================
# VINTAGE CURVE CALCULATIONS
# =============================================================================

def build_vintage_data(success_df):
    """
    Build vintage curve data from success DataFrame.
    Returns DataFrame with cumulative rates by cohort (MONTH), group, and day.
    Aggregates multiple deployments within same month into single cohort.
    """
    # Total clients per cohort (MONTH) and group
    # Use median WINDOW_DAYS when multiple deployments exist in same month
    totals = success_df.groupBy("COHORT", "GROUP").agg(
        F.count("*").alias("TOTAL_CLIENTS"),
        F.expr("percentile_approx(WINDOW_DAYS, 0.5)").alias("WINDOW_DAYS")  # Median window
    )
    
    # Successes by cohort, group, and day
    successes = success_df.filter(
        F.col("SUCCESS_FLAG") == 1
    ).groupBy("COHORT", "GROUP", "DAYS_TO_FIRST_SUCCESS").agg(
        F.count("*").alias("SUCCESSES_ON_DAY")
    )
    
    # Join totals
    vintage = successes.join(totals, on=["COHORT", "GROUP"], how="left")
    
    return vintage.orderBy("COHORT", "GROUP", "DAYS_TO_FIRST_SUCCESS")


def calculate_confidence_interval(test_successes, test_n, ctrl_successes, ctrl_n, confidence=0.95):
    """
    Calculate confidence interval for absolute lift (difference in proportions).
    Uses normal approximation.
    """
    if test_n == 0 or ctrl_n == 0:
        return np.nan, np.nan, np.nan
    
    p_test = test_successes / test_n
    p_ctrl = ctrl_successes / ctrl_n
    
    lift = p_test - p_ctrl
    
    # Standard error of difference
    se = np.sqrt((p_test * (1 - p_test) / test_n) + (p_ctrl * (1 - p_ctrl) / ctrl_n))
    
    # Z-score for confidence level
    z = stats.norm.ppf(1 - (1 - confidence) / 2)
    
    ci_lower = lift - z * se
    ci_upper = lift + z * se
    
    return lift, ci_lower, ci_upper


def prepare_vintage_table(vintage_spark_df):
    """
    Convert to Pandas, calculate cumulative rates, lift, and CI.
    Uses actual WINDOW_DAYS from data (not hardcoded).
    """
    pdf = vintage_spark_df.toPandas()
    
    if pdf.empty:
        return pdf
    
    # Sort
    pdf = pdf.sort_values(["COHORT", "GROUP", "DAYS_TO_FIRST_SUCCESS"])
    
    # Cumulative successes per cohort-group
    pdf["CUMULATIVE_SUCCESSES"] = pdf.groupby(["COHORT", "GROUP"])["SUCCESSES_ON_DAY"].cumsum()
    pdf["CUMULATIVE_RATE"] = pdf["CUMULATIVE_SUCCESSES"] / pdf["TOTAL_CLIENTS"] * 100
    
    # Rename for clarity
    pdf = pdf.rename(columns={"DAYS_TO_FIRST_SUCCESS": "DAY"})
    
    # Fill in missing days for complete curves
    cohorts = pdf["COHORT"].unique()
    groups = ["TEST", "CONTROL"]
    
    # Create complete grid
    complete_rows = []
    for cohort in cohorts:
        for group in groups:
            cohort_group_data = pdf[(pdf["COHORT"] == cohort) & (pdf["GROUP"] == group)]
            if cohort_group_data.empty:
                continue
            
            total_clients = cohort_group_data["TOTAL_CLIENTS"].iloc[0]
            window_days = int(cohort_group_data["WINDOW_DAYS"].iloc[0])
            max_day_with_data = cohort_group_data["DAY"].max()
            
            cum_successes = 0
            for day in range(0, window_days + 1):
                if day > max_day_with_data:
                    break  # Don't extrapolate beyond available data
                
                day_data = cohort_group_data[cohort_group_data["DAY"] == day]
                if not day_data.empty:
                    cum_successes = day_data["CUMULATIVE_SUCCESSES"].iloc[0]
                
                complete_rows.append({
                    "COHORT": cohort,
                    "GROUP": group,
                    "DAY": day,
                    "WINDOW_DAYS": window_days,
                    "TOTAL_CLIENTS": total_clients,
                    "CUMULATIVE_SUCCESSES": cum_successes,
                    "CUMULATIVE_RATE": cum_successes / total_clients * 100 if total_clients > 0 else 0
                })
    
    complete_df = pd.DataFrame(complete_rows)
    
    # Calculate lift and CI for each cohort-day
    lift_rows = []
    for cohort in cohorts:
        cohort_data = complete_df[complete_df["COHORT"] == cohort]
        test_data = cohort_data[cohort_data["GROUP"] == "TEST"]
        ctrl_data = cohort_data[cohort_data["GROUP"] == "CONTROL"]
        
        if test_data.empty or ctrl_data.empty:
            continue
        
        window_days = int(test_data["WINDOW_DAYS"].iloc[0])
        
        for day in test_data["DAY"].unique():
            test_row = test_data[test_data["DAY"] == day]
            ctrl_row = ctrl_data[ctrl_data["DAY"] == day]
            
            if test_row.empty or ctrl_row.empty:
                continue
            
            test_successes = test_row["CUMULATIVE_SUCCESSES"].iloc[0]
            test_n = test_row["TOTAL_CLIENTS"].iloc[0]
            ctrl_successes = ctrl_row["CUMULATIVE_SUCCESSES"].iloc[0]
            ctrl_n = ctrl_row["TOTAL_CLIENTS"].iloc[0]
            
            lift, ci_lower, ci_upper = calculate_confidence_interval(
                test_successes, test_n, ctrl_successes, ctrl_n
            )
            
            lift_rows.append({
                "COHORT": cohort,
                "DAY": day,
                "WINDOW_DAYS": window_days,
                "TEST_CLIENTS": test_n,
                "TEST_SUCCESSES": test_successes,
                "TEST_RATE": test_row["CUMULATIVE_RATE"].iloc[0],
                "CTRL_CLIENTS": ctrl_n,
                "CTRL_SUCCESSES": ctrl_successes,
                "CTRL_RATE": ctrl_row["CUMULATIVE_RATE"].iloc[0],
                "ABS_LIFT": lift * 100,  # Convert to percentage points
                "CI_LOWER": ci_lower * 100,
                "CI_UPPER": ci_upper * 100
            })
    
    lift_df = pd.DataFrame(lift_rows)
    
    # Add significance flag (CI doesn't cross zero)
    if not lift_df.empty:
        lift_df["SIGNIFICANT"] = (lift_df["CI_LOWER"] > 0) | (lift_df["CI_UPPER"] < 0)
    
    return lift_df


# =============================================================================
# PLOTTING FUNCTIONS (FIXED FOR OLDER MATPLOTLIB)
# =============================================================================

def plot_campaign_vintage(vintage_df, mne, config, save_path):
    """
    Plot vintage curves for ALL cohorts on ONE chart.
    Each cohort has two lines: solid = Test, dashed = Control (same color)
    """
    if vintage_df.empty:
        print(f"    WARNING: No data to plot for {mne}")
        return
    
    # FIX: Use reasonable figure size
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Get unique cohorts and assign colors
    cohorts = sorted(vintage_df["COHORT"].unique())
    colors = plt.cm.tab10(np.linspace(0, 1, min(len(cohorts), 10)))
    
    # If more than 10 cohorts, cycle colors
    if len(cohorts) > 10:
        colors = plt.cm.tab20(np.linspace(0, 1, min(len(cohorts), 20)))
    
    # Plot each cohort - Test (solid) and Control (dashed) same color
    for i, cohort in enumerate(cohorts):
        cohort_data = vintage_df[vintage_df["COHORT"] == cohort]
        if cohort_data.empty:
            continue
        
        color = colors[i % len(colors)]
        test_n = int(cohort_data["TEST_CLIENTS"].iloc[0])
        ctrl_n = int(cohort_data["CTRL_CLIENTS"].iloc[0])
        
        # Test line (solid)
        ax.plot(
            cohort_data["DAY"],
            cohort_data["TEST_RATE"],
            linestyle='-',
            marker='o',
            linewidth=1.5,
            markersize=3,
            color=color,
            label=f'{cohort} Test (n={test_n:,})',
            alpha=0.9
        )
        
        # Control line (dashed, same color)
        ax.plot(
            cohort_data["DAY"],
            cohort_data["CTRL_RATE"],
            linestyle='--',
            marker='s',
            linewidth=1.5,
            markersize=3,
            color=color,
            label=f'{cohort} Ctrl (n={ctrl_n:,})',
            alpha=0.7
        )
    
    ax.set_xlabel("Days from Treatment", fontsize=12)
    ax.set_ylabel("Cumulative Conversion Rate (%)", fontsize=12)
    ax.set_title(f"{mne} - {config['description']}\nVintage Curves: Test (solid) vs Control (dashed) | {config['success_type']}", fontsize=13)
    ax.legend(title="Cohort / Group", bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=8)
    ax.grid(True, alpha=0.3)
    ax.set_xlim(0, None)
    ax.set_ylim(0, None)
    
    plt.tight_layout()
    plt.savefig(save_path, dpi=100, bbox_inches='tight')
    plt.close()
    
    print(f"    Plot saved: {save_path}")


def plot_test_vs_control_by_cohort(vintage_df, mne, config, save_path):
    """
    Alternative view: One subplot per cohort showing Test vs Control.
    Useful when you have fewer cohorts and want to compare directly.
    """
    cohorts = sorted(vintage_df["COHORT"].unique())
    n_cohorts = len(cohorts)
    
    if n_cohorts == 0:
        print(f"    WARNING: No cohorts to plot for {mne}")
        return
    
    # Calculate grid dimensions
    n_cols = min(3, n_cohorts)
    n_rows = (n_cohorts + n_cols - 1) // n_cols
    
    # FIX: Limit figure size
    fig_width = min(5 * n_cols, 15)
    fig_height = min(4 * n_rows, 12)
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(fig_width, fig_height), squeeze=False)
    
    for idx, cohort in enumerate(cohorts):
        row = idx // n_cols
        col = idx % n_cols
        ax = axes[row, col]
        
        cohort_data = vintage_df[vintage_df["COHORT"] == cohort]
        if cohort_data.empty:
            continue
        
        window_days = int(cohort_data["WINDOW_DAYS"].iloc[0])
        final_row = cohort_data[cohort_data["DAY"] == cohort_data["DAY"].max()].iloc[0]
        
        # Plot Test and Control
        ax.plot(cohort_data["DAY"], cohort_data["TEST_RATE"], 
                marker='o', linewidth=1.5, markersize=3, color='#2E86AB',
                label=f'Test (n={int(final_row["TEST_CLIENTS"]):,})')
        ax.plot(cohort_data["DAY"], cohort_data["CTRL_RATE"], 
                marker='s', linewidth=1.5, markersize=3, color='#A23B72',
                label=f'Control (n={int(final_row["CTRL_CLIENTS"]):,})')
        
        # Add lift annotation
        ax.annotate(
            f'Lift: {final_row["ABS_LIFT"]:.2f}pp\n[{final_row["CI_LOWER"]:.2f}, {final_row["CI_UPPER"]:.2f}]',
            xy=(0.95, 0.05), xycoords='axes fraction',
            fontsize=8, ha='right', va='bottom',
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.8)
        )
        
        ax.set_title(f'Cohort: {cohort}', fontsize=10)
        ax.set_xlabel("Days", fontsize=9)
        ax.set_ylabel("Rate (%)", fontsize=9)
        ax.legend(fontsize=7, loc='upper left')
        ax.grid(True, alpha=0.3)
        ax.set_xlim(0, window_days)
        ax.set_ylim(0, None)
    
    # Hide empty subplots
    for idx in range(n_cohorts, n_rows * n_cols):
        row = idx // n_cols
        col = idx % n_cols
        axes[row, col].set_visible(False)
    
    fig.suptitle(f"{mne} - {config['description']} | {config['success_type']}", fontsize=13, y=1.02)
    plt.tight_layout()
    plt.savefig(save_path, dpi=100, bbox_inches='tight')
    plt.close()
    
    print(f"    Plot saved: {save_path}")


def generate_summary_table(lift_df, mne):
    """
    Generate summary table with final rates per cohort.
    """
    if lift_df.empty:
        return pd.DataFrame()
    
    # Get max day per cohort (final measurement)
    final_rates = lift_df.loc[lift_df.groupby("COHORT")["DAY"].idxmax()].copy()
    
    final_rates["MNE"] = mne
    
    final_rates = final_rates[[
        "MNE", "COHORT", "WINDOW_DAYS", "TEST_CLIENTS", "TEST_SUCCESSES", "TEST_RATE",
        "CTRL_CLIENTS", "CTRL_SUCCESSES", "CTRL_RATE",
        "ABS_LIFT", "CI_LOWER", "CI_UPPER", "SIGNIFICANT"
    ]].sort_values("COHORT")
    
    return final_rates


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def run_vintage_analysis(mne):
    """
    Run full vintage analysis for a campaign.
    """
    print(f"\n{'='*60}")
    print(f"Running Vintage Analysis for {mne}")
    print(f"{'='*60}")
    
    config = CAMPAIGN_CONFIG[mne]
    print(f"Campaign: {config['description']}")
    print(f"Success Type: {config['success_type']}")
    print(f"Source: {config['success_source']}")
    
    # Step 1: Load data
    print("\n[1] Loading tactic data...")
    tactic_df = load_tactic(mne)
    tactic_count = tactic_df.count()
    print(f"    Tactic records: {tactic_count:,}")
    
    if tactic_count == 0:
        print(f"    WARNING: No tactic records found for {mne}. Skipping.")
        return None, None
    
    print("\n[2] Loading success table...")
    success_table = load_success_table(config)
    
    # Step 2: Detect success
    print("\n[3] Detecting success...")
    success_df = detect_success(tactic_df, success_table, config)
    success_df.persist(StorageLevel.MEMORY_AND_DISK)
    
    # Summary
    print("\n[4] Success summary by group:")
    success_df.groupBy("GROUP").agg(
        F.count("*").alias("TOTAL"),
        F.sum("SUCCESS_FLAG").alias("SUCCESSES"),
        F.avg("SUCCESS_FLAG").alias("SUCCESS_RATE")
    ).show()
    
    # Step 3: Build vintage data
    print("\n[5] Building vintage curves...")
    vintage_spark = build_vintage_data(success_df)
    
    # Step 4: Calculate lift and CI
    print("\n[6] Calculating lift and confidence intervals...")
    vintage_table = prepare_vintage_table(vintage_spark)
    
    if vintage_table.empty:
        print("    WARNING: No vintage data generated!")
        return None, None
    
    # Step 5: Generate plots - ALL cohorts on one chart per campaign
    print("\n[7] Generating plots...")
    cohorts = sorted(vintage_table["COHORT"].unique())
    print(f"    Found {len(cohorts)} cohorts")
    
    # Plot 1: All cohorts - Test curves on top, Control curves on bottom
    save_path_1 = f"/user/427966379/{mne}_vintage_all_cohorts.png"
    plot_campaign_vintage(vintage_table, mne, config, save_path_1)
    
    # Plot 2: Grid view - one subplot per cohort with Test vs Control
    if len(cohorts) <= 12:  # Only if not too many cohorts
        save_path_2 = f"/user/427966379/{mne}_vintage_grid.png"
        plot_test_vs_control_by_cohort(vintage_table, mne, config, save_path_2)
    
    # Step 6: Generate summary table
    print("\n[8] Summary Table (Final Rates by Cohort):")
    summary_table = generate_summary_table(vintage_table, mne)
    print(summary_table.to_string(index=False))
    
    # Step 7: Save outputs
    print("\n[9] Saving outputs...")
    vintage_table["MNE"] = mne
    vintage_table.to_csv(f"/user/427966379/{mne}_vintage_full.csv", index=False)
    summary_table.to_csv(f"/user/427966379/{mne}_vintage_summary.csv", index=False)
    print(f"    Full table: /user/427966379/{mne}_vintage_full.csv")
    print(f"    Summary: /user/427966379/{mne}_vintage_summary.csv")
    
    # Unpersist
    success_df.unpersist()
    
    return vintage_table, summary_table


# =============================================================================
# RUN ALL CAMPAIGNS
# =============================================================================

if __name__ == "__main__":
    
    # Store all results
    all_vintage_tables = []
    all_summary_tables = []
    
    # Run for each campaign
    for mne in ["VCN", "VDA", "VDT", "VUI", "VUT", "VAW"]:
        try:
            vintage_table, summary_table = run_vintage_analysis(mne)
            if vintage_table is not None:
                all_vintage_tables.append(vintage_table)
                all_summary_tables.append(summary_table)
        except Exception as e:
            print(f"\n*** ERROR processing {mne}: {str(e)} ***\n")
            continue
    
    # Combine all results
    if all_vintage_tables:
        print("\n" + "="*60)
        print("COMBINING ALL CAMPAIGN RESULTS")
        print("="*60)
        
        combined_vintage = pd.concat(all_vintage_tables, ignore_index=True)
        combined_summary = pd.concat(all_summary_tables, ignore_index=True)
        
        combined_vintage.to_csv("/user/427966379/ALL_CAMPAIGNS_vintage_full.csv", index=False)
        combined_summary.to_csv("/user/427966379/ALL_CAMPAIGNS_vintage_summary.csv", index=False)
        
        print(f"\nCombined full table: /user/427966379/ALL_CAMPAIGNS_vintage_full.csv")
        print(f"Combined summary: /user/427966379/ALL_CAMPAIGNS_vintage_summary.csv")
        
        print("\n" + "="*60)
        print("FINAL SUMMARY - ALL CAMPAIGNS")
        print("="*60)
        print(combined_summary.to_string(index=False))
    
    print("\n" + "="*60)
    print("ANALYSIS COMPLETE")
    print("="*60)
