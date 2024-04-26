import pandas as pd

data_path = 'C:/Users/Katie/Downloads/Data Engineering Activities/Week 4/Hwy26Crashes2019_S23.csv'
crashes = pd.read_csv(data_path)
print(crashes.head())

def check_assertion(description, condition):
    try:
        assert condition, description
        print(f"Passed: {description}")
    except AssertionError as error:
        print(f"Failed: {error}")

def run_assertions(df):
    # Existence Assertions
    check_assertion("Every record has a Crash ID", df['Crash ID'].notna().all())
    check_assertion("Every record has a Crash Day", df['Crash Day'].notna().all())

    # Limit Assertions
    df['Crash Day'] = pd.to_datetime(df['Crash Day'], errors='coerce')
    check_assertion("All crashes occurred during the year 2019", (df['Crash Day'].dt.year == 2019).all())
    check_assertion("Crash Hour within valid range (0-23)", df['Crash Hour'].dropna().between(0, 23).all())

    # Intra-record Assertions
    latitude_ok = df.dropna(subset=['Latitude Degrees']).dropna(subset=['Longitude Degrees']).shape[0] == df.dropna(subset=['Latitude Degrees']).shape[0]
    check_assertion("Every Latitude has corresponding Longitude", latitude_ok)

    # Inter-record Check Assertions
    vehicle_ids_valid = df['Vehicle ID'].isin(df['Crash ID']).all()
    check_assertion("Every vehicle listed was part of a known crash", vehicle_ids_valid)


    # Summary Assertions
    total_crashes = len(df)
    check_assertion("There were thousands of crashes but not millions", 1000 <= total_crashes <= 999999)

    alcohol_involved_check = df[df['Alcohol-Involved Flag'] == 'Yes'].shape[0] <= total_crashes
    check_assertion("The number of crashes involving alcohol does not exceed total crashes", alcohol_involved_check)

    # Statistical Distribution Assertions
    monthly_crashes = df['Crash Day'].dt.month.value_counts()
    even_distribution = monthly_crashes.std() / monthly_crashes.mean() < 0.1
    check_assertion("Crashes are evenly distributed throughout the months of the year", even_distribution)



run_assertions(crashes)
