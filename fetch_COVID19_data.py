import time
import schedule
import pandas as pd
timestr = time.strftime("%Y%m%d-%H%M%S")

confirmed_cases_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
deaths_cases_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"
recovered_cases_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv"


def get_n_melt_data(data_url,case_type):
    df = pd.read_csv(data_url)
    melted_df = df.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'])
    melted_df.rename(columns={"variable":"Date", "value":case_type}, inplace=True)
    return melted_df

def merge_data(confirm_df, recovered_df, death_df):
    new_df = confirm_df.join(recovered_df["Recovered"]).join(death_df["Deaths"])
    return new_df

def fetch_data():
    """Fetch and Prepare data"""
    confirm_df = get_n_melt_data(confirmed_cases_url, "Confirmed")
    recovered_df = get_n_melt_data(recovered_cases_url, "Recovered")
    death_df = get_n_melt_data(deaths_cases_url, "Deaths")
    print("Merging Dataset")

    final_df = merge_data(confirm_df, recovered_df, death_df)
    print("Preview Data")
    print(final_df.tail(5))
    filename = "COVID19_merged_dataset_updated_{}.csv".format(timestr)
    print("Saving Dataset{}".format(filename, index=False))
    final_df.to_csv(filename)
    print("Finished")

# Task
schedule.every(5).seconds.do(fetch_data)

while True:
    schedule.run_pending()
    time.sleep(1)

