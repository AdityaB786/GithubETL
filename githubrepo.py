import requests
import pandas as pd

# Step 1: Extract
def fetch_github_repos(username):
    print(f"Fetching repos for user: {username}")
    url = f"https://api.github.com/users/{username}/repos"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

# Step 2: Transform
def transform_data(repos):
    df = pd.json_normalize(repos)
    df = df[["name", "html_url", "stargazers_count", "forks_count", "created_at"]]
    df["created_at"] = pd.to_datetime(df["created_at"])  # parse to datetime
    now = pd.Timestamp.now(tz="UTC")  # make 'now' timezone-aware
    df["stars_per_day"] = (
        df["stargazers_count"] / (now - df["created_at"]).dt.days.clip(lower=1)
    ).round(2)  # round to 2 decimals
    return df.sort_values(by="stargazers_count", ascending=False)

# Step 3: Load
def save_to_csv(df, filename="github_repos.csv"):
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")

# Run the ETL
def run_etl(username):
    repos = fetch_github_repos(username)
    df = transform_data(repos)
    print(df[["name", "stargazers_count", "stars_per_day"]])
    save_to_csv(df)

# Example: Replace 'octocat' with your GitHub username
run_etl("AdityaB786")
