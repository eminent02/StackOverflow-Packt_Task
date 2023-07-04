import requests
import os

from datetime import datetime, timedelta,date



def fetch_trending_tags():
    # Set up the API endpoint URL
    api_url = "https://api.stackexchange.com/2.3/tags"

    # Calculate the date range for the last 30 days
    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    # Set the query parameters
    params = {
        "order": "desc",
        "sort": "popular",
        "site": "stackoverflow",
        "fromdate": from_date,
        "todate": to_date
    }

    try:
        # Send a GET request to the API endpoint
        response = requests.get(api_url, params=params)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Extract the JSON data from the response
            data = response.json()

            # Extract the trending tags from the response data
            trending_tags = [tag for tag in data["items"]]
            tag_values = [(tag["name"], tag["count"], (date.today().replace(day=1) - timedelta(days=1)).strftime("%Y-%m")) for tag in trending_tags]
            print(tag_values)
            return trending_tags
        else:
            print("Error: Request failed with status code", response.status_code)

    except requests.exceptions.RequestException as e:
        print("Error:", e)

# Call the function to fetch and display the trending tags
fetch_trending_tags()