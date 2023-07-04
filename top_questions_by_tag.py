import requests
import os

from datetime import datetime, timedelta



def fetch_top_questions(tag):
    # Set up the API endpoint URL
    api_url = "https://api.stackexchange.com/2.3/questions/"

    # Calculate the date range for the last 30 days
    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    # Set the query parameters
    params = {
        "order": "desc",
        "sort": "votes",
        "site": "stackoverflow",
        "fromdate": from_date,
        "todate": to_date,
        "pagesize":10,
        "tagged":tag
    }

    try:
        # Send a GET request to the API endpoint
        response = requests.get(api_url, params=params)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Extract the JSON data from the response
            data = response.json()

            # Extract the top questions of alltime popular tags in the last 30 days from the response data
            top_questions = [q for q in data["items"]]
            print(type(top_questions))
            return top_questions
        else:
            print("Error: Request failed with status code", response.status_code)

    except requests.exceptions.RequestException as e:
        print("Error:", e)

# Call the function to fetch and display the top questions asked on all time popular tags
fetch_top_questions("python")