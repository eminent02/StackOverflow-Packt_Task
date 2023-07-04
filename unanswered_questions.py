import requests
import os

from datetime import datetime, timedelta



def fetch_unanswered_questions():
    # Set up the API endpoint URL
    api_url = "https://api.stackexchange.com/2.3/questions/unanswered"

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
        "pagesize":10
    }

    try:
        # Send a GET request to the API endpoint
        response = requests.get(api_url, params=params)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Extract the JSON data from the response
            data = response.json()

            # Extract the top 10 unanswered questions of past month from the response data
            unanswered_questions = [q for q in data["items"]]
            print(unanswered_questions)
            return unanswered_questions
        else:
            print("Error: Request failed with status code", response.status_code)

    except requests.exceptions.RequestException as e:
        print("Error:", e)

# Call the function to fetch and display the unanswered questions
fetch_unanswered_questions()