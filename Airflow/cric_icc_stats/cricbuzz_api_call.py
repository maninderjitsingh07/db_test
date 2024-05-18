import requests
import csv

url = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen"
querystring = {"formatType":"test"}
headers = {
	"X-RapidAPI-Key": "8dfee58215msh8fcfd92cc1a48f2p1e5c92jsn6169bab1275c",
	"X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

if response.status_code == 200:
    data = response.json().get('rank', [])  # Extracting the 'rank' data
    csv_filename = 'batsmen_rankings.csv'

    if data:
    #header
        field_names = ['rank', 'name', 'country','rating','points','lastUpdatedOn']  
        # Write data to CSV file with only specified field names
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            for entry in data:
                writer.writerow({field: entry.get(field) for field in field_names})

        print(f"Data extraction is successfully completed to '{csv_filename}'")
    else:
        print("No data available.")

else:
    print("Failed to fetch data:", response.status_code)
        
