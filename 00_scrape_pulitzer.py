import json
import pandas as pd
import requests
import os
import time

def main():
    with open('vocab.json', 'r') as f:
        data = json.load(f)

    rows = [
        {
            "tid": v.get("tid", ""),
            "name": v.get("name", ""),
            "v_name": v.get("v_name", ""),
            "active_year_note": v.get("fields", {}).get("active_year_note", ""),
            "property": v.get("fields", {}).get("property", "")
        }
        for v in data if "tid" in v and "name" in v and "v_name" in v
    ]

    df = pd.DataFrame(rows)
    if not os.path.exists('csv'):
        os.makedirs('csv')
    df.to_csv('csv/vocab.csv', index=False)

    df_years = df[df['v_name'].str.contains("Years", na=False)]
    df_years = df_years.drop(columns=['active_year_note', 'property'])
    df_years.to_csv('csv/years.csv', index=False)

    df_awards = df[df['v_name'].str.contains("Award Category", na=False)]
    df_awards = df_awards.sort_values(by=['property', 'tid'], ascending=[False, True])
    df_awards.to_csv('csv/awards.csv', index=False)

    if not os.path.exists('json/finalists_by_year'):
        os.makedirs('json/finalists_by_year')

    if not os.path.exists('json/winners_by_year'):
        os.makedirs('json/winners_by_year')


    #df_years = df_years.iloc[0:10]
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.pulitzer.org/prize-winners-by-year/2023",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "Pragma": "no-cache",
        "Cache-Control": "no-cache"
    }
    for _, row in df_years.iterrows():
        tid = row['tid']
        name = row['name']
        print(f"Downloading Winners {name} - {tid}")
        url = f"https://www.pulitzer.org/cache/api/1/winners/year/{tid}/raw.json"
        response = requests.get(url, headers=headers)
        print(f"Response code: {response.status_code}")
        if response.status_code == 200:
            filename = f'json/winners_by_year/winners_year_{name}_tid_{tid}.json'
            with open(filename, 'w') as f:
                f.write(response.text)
        else:
            print(f"Failed to download winners {url}")

        print(f"Downloading Finalists {name} - {tid}")
        url = f"https://www.pulitzer.org/cache/api/1/finalist/all/{tid}/raw.json"
        response = requests.get(url, headers=headers)
        print(f"Response code: {response.status_code}")
        if response.status_code == 200:
            filename = f'json/finalists_by_year/finalists_year_{name}_tid_{tid}.json'
            with open(filename, 'w') as f:
                f.write(response.text)
        else:
            print(f"Failed to download winners {url}")
    
        # add a delay to avoid overwhelming the server
        time.sleep(1)
 
        

if __name__ == "__main__":
    main()