import requests
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def fetch_data(endpoint: str, page_limit: int = 500, sleep_time: float = 0.5 ) -> dict:
    
    url = f'https://api.spaceflightnewsapi.net/v4/{endpoint}'
    all_data = []
    page = 1
    
    params = {
        "limit" : page_limit
    }

    while True:
        try :
            response = requests.get(url, params)
            print
            response.raise_for_status()
            
            data = response.json()
            all_data.extend(data['results'])
            
            logging.info(f"Page {page} sucessully requested.")
            
            if not data['next']:
                logging.info("Finish pagination")
                break
            
            url = data["next"]
        
        except Exception as e:
            logging.error(f"Response Status: {response.status_code}")
            logging.error(f"Response Reason: {response.reason}")
            logging.error(f"Error: {e}")
            break
    
        page += 1
    
    return all_data