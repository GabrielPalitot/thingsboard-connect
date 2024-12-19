import logging
from tb_rest_client.rest_client_ce import *
from tb_rest_client.rest import ApiException
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# ThingsBoard REST API URL
url = "https://thingsboard.cloud"

USERNAME = os.getenv('USERNAME_THINGSBOARD')
PASSWORD = os.getenv('PASSWORD_THINGSBOARD')
ENTITY_TYPE = os.getenv('ENTITY_TYPE')
ID = os.getenv('ID')
keys = os.getenv('KEYS')
timestamps =[1734546340624,1734719140624]

def main():
    with RestClientCE(base_url=url) as rest_client:
        try:
            rest_client.login(username=USERNAME, password=PASSWORD)
            entity = EntityId(id=ID, entity_type=ENTITY_TYPE)
            data = rest_client.get_timeseries(entity, keys, timestamps[0], timestamps[1], limit=500)
            records = []
            for key, values in data.items():
                for entry in values:
                    records.append({
                        'timestamp': pd.to_datetime(entry['ts'], unit='ms'),
                        key: entry['value']
                    })
            
            df = pd.DataFrame(records)

            df = df.groupby('timestamp').first().reset_index()

            df['timestamp'] = df['timestamp'].dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo')
            df['umidade_categoria'] = df['umidade_solo'].apply(lambda x: 'solo molhado' if int(x) < 3000 else 'solo seco')
            df['detector_categoria'] = df['detector_chuva'].apply(lambda x: 'umido' if int(x) < 3000 else 'seco')
            df.to_csv('data.csv', index=False)

        except ApiException as e:
            logging.exception(e)


if __name__ == '__main__':
    main()