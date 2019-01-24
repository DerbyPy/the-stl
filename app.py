import os
import concurrent.futures
import urllib.request
import urllib.parse
import csv
import json
from queue import Queue
from pprint import pprint
from functools import singledispatch
import pdb

csv_file = 'games-features.csv'
gb_key = os.getenv("giant_bomb_key")
content_type = "application/json"
user_agent = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:64.0) Gecko/20100101 Firefox/64.0'

@singledispatch
def display(publishers, game):
    pass

@display.register(str)
def _(publishers: str, game: str):
    pprint(f"{game}: {publishers}")

@display.register(list)
def _(publishers: list, game: str):
    pprint(f"{game}: {' & '.join(publishers)}")

def load_url(url, timeout):
    with urllib.request.urlopen(url, timeout=timeout) as conn:
        return conn.read()

def fetch_results(urls, result_queue, exception_queue):
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:

        future_to_url = {executor.submit(load_url, url, 60): url for url in
                         urls}

        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
            except Exception as exc:
                pprint(exc)
                exception_queue.put(exc)
            else:
                print(f"{url} page is {len(data)} bytes")
                result_queue.put(json.loads(data), timeout=1)

def load_game_data(csv_file):
    with open(csv_file) as csv_file:
        reader = csv.DictReader(csv_file)
        return [row for row in reader]

def process_id_results(result_queue, processed_results=[]):
    while not result_queue.empty():
        result = result_queue.get(timeout=1)
        id_list = result['results']

        for row in id_list:
            processed_results.append(row['id'])

        return processed_results

def process_publisher_results(result_queue, processed_results=[]):
    while not result_queue.empty():
        result = result_queue.get(timeout=1)
        game = result['results']['name']
        publishers = [ row['name'] for row in result['results']['publishers']]

        processed_results.append({game: publishers})

    return processed_results

if __name__ == "__main__":
    game_data = load_game_data('games-features.csv')

    games = [[row['QueryName'], row['Metacritic']] for row in game_data]
    games = games[7:10]
    game_titles = [game[0] for game in games]
    game_urls = [game.replace(' ', '%20').lower() for game in game_titles]
    game_urls = [f"https://www.giantbomb.com/api/search/?api_key={gb_key}&query={game}&field_list=name,id&format=json" for game in  game_urls]

    results = Queue()
    excs = Queue()

    fetch_results(game_urls, results, excs)
    game_ids = process_id_results(results)

    pub_urls = [f"https://www.giantbomb.com/api/game/{_id}/?api_key={gb_key}&field_list=name,publishers&format=json" for _id in game_ids]

    results = Queue()
    fetch_results(pub_urls, results, excs)
    game_publishers = process_publisher_results(results)

    pprint(game_publishers)
    pprint(games)

    for publisher in game_publishers:
        for game, pub in publisher.items():
            if len(pub) == 1:
                display(str(pub[0]), game)
            else:
                display(pub, game)
