import requests
import pandas as pd
import json
import redis
import yaml
import matplotlib.pyplot as plt


class BookDataProcessor:
    def __init__(self, isbn_numbers, api_key, redis_host, redis_port, redis_password):
        """
        These Args info are retrived from the Yaml file
        Args:
        :param isbn_numbers: Lists of International Standard Book Numbers
        :param api_key: Private API key used for accessing the pubilc google books API
        :param redis_host: Redis host
        :param redis_port: Redis Port number
        :param redis_password: Redis password
        """
        self.data = None
        self.isbn_numbers = isbn_numbers
        self.api_key = api_key
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_password = redis_password
        self.books_data = []

    def fetch_books_data_from_api(self):
        """
        This function will access the Google Book API. If the response is 200,
        it downloads the data from the API else, it prints the exception
        """
        for isbn in self.isbn_numbers:
            try:
                url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}&key={self.api_key}"
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
                self.data = data
            except requests.exceptions.HTTPError as e:
                print(f"Failed to fetch data from the API for ISBN {isbn}. Error: {e}")

    def store_data_in_redis(self, json_str):
        """
        This function will build the connection with redis database and inserts the data into the redis database
        :param json_str: Json formatted data that is used to insert into database
        """
        try:
            creds_provider = redis.ConnectionPool(
                host=self.redis_host,
                port=self.redis_port,
                password=self.redis_password
            )

            redis_connection = redis.Redis(connection_pool=creds_provider)
            redis_connection.json().set('booksdata:title:info', '.', json_str)
            print("Data stored in Redis Completed.")
            self.retrieve_and_print_data_from_redis_datastore(redis_connection)

        except Exception as e:
            print(f"Error in storing data in Redis: {e}")

    def data_transformations_using_pandas(self):
        """
        This function will Retrives the relevent and necessary information from the Json Data that is downloaded from the
        API

        VolumeInfo: It contains the info related to the Title, Author,
        """
        data = self.data
        if "items" in data and data["items"]:
            self.books_data.extend(data["items"])
        else:
            print("No data found for ISBN")

        if self.books_data:
            book_info_list = []

            for book_data in self.books_data:
                if "volumeInfo" in book_data:
                    book_info_list.append(book_data["volumeInfo"])
                else:
                    print("VolumeInfo not found for a book.")

            df = pd.DataFrame(book_info_list)
            df2 = df.fillna("")
            result_json = {"items": df2.to_dict(orient="records")}
            json_str = json.dumps(result_json, indent=4)
            print("Insert Operation1")
            self.store_data_in_redis(json_str)
        else:
            print("No data is available to fetch, Please check the API creds")

    def retrieve_and_print_data_from_redis_datastore(self, redis_connection):
        """
        This function builds the connection with redis and gets the data from it
        :param redis_connection: This contains the redis connection info
        :return: Gets the info back from the redis database and prints them
        """
        try:
            json_data = redis_connection.json().get('booksdata:title:info')
            data = json.loads(json_data)
            print(data)
            title = data['items'][0]['title']
            authors = data['items'][0]['authors']
            average_rating = data['items'][0]['averageRating']
            ratings_count = data['items'][0]['ratingsCount']
            print("Search Operation2")
            print(f"Title: {title}")
            print(f"Authors: {', '.join(authors)}")
            print(f"Average Rating: {average_rating}")
            print(f"Ratings Count: {ratings_count}")


        except Exception as e:
            print(f"Error in retrieving data from Redis: {e}")

if __name__ == "__main__":
    with open("config.yaml", "r") as file:
        config = yaml.safe_load(file)
    isbn_numbers = config['ISBN']['isbn_numbers']
    API_KEY = config['API KEY']['API_KEY']
    REDIS_HOST = config['REDIS CRED']['REDIS_HOST']
    REDIS_PORT = config['REDIS CRED']['REDIS_PORT']
    REDIS_PASSWORD = config['REDIS CRED']['REDIS_PASSWORD']

    book_processor = BookDataProcessor(isbn_numbers, API_KEY, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)
    book_processor.fetch_books_data_from_api()
    book_processor.data_transformations_using_pandas()