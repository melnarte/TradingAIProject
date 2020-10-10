

class Collector():

    def __init__(self, data_path : str, api_token='pk_d4de3407f29a4f7897c3669c42f11a61'):
        self.data_path = data_path
        self.intraday_api_url = 'https://cloud.iexapis.com/stable/stock/'
        self.api_token = api_token
