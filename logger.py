import asyncio
import datetime
from utility import make_path


class Logger:
    def __init__(self, path, name):
        self.folder = 'logs'
        self.path = self.folder + '/' + path
        self.name = name
        self.time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    def get_time(self):
        self.time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    def get_date(self):
        self.date = datetime.datetime.now().strftime("%Y-%m-%d")

    async def info(self, message):
        self.get_time()
        with open(self.path + '/' + self.name + '_' + self.date + '.log', 'a') as f:
            f.write(self.time + ', INFO: ' + message + '\n')

    async def error(self, message):
        self.get_time()
        with open(self.path + '/' + self.name + '_' + self.date + '.log', 'a') as f:
            f.write(self.time + ', ERROR: ' + message + '\n')
