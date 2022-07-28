import asyncio

class OrderBook:
    async def __init__(self, keywords, logger=None):
        self.keywords = keywords
        self.orderbook = {}
        self.logger = logger
        await self.clear()
        
    async def clear(self):
        self.orderbook = {key: -1 for key in self.keywords}
        if self.logger:
            self.logger.info("Orderbook cleared")
    
    async def update(self, key, value):
        if key in self.orderbook:
            self.orderbook[key] = value
            if self.logger:
                self.logger.info("Orderbook updated: {} = {}".format(key, value))
        else:
            if self.logger:
                self.logger.error("Orderbook update failed: {} not in orderbook".format(key))
    
    async def updates(self, orderbook):
        for key, value in orderbook.items():
            await self.update(key, value)
    
    async def get(self, key):
        if key in self.orderbook:
            return self.orderbook[key]
        else:
            if self.logger:
                self.logger.error("Orderbook get failed: {} not in orderbook".format(key))
            return -1
    
    async def gets(self):
        return self.orderbook
        