import asyncio

class BaseParamsManager:
    async def __init__(self, keywords, logger=None):
        self.params = {}
        self.keywords = keywords
        self.logger = logger
    
    async def clear(self):
        self.params = {}
        if self.logger:
            self.logger.info("Params cleared")
    
    async def set_params(self, params):
        self.params = params
    
    async def get_params(self):
        return self.params
    
    async def get_param(self, key):
        if key in self.params:
            return self.params[key]
        else:
            self.logger.error("Param get failed: {} not in params".format(key))
            return -1
    
    async def update_param(self, key, value):
        if key in self.params:
            self.params[key] = value
            if self.logger:
                self.logger.info("Param updated: {} = {}".format(key, value))
        else:
            if self.logger:
                self.logger.error("Param update failed: {} not in params".format(key))
    
    async def update_params(self, params):
        for key, value in params.items():
            await self.update_param(key, value)
    
    