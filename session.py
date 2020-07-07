import asyncio
from typing import List, Optional

from aiohttp import ClientSession, ClientResponse

from user_agent import USER_AGENTS

class ScrapperSession(ClientSession):
    '''
    WIP
    '''
    
    def __init__(
        self, 
        user_agents: Optional[List[str]] = None,
        use_random_ua: bool = True,
        *asrgs,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.use_random_ua = use_random_ua
        self.user_agents = user_agents or USER_AGENTS

    async def get_url(self, url: List[str], **kwargs):
        pass

