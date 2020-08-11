import socket
from typing import List, Dict, Any
import ssl

import aiohttp
import requests
from aiohttp.resolver import AbstractResolver
from pixivpy_async import PixivClient


class CFResolver(AbstractResolver):
    def __init__(self):
        self.session = requests.Session()

    async def resolve(self, host: str, port: int, family: int) -> List[Dict[str, Any]]:
        params = {
            'ct': 'application/dns-json',
            'name': host,
            'type': 'A',
            'do': 'false',
            'cd': 'false',
        }
        rsp = self.session.get("https://cloudflare-dns.com/dns-query", params=params, timeout=3)
        hosts = rsp.json()['Answer']
        return [{'hostname': host,
                 'host': i['data'], 'port': port,
                 'family': family, 'proto': 0,
                 'flags': socket.AI_NUMERICHOST} for i in hosts]

    async def close(self) -> None:
        self.session.close()

def sni_callback(socket:ssl.SSLSocket , name:str, ctx:ssl.SSLContext):
    pass

class PixivClientImpl(PixivClient):
    def __init__(self, limit=30, timeout=10):
        self.cxt = ssl.create_default_context()
        self.conn = aiohttp.TCPConnector(limit_per_host=limit, resolver=CFResolver())
        self.client = aiohttp.ClientSession(
            connector=self.conn,
            timeout=aiohttp.ClientTimeout(total=timeout),
        )


