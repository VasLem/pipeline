from urllib.request import urlopen
from urllib.error import URLError
import socket


def internetOn():
    try:
        urlopen("http://8.8.8.8", timeout=1)
        return True
    except URLError as err:
        return False
