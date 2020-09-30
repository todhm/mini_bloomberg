from stem import Signal
from stem.control import Controller
import socket


def reset_ip_address():
    ip_addr = socket.gethostbyname('torproxy')
    with Controller.from_port(
        address=ip_addr,
        port=9051
    ) as controller:
        controller.authenticate()
        controller.signal(Signal.NEWNYM)
        # time.sleep(controller.get_newnym_wait())

    