# get the last leader from pyro ns and send message for leader

import Pyro5.api
import Pyro5.nameserver

def main():
    ns = Pyro5.api.locate_ns()
    last_leader = max([int(i. split("Lider_Termo")[1]) for i in ns.list().keys() if "Lider_Termo" in i])
    proxy_obj = Pyro5.api.Proxy(ns.lookup(f"Lider_Termo{last_leader}"))
    print(f"{ns.lookup(f'Lider_Termo{last_leader}')} is the leader")
    proxy_obj.send_message("Hello from client")
main()






