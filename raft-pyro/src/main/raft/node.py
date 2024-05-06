import os
import random
import threading
import time
from functools import reduce
from socket import socket, AF_INET, SOCK_STREAM
from typing import List

import Pyro5.server
import Pyro5.api
import Pyro5.nameserver
import Pyro5.errors

from src.main.raft.state.RaftState import RaftState


class LogEntry:
    def __init__(self, term, command):
        self.term = term
        self.command = command


# Persistent state on all servers:
# (Updated on stable storage before responding to RPCs)
# currentTerm latest term server has seen (initialized to 0
# on first boot, increases monotonically)
# votedFor candidateId that received vote in current
# term (or null if none)
# log[] log entries; each entry contains command
# for state machine, and term when entry
# was received by leader (first index is 1)
# Volatile state on all servers:
# commitIndex index of highest log entry known to be
# committed (initialized to 0, increases
# monotonically)
# lastApplied index of highest log entry applied to state
# machine (initialized to 0, increases
# monotonically)
# Volatile state on leaders:
# (Reinitialized after election)
# nextIndex[] for each server, index of the next log entry
# to send to that server (initialized to leader
# last log index + 1)
# matchIndex[] for each server, index of highest log entry
# known to be replicated on server
# (initialized to 0, increases monotonically)
class RaftNode:
    def __init__(self, node_id: int, daemon):
        self.node_id = node_id
        self.other_nodes = {"node.1": 8086, "node.2": 8087, "node.3": 8088, "node.4": 8089}
        self.other_nodes.pop(f"node.{node_id}")

        self.daemon = daemon
        print(f"main thread: {threading.main_thread().ident}")
        self.uri = self.daemon.register(self, objectId=f"node.{node_id}")
        print(f"Node {self.node_id} URI: {self.uri}")
        print(Pyro5.server.DaemonObject(self.daemon).info())
        print(Pyro5.server.DaemonObject(self.daemon).ping())
        print(Pyro5.server.DaemonObject(self.daemon).registered())

        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = 0
        self.last_applied = 0
        self.state = RaftState.FOLLOWER
        self.next_index = {}  # Dict[int, int] - Key: node_id, Value: index to send next logentry entry to
        self.match_index = {}  # Dict[int, int] - Key: node_id, Value: index of highest logentry entry replicated on node
        self.leader_id = None
        self.votes_received = 0
        self.election_timer = threading.Timer(random.uniform(150.00, 300.00) / 1000,
                                              self.handle_election_timeout)
        self.heartbeat_timer = threading.Timer(random.uniform(1000.00, 2000.00) / 1000,
                                               self.handle_heartbeat_timeout).start()

    def start_election(self):
        self.reset_election_timer()
        self.election_timer.start()
        self.current_term += 1
        self.state = RaftState.CANDIDATE
        self.voted_for = self.node_id
        self.votes_received = 1
        print(f"Node {self.node_id} starting election for term {self.current_term}")
        threading_pool = []
        for node_id in self.other_nodes:
            # self.send_request_vote(self.node_id, node_id, self.current_term)
            t = threading.Thread(target=self.send_request_vote, args=(self.node_id, node_id, self.current_term))
            t.start()
            threading_pool.append(t)
        for t in threading_pool:
            t.join()
        self.election_timer.cancel()
        if self.votes_received > (len(self.other_nodes) + 1) / 2:
            self.become_leader()
        self.voted_for = None
        self.votes_received = 0
        self.reset_heartbeat_timer()

    def become_leader(self):
        self.state = RaftState.LEADER
        self.leader_id = self.node_id

        print(f"Node {self.node_id} became leader for term {self.current_term}")

        # Register in nameserver
        ns = Pyro5.api.locate_ns()
        ns.register(f"Lider_Termo{self.current_term}", self.uri)

    def become_follower(self, leader_id, term):
        self.state = RaftState.FOLLOWER
        self.leader_id = leader_id
        self.current_term = term
        self.voted_for = None
        self.votes_received = 0
        self.reset_heartbeat_timer()
        print(f"Node {self.node_id} became follower for term {self.current_term}")

    def commit_entry(self):
        pass

    def send_append(self, message=[]):
        if message is None:
            message = []
        for node_id in self.other_nodes:
            try:
                print(f"Node {self.node_id} sending heartbeat to node {node_id}")
                port = self.other_nodes[node_id]
                obj = Pyro5.api.Proxy(f"PYRO:{node_id}@localhost:{port}")
                #     def append_entries(self, leader_id, term, prev_log_index, prev_log_term, entries, leader_commit):
                prev_log_index = reduce(lambda x, y: x + 1, self.log, 0)
                print(f"Node {self.node_id} sending heartbeat to node {node_id} with prev_log_index {prev_log_index}")
                r = obj.append_entries(self.node_id, self.current_term, prev_log_index, self.current_term, message,
                                       self.commit_index)
                if r[0] is False:
                    self.become_follower(r[1], r[2])
                print(f"Node {self.node_id} received response from node {node_id}: {r}")
            except Pyro5.errors.CommunicationError as e:
                print(f"Node {self.node_id} could not send heartbeat to node {node_id}: {e}")

    def reset_election_timer(self):
        if self.election_timer is not None:
            self.election_timer.cancel()
        self.election_timer = threading.Timer(random.uniform(150.00, 300.00) / 1000,
                                              self.handle_election_timeout)

    def reset_heartbeat_timer(self):
        print(f"Node {self.node_id} resetting heartbeat timer")
        if self.heartbeat_timer is not None:
            self.heartbeat_timer.cancel()
        if self.state is not RaftState.LEADER:
            print(f"Node {self.node_id} starting heartbeat timer")
            self.heartbeat_timer = threading.Timer(random.uniform(1000.00, 2000.00) / 1000,
                                                   self.handle_heartbeat_timeout)
        if self.state is RaftState.LEADER:
            print(f"Node {self.node_id} is leader, not starting heartbeat timer")
            self.heartbeat_timer = threading.Timer(random.uniform(150.00, 300.00) / 1000, self.send_append, args=([]))
        self.heartbeat_timer.start()

    def handle_election_timeout(self):
        print(f"Node {self.node_id} election timeout")
        self.start_election()

    def handle_heartbeat_timeout(self):
        print(f"Node {self.node_id} heartbeat timeout")
        self.start_election()

    def send_request_vote(self, candidate_id, node_id, term):
        try:
            print(f"Node {self.node_id} sending request vote to node {node_id}")
            port = self.other_nodes[node_id]
            print(f"Node {self.node_id} sending request vote to node {node_id} on port {port}")
            with Pyro5.api.Proxy(f"PYRO:{node_id}@localhost:{port}") as obj:
                obj._pyroClaimOwnership()
                r = obj.request_vote(candidate_id, term)
            if r:
                self.votes_received += 1
                print(f"Votes received: {self.votes_received}")
            print(f"Node {self.node_id} received response from node {node_id}: {r}")
        except Pyro5.errors.CommunicationError as e:
            print(f"Node {self.node_id} could not send request vote to node {node_id}: {e}")

    # invoked by candidates to gather votes (§5.2).
    # Arguments:
    # term candidate’s term
    # candidateId candidate requesting vote
    # lastLogIndex index of candidate’s last log entry (§5.4)
    # lastLogTerm term of candidate’s last log entry (§5.4)
    # Results:
    # term currentTerm, for candidate to update itself
    # voteGranted true means candidate received vote
    # Receiver implementation:
    # 1. Reply false if term < currentTerm (§5.1)
    # 2. If votedFor is null or candidateId, and candidate’s log is at
    # least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
    @Pyro5.server.expose
    def request_vote(self, candidate_id, term):
        if term < self.current_term:
            return False
        # if self.voted_for is None or self.voted_for == candidate_id:
        #     self.voted_for = candidate_id
        #     self.state = RaftState.FOLLOWER
        #     return True
        return True

    # invoked by leader to replicate log entries (§5.3); also used as
    # heartbeat (§5.2).
    # Arguments:
    # term leader’s term
    # leaderId so follower can redirect clients
    # prevLogIndex index of log entry immediately preceding
    # new ones
    # prevLogTerm term of prevLogIndex entry
    # entries[] log entries to store (empty for heartbeat;
    # may send more than one for efficiency)
    # leaderCommit leader’s commitIndex
    # Results:
    # term currentTerm, for leader to update itself
    # success true if follower contained entry matching
    # prevLogIndex and prevLogTerm
    # Receiver implementation:
    # 1. Reply false if term < currentTerm (§5.1)
    # 2. Reply false if log doesn’t contain an entry at prevLogIndex
    # whose term matches prevLogTerm (§5.3)
    # 3. If an existing entry conflicts with a new one (same index
    # but different terms), delete the existing entry and all that
    # follow it (§5.3)
    # 4. Append any new entries not already in the log
    # 5. If leaderCommit > commitIndex, set commitIndex =
    # min(leaderCommit, index of last new entry)
    @Pyro5.server.expose
    def append_entries(self, leader_id, term, prev_log_index, prev_log_term, entries, leader_commit):
        self.reset_heartbeat_timer()
        if term < self.current_term:
            return False, self.current_term, self.node_id
        if prev_log_index > len(self.log) or self.log[prev_log_index] != prev_log_term:
            return False, self.current_term, self.node_id
        if len(entries) > 0:
            for i, entry in enumerate(entries):
                self.log.append(LogEntry(term, entry))
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log))
        return True

    @Pyro5.server.expose
    def send_message(self, message):
        print(f"Node {self.node_id} received message: {message}")
        self.log.append(LogEntry(self.current_term, message))
        self.send_append(message)


portD = int(os.environ.get("PORT"))
daemon = Pyro5.server.Daemon(port=portD)
# get host and port for node_id


threading.Thread(target=daemon.requestLoop).start()

raft_node = RaftNode(int(os.environ.get("NODE_ID")), daemon)
# def start_heartbeat_timer(self) -> None:
#     if(self.state != RaftStatus.LEADER):
#         return
#
#     timeout = random.randint(50, 100) / 100
#     self.heartbeat_timer = threading.Timer(timeout, self.send_heartbeat_broadcast)
#     self.heartbeat_timer.start()
#
# def send_heartbeat_broadcast(self):
#     try:
#         self.heartbeat_timer.cancel()
#
#         if(self.state != RaftStatus.LEADER):
#             return
#
#         self.nodes = self.search_nod
#     if not self.nodes:
#         return
#
# threads = []
# for node_name, node_uri in self.nodes.items():
#     thread = threading.Thread(target=self.send_heartbeat_to_node, args=(node_name, node_uri))
#     threads.append(thread)
#     thread.start()
#
# for thread in threads:
#     thread.join()
