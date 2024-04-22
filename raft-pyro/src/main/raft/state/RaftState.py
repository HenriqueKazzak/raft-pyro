from enum import Enum


class RaftState(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3
