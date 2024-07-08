from time import sleep

import grpc
from termcolor import colored

import branch_pb2_grpc
from branch_pb2 import MsgRequest


class Customer:
    def __init__(self, id, events):
        self.id = id
        self.events = events
        self.recvMsg = list()
        self.writeset = list()

    # Send gRPC request for each event
    def executeEvents(self):
        for event in self.events:
            # print(colored("\nC#" + str(self.id) + " to B#" + str(event["dest"]) + "\t" + str(event), "cyan"))

            # Setup gRPC channel & client stub for branch
            port = str(50000 + event["dest"])
            channel = grpc.insecure_channel("localhost:" + port)
            stub = branch_pb2_grpc.BranchStub(channel)

            # Set MsgRequest.money = 0 for query events
            money = event["money"] if event["interface"] != "query" else 0

            # Send request to Branch server
            response = stub.MsgDelivery(MsgRequest(interface=event["interface"], money=money, writeset=self.writeset))

            # Append to self.recvMsg list
            self.recvMsg.append({"interface": response.interface, "dest": event["dest"], "money": response.money})

            # Update writeset from response
            if event["interface"] != "query":
                self.writeset = response.writeset

            # Sleep to ensure writesets have time to update
            sleep(0.25)

        # Return output msg
        return {"id": self.id, "balance": self.recvMsg[-1]["money"]}
