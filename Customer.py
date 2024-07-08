import grpc

import branch_pb2_grpc
from branch_pb2 import MsgRequest

class Customer:
    def __init__(self, id, events):
        # unique ID of the Customer
        self.id = id
        # events from the input
        self.events = events
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # a list of prior write operations executed
        self.priorWrites = list()
        
    # Send gRPC request for each event
    def executeEvents(self):
        for event in self.events:
            # Setup gRPC channel & client stub for branch
            port = str(50000 + event["branch"])
            branch_address = f"localhost:{port}"
            channel = grpc.insecure_channel(branch_address)
            stub = branch_pb2_grpc.BranchStub(channel)

            # Set MsgRequest.money = 0 for query events
            if event["interface"] == "query":
                event["money"] = 0

            # Send request to Branch server
            response = stub.MsgDelivery(MsgRequest(id=self.id, interface=event["interface"], money=event["money"], priorWrites=self.priorWrites))

            # Append to self.recvMsg list
            if event["interface"] == "query":
                self.recvMsg.append({"id": self.id, "recv": [{"interface": response.interface, "branch": event["branch"], "balance": response.balance}]})
            else:
                self.recvMsg.append({"id": self.id, "recv": [{"interface": response.interface, "branch": event["branch"], "result": response.result}]})

            # Update priorwrites from response
            if event["interface"] != "query":
                self.priorWrites = response.priorWrites

        # Return output msg
        return self.recvMsg
