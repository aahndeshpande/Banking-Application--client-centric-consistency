from concurrent import futures

import grpc
import time

import branch_pb2_grpc
from branch_pb2 import MsgRequest, MsgResponse


class Branch(branch_pb2_grpc.BranchServicer):
    def __init__(self, id, balance, branches):
        self.id = id
        self.balance = balance
        self.branches = branches
        self.stubList = list()
        self.priorWrites = dict()
        self.recvMsg = list()

    # Setup gRPC channel & client stub for each branch
    def createStubs(self):
        for branchId in self.branches:
            if branchId != self.id:
                port = str(50000 + branchId)
                channel = grpc.insecure_channel("localhost:" + port)
                self.stubList.append(branch_pb2_grpc.BranchStub(channel))

    # Generate new event ID, append to priorWrites
    def updateWrites(self, id):
        newEventId = len(self.priorWrites.get(id)) + 1
        self.priorWrites.get(id).append(newEventId)

    # TODO
    # Verify self.priorWrites contains all entries from incoming priorWrites
    def verifyPreviousWrites(self, ws, id):
        if not id in self.priorWrites:
            self.priorWrites[id] = list()
        return all(entry in self.priorWrites.get(id) for entry in ws)

    # Incoming MsgRequest from Customer transaction
    def MsgDelivery(self, request, context):
        # Enforce read your writes by waiting till all the prior writes are propagated
        while not self.verifyPreviousWrites(request.priorWrites, request.id):
            time.sleep(0.1)
        return self.ProcessMsg(request, False)

    # Incoming MsgRequest from Branch propagation
    def MsgPropagation(self, request, context):
        # Enforce read your writes by waiting till all the prior writes are propagated
        while not self.verifyPreviousWrites(request.priorWrites, request.id):
            time.sleep(0.1)
        return self.ProcessMsg(request, True)

    # Handle received Msg, generate and return a MsgResponse
    def ProcessMsg(self, request, isPropagation):
        result = "success"
        
        if request.money < 0:
            result = "fail"   
        elif request.interface == "query":
            pass
        elif request.interface == "deposit":
            self.balance += request.money
        elif request.interface == "withdraw":
            if self.balance >= request.money:
                self.balance -= request.money
            else:
                result = "fail"
                
         # Create msg to be appended to self.recvMsg list
        msg = {"interface": request.interface, "result": result}

        # Add 'money' entry for 'query' events
        if request.interface == "query":
            msg["money"] = request.money

        self.recvMsg.append(msg)

        response = MsgResponse(
            interface=request.interface, balance=self.balance, priorWrites = self.priorWrites[request.id], result=result
        )
        
        if request.interface != "query":
            # Update priorWrites with a new event ID
            self.updateWrites(request.id)

            # Propagate to other branches
            if not isPropagation:
                self.Propagate_Transaction(request)
                # time.sleep(0.5)
        return response

    # Propagate Customer transactions to other Branches
    def Propagate_Transaction(self, request):
        for stub in self.stubList:
            stub.MsgPropagation(MsgRequest(id = request.id, interface=request.interface, money=request.money, priorWrites=request.priorWrites))
