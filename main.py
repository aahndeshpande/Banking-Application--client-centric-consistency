import argparse
import json
import multiprocessing
from concurrent import futures
from time import sleep

import grpc
from termcolor import colored

import branch_pb2_grpc
from Branch import Branch
from Customer import Customer

# Start branch gRPC server process
def serveBranch(branch):
    branch.createStubs()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    branch_pb2_grpc.add_BranchServicer_to_server(branch, server)
    port = str(50000 + branch.id)
    server.add_insecure_port("[::]:" + port)
    server.start()
    server.wait_for_termination()


# Start customer gRPC client processes
def serveCustomer(customer, output_filename):
    # Execute events and get Customer balance output
    output = customer.executeEvents()

    # Interpret existing contents as JSON Array & append new output entry
    output_json = json.load(open(output_filename))
    updated_output_json = output_json + output

    # Overwrite contents of output file with updated JSON
    output_file = open(output_filename, "w")
    output_file.write(json.dumps(updated_output_json, indent=4))
    output_file.close()


# Parse JSON & create objects/processes
def createProcesses(processes, output_filename):
    customers, customerProcesses, branches, branchIds, branchProcesses = [], [], [], [], []
    sleep_time = 0.5
    # Instantiate Branch objects
    for process in processes:
        if process["type"] == "branch":
            branch = Branch(process["id"], process["balance"], branchIds)
            branches.append(branch)
            branchIds.append(branch.id)

    # Spawn Branch processes
    for branch in branches:
        branch_process = multiprocessing.Process(target=serveBranch, args=(branch,))
        branchProcesses.append(branch_process)
        branch_process.start()

    # Allow branch processes to start
    sleep(sleep_time)

    # Instantiate Customer objects
    for process in processes:
        if process["type"] == "customer":
            customer = Customer(process["id"], process["events"])
            customers.append(customer)

    # Spawn Customer processes
    for customer in customers:
        customer_process = multiprocessing.Process(target=serveCustomer, args=(customer, output_filename))
        customerProcesses.append(customer_process)
        customer_process.start()

    # Wait for Customer processes to complete
    for customerProcess in customerProcesses:
        customerProcess.join()
    
    # Terminate Branch processes
    for branchProcess in branchProcesses:
        branchProcess.terminate()

def main():
    # Setup command line argument for 'input_file'
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file")
    args = parser.parse_args()
    
    try:
        # Load JSON file from 'input_file' arg
        input_file = open(args.input_file)
        input_json = json.load(input_file)
        output_filename = "output.json"
        output_file = open("output.json", "w")
        output_file.write("[]")
        output_file.close()
        # Create objects/processes from input file
        createProcesses(input_json, output_filename)
    except FileNotFoundError:
        print(colored("FileNotFoundError: '" + args.input_file + "'", "red"))
    except json.decoder.JSONDecodeError:
        print(colored("Error decoding JSON file. Please make sure that the file is in JSON format", "red"))
    
if __name__ == "__main__":
    main()