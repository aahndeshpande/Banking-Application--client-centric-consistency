<h1>CSE 531: Client-Centric Consistency Project</h1>

Andrew Flores

CSE 531: Distributed and Multiprocessor Operating Systems, Spring 2021 (B)



## Quick Start

1. `git clone` the repo and `cd` into the project
2. `python3 -m venv env` to initialize the virtual environment
3. `source env/bin/activate` to activate the virtual environment
4. `pip install -r requirements.txt` to install project dependencies
5. `python main.py <filename>.json` to start the program
6. The result will be written to `output/<filename>.json`

## Python Environment

Python 3 is required for this project. `venv` is used to sandbox the Python project and dependencies in a virtual environment.

In order to use the included Python version and project dependency files, the virtual environment must be initialized and activated before the program is ran.

From the project root:

```sh
# Initialize the Python virtual environment
python3 -m venv env

# Activate the virtual environment
source env/bin/activate

# Install project dependencies in the virtual environment
pip install -r requirements.txt
```

For more information, please refer to the [12. Virtual Environments and Packages](https://docs.python.org/3/tutorial/venv.html) page of the official Python documentation.

## Overview

The following important files are included in this project:

* `main.py`: Main program to be executed from the command line with: `python main.py input.json`

* `branch.proto`: Protocol buffer file defining RPC messages & services. This file has already been compiled to produce the `branch_pb2.py` & `branch_pb2_grpc.py` files.

* `Branch.py`: Branch class served as a gRPC server to process customer transactions and propagate them to other branches.

* `Customer.py`: Customer class with gRPC client branch stub to send transaction requests to its corresponding bank branch.

## Input Files

The input file should be in `.json` format and is passed to the program via a command line argument:

```sh
python main.py <filename>.json
```

The following input files are included in the `input` directory from the CSE 531 Client-Centric Consistency Project instructions:

### input/monotonic-writes.json
(Ran with `python main.py input/monotonic-writes.json`)
```json
[
  {
    "id": 1,
    "type": "customer",
    "events": [
      { "interface": "deposit", "money": 400, "dest": 1 },
      { "interface": "withdraw", "money": 400, "dest": 2 },
      { "interface": "query", "dest": 2 }
    ]
  },
  {
    "id": 1,
    "type": "bank",
    "balance": 0
  },
  {
    "id": 2,
    "type": "bank",
    "balance": 0
  }
]
```

### input/read-your-writes.json
(Ran with `python main.py input/read-your-writes.json`)
```json
[
  {
    "id": 1,
    "type": "customer",
    "events": [
      { "interface": "deposit", "money": 400, "dest": 1 },
      { "interface": "query", "dest": 2 }
    ]
  },
  {
    "id": 1,
    "type": "bank",
    "balance": 0
  },
  {
    "id": 2,
    "type": "bank",
    "balance": 0
  }
]
```

## Output Files
The program will write the results of the Customer balance outputs in a JSON file in the `output` directory. The filename will be the same as your input file (e.g: `python main.py input/monotonic-writes.json` will produce the output file `output/monotonic-writes.json`).

The following JSON outputs are pre-computed from the example input JSON files in the `input` directory included from the project instructions:

### output/monotonic-writes.json
```json
[
    {
        "id": 1,
        "balance": 0
    }
]
```

### output/read-your-writes.json
```json
[
    {
        "id": 1,
        "balance": 400
    }
]
```