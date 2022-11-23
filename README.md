# craqued: Chain Replication - A QUick and Easy Deployment

To run this:

Step 1: Run the coordinator
```
python coordinator.py
```

Step 2: Set up your servers
```
python node.py
```

The default port is 5002. To set a different port, use the `-P` flag.
```
python node.py -P 5003
```

Step 3: Run as many clients as you want
```
python client.py
```

This will set up the command shell. Use the following commands to read and write data.
```
sup?> write a 10
sup?> read a
10
```
