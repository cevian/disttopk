import socket

servers = []
#for i in range(1, 51):
#  servers.append("node"+str(i)+".mpisws.vicci.org")
#  servers.append("node"+str(i)+".gt.vicci.org")

s_as_str = """node10.gt.vicci.org
node1.gt.vicci.org
node2.gt.vicci.org
node14.gt.vicci.org
node3.gt.vicci.org
node5.gt.vicci.org
node6.gt.vicci.org
node20.gt.vicci.org
node8.gt.vicci.org
node32.gt.vicci.org
node24.gt.vicci.org
node10.mpisws.vicci.org
node11.mpisws.vicci.org
node30.mpisws.vicci.org
node31.mpisws.vicci.org
node14.mpisws.vicci.org
node15.mpisws.vicci.org
node1.mpisws.vicci.org
node17.mpisws.vicci.org
node33.mpisws.vicci.org
node19.mpisws.vicci.org
node20.mpisws.vicci.org
node21.mpisws.vicci.org
node22.mpisws.vicci.org
node23.mpisws.vicci.org
node24.mpisws.vicci.org
node34.mpisws.vicci.org
node26.mpisws.vicci.org
node2.mpisws.vicci.org
node3.mpisws.vicci.org
node4.mpisws.vicci.org
node5.mpisws.vicci.org
node6.mpisws.vicci.org
"""
#node31.washington.vicci.org
#node35.washington.vicci.org
#node36.washington.vicci.org
#node37.washington.vicci.org
#node38.washington.vicci.org
#node39.washington.vicci.org
#node41.washington.vicci.org
#node42.washington.vicci.org
#node43.washington.vicci.org
#node44.washington.vicci.org
#node45.washington.vicci.org
#node47.washington.vicci.org
#node48.washington.vicci.org
#node49.washington.vicci.org
#node67.washington.vicci.org
# 


servers = [s for s in s_as_str.splitlines()]

#servers=["node1.mpisws.vicci.org"]

for index, server in enumerate(servers):
  print server + " public_ip=" +socket.gethostbyname(server)+" server_index="+str(index)

print "---------------"
for server in servers:
  print server,
