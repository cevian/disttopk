import socket

servers = []
#for i in range(1, 51):
#  servers.append("node"+str(i)+".mpisws.vicci.org")
#  servers.append("node"+str(i)+".gt.vicci.org")

s_as_str = """sns1.cs.princeton.edu
sns2.cs.princeton.edu
sns3.cs.princeton.edu
sns5.cs.princeton.edu
sns6.cs.princeton.edu
sns9.cs.princeton.edu
sns10.cs.princeton.edu
sns12.cs.princeton.edu
sns13.cs.princeton.edu
sns14.cs.princeton.edu
sns15.cs.princeton.edu
sns16.cs.princeton.edu
sns17.cs.princeton.edu
sns18.cs.princeton.edu
sns19.cs.princeton.edu
sns20.cs.princeton.edu
sns21.cs.princeton.edu
sns22.cs.princeton.edu
sns23.cs.princeton.edu
sns24.cs.princeton.edu
sns25.cs.princeton.edu
sns26.cs.princeton.edu
sns27.cs.princeton.edu
sns38.cs.princeton.edu
sns39.cs.princeton.edu
sns40.cs.princeton.edu
sns42.cs.princeton.edu
sns47.cs.princeton.edu
sns48.cs.princeton.edu
sns49.cs.princeton.edu
sns50.cs.princeton.edu
sns51.cs.princeton.edu
sns53.cs.princeton.edu
"""

servers = [s for s in s_as_str.splitlines()]

#servers=["node1.mpisws.vicci.org"]

print """[workers:children]
setup

[setup]"""

for index, server in enumerate(servers):
  print server + " public_ip=" +socket.gethostbyname(server)+" server_index="+str(index)

print "#---------------"
print "#",
for server in servers:
  print server,
