CS 519 GFS project
Yuhang Zhou yz853
Yi Qi yq97
12/21/2019

In each server and client
pip install rpyc
pip install numpy
#using "pip install rpyc --user" if running in ilab server.

1st step:
"python3 master.py" in master server(ilab server or local device)
"python3 chunkserver.py" in chunk server(ilab server or local device)

2nd step:
get the ip of master server and chunk server
change the ip of two servers in "client.py" in Line 13,14
If you deploy these two servers in local device,
change the ip to "localhost"


3rd step:
https://drive.google.com/drive/folders/1KqY4yfi60R5q21Lv6S_MRakt0XXF-1-o?usp=sharing
Go to this link and download the whole "client" folder
put the "client" folder at the root of client

4th step:
run function upload_test() in "client.py" in Line 178 to upload all sources files to the GFS
run function modify_test() in "client.py" in Line 179 to do the modify test in GFS