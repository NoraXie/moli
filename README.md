# moli

moli is a little exercise about zookeeper.

moli has a c/s architecture. 

c is zkclient. With zkclient user will be easy to manage their server in the cloud. If one of your server suspend or shutdown abnormal, zkclient will show you which one is it. Also if you add a new server zkclient also know about that.

s is zkserver. zkserver just watching the diffs about servers which registered on zookeeper. Once something happened, like one of your server offline, it will notice zkclient.