import BLS_Request
import pc
import wp

print("Choose an option:")
print("1: pc        (Industry)")
print("2: wp        (Commodity)")
wpORpc = str(input("Type either pc or wp: "))
while wpORpc != "wp" and wpORpc != "pc":
    wpORpc = str(input("Type either pc or wp: "))
if wpORpc == "wp":
    wp.wpProcessing()
elif wpORpc == "pc":
    pc.pcProcessing()