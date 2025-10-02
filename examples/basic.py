from nats.metal import NATS

nc = NATS.connect("localhost")

for i in range(1000):
  nc.publish("foo", b'bar')

nc.close()
