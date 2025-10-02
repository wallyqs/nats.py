from nats.metal import NATS

nc = NATS.connect("localhost")

nc.publish("foo", b'bar')

nc.close()
