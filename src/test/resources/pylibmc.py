#!/usr/bin/python

import sys
import pylibmc

memc = pylibmc.Client(["192.168.142.252:11211"], binary=True)

if memc is None:
    print "failed to create client"
    exit(1)

if memc.set("some_key", "this is a value") is None:
    print "failed to set record"
    exit(1)

print memc.get("some_key")
print memc.get("some_key")