#!/usr/bin/env ruby

require 'rubygems'
require 'memcached'

opts = {:binary_protocol => true }
cache = Memcached.new('192.168.142.253:11211', opts)

cache.set('v1', 'before')
p cache.get('v1')
cache.set('v1', 'after')
p cache.get('v1')
